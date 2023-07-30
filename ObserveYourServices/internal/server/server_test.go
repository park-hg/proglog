package server

import (
	"context"
	"flag"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	api "github.com/park-hg/proglog/api/v1"
	"github.com/park-hg/proglog/internal/auth"
	"github.com/park-hg/proglog/internal/config"
	"github.com/park-hg/proglog/internal/log"
)

var debug = flag.Bool("debug", false, "Enable observability for debugging.")

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"unauthorized fails":                                 testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (rootClient, nobodyClient api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
		})
		assert.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.Dial(l.Addr().String(), opts...)
		assert.NoError(t, err)

		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	assert.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	assert.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	assert.NoError(t, err)

	authorizer, err := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	assert.NoError(t, err)
	cfg = &Config{CommitLog: clog, Authorizer: authorizer}
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	assert.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	var rootConn, nobodyConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(config.RootClientCertFile, config.RootClientKeyFile)
	nobodyConn, nobodyClient, _ = newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile)

	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := os.CreateTemp("", "metrics-*.log")
		assert.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		tracesLogFile, err := os.CreateTemp("", "traces-*.log")
		assert.NoError(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			ReportingInterval: time.Second,
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
		})
		assert.NoError(t, err)
		err = telemetryExporter.Start()
		assert.NoError(t, err)
	}

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		clog.Remove()
		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
	}
}

func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{Value: []byte("hello world")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	assert.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	assert.NoError(t, err)
	assert.Equal(t, want.Value, consume.Record.Value)
	assert.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	assert.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}

	{
		stream, err := client.ProduceStream(ctx)
		assert.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			assert.NoError(t, err)
			res, err := stream.Recv()
			assert.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf(
					"got offset: %d, want: %d",
					res.Offset,
					offset,
				)
			}
		}
	}

	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		assert.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			assert.NoError(t, err)
			assert.Equal(t,
				res.Record,
				&api.Record{
					Value:  record.Value,
					Offset: uint64(i),
				},
			)
		}
	}
}

func testUnauthorized(t *testing.T, _, client api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}})
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	if consume != nil {
		t.Fatalf("consume reponse should be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
}
