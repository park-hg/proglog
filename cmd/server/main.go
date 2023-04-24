package main

import (
	"log"

	"github.com/park-hg/proglog/internal/server"
)

func main() {
	server := server.NewHttpServer(":8080")
	log.Fatal(server.ListenAndServe())
}
