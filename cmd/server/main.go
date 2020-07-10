package main

import (
	"log"

	"github.com/andreylm/proglog/internal/server"
)

func main() {
	log.SetFlags(log.Lshortfile | log.Ldate)
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
