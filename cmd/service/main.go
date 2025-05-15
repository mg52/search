package main

import (
	"log"
	"net/http"

	ht "github.com/mg52/search/cmd/http"
)

func main() {
	ht := ht.NewHTTP()

	mux := http.NewServeMux()
	mux.HandleFunc("/create-index", ht.CreateIndex)
	mux.HandleFunc("/search", ht.Search)
	mux.HandleFunc("/add-to-index", ht.AddToIndex)
	mux.HandleFunc("/add-or-update-document", ht.AddOrUpdateDocument)
	mux.HandleFunc("/bulk-add-or-update-document", ht.AddOrUpdateDocumentInBulk)
	mux.HandleFunc("/remove-document-by-id", ht.RemoveDocumentByID)
	mux.HandleFunc("/save-controller", ht.SaveController)
	mux.HandleFunc("/load-controller", ht.LoadController)
	mux.HandleFunc("/health", ht.Health)

	addr := ":8080"
	log.Printf("Starting server on %s…", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
