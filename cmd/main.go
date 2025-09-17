package main

import (
	"custom-database/cmd/mode"
	"custom-database/internal/backend"
	"custom-database/internal/parser"
	"log"
)

func main() {
	parser := parser.NewParser()

	mb, err := backend.NewMemoryBackend()
	if err != nil {
		log.Fatal("Error creating memory backend:", err)
	}

	mode.RunConsoleMode(parser, mb)
}
