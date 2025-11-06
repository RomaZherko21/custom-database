package main

import (
	// "custom-database/cmd/mode"
	"custom-database/cmd/mode/console_mode"
	"custom-database/cmd/mode/http_mode"
	"custom-database/internal/backend"
	"custom-database/internal/http/handlers"
	"custom-database/internal/parser"
	"flag"
	"log"
)

// @title Custom Database API
// @version 1.0
// @description API для работы с кастомной базой данных
// @host localhost:8080
// @BasePath /
func main() {
	mode := flag.String("mode", "console", "Режим работы: console или http")
	port := flag.String("port", "port", "Порт для HTTP сервера")
	flag.Parse()

	parserService := parser.NewParser()
	mb, err := backend.NewMemoryBackend()
	if err != nil {
		log.Fatal("Error creating memory backend:", err)
	}

	handlers := handlers.NewHttpHandlers(parserService, mb)

	switch *mode {
	case "console":
		console_mode.RunConsoleMode(parserService, mb)
	case "http":
		http_mode.RunHttpServer(handlers, *port)
	default:
		log.Fatal("Неизвестный режим работы. Используйте 'console' или 'http'")
	}
}
