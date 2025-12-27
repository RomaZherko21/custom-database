package main

import (
	// "custom-database/cmd/mode"
	"custom-database/cmd/mode/console_mode"
	"custom-database/cmd/mode/http_mode"
	"custom-database/internal/http/handlers"
	"custom-database/internal/operator_execution/executors"
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

	backendService, err := executors.NewBackendService()
	if err != nil {
		log.Fatal("Error creating backend service:", err)
	}

	handlers := handlers.NewHttpHandlers(backendService)

	switch *mode {
	case "console":
		console_mode.RunConsoleMode(backendService)
	case "http":
		http_mode.RunHttpServer(handlers, *port)
	default:
		log.Fatal("Неизвестный режим работы. Используйте 'console' или 'http'")
	}
}
