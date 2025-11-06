# Запуск консольного интерфейса
run:
	go run ./cmd/main.go -mode console

# Запуск HTTP сервера
run-http:
	go run ./cmd/main.go -mode http -port 8080

# Запуск всех тестов
test:
	go test ./internal/... -v

# Запуск тестов, отображающий только первый тест с ошибкой
test-fail:
	go test -failfast ./internal/...

# Запуск тестов E2E, без кэша
test-e2e:
	go test ./e2e/... -v -count=1

# Генерация Swagger документации
generate:
	swag init -g ./cmd/main.go -o ./docs