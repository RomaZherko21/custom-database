# Запуск основного приложения
run:
	go run ./cmd/main.go

# Запуск всех тестов
test:
	go test ./internal/... -v

# Запуск тестов, отображающий только первый тест с ошибкой
test-fail:
	go test -failfast ./internal/...