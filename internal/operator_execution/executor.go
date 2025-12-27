package operator_execution

type Executor interface {
	// Init инициализирует оператор перед началом выполнения
	Init() error

	// Next возвращает следующий tuple или nil, если данных больше нет
	// Возвращает ошибку, если произошла проблема при выполнении
	Next() (*Tuple, error)

	// Close освобождает ресурсы оператора
	Close() error
}
