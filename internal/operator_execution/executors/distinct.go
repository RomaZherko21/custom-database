package executors

import (
	"custom-database/internal/operator_execution"
	"fmt"
	"strings"
)

type DistinctExecutor struct {
	child operator_execution.Executor

	seenTuples map[string]bool // Хеш-мапа для отслеживания уникальных tupleей
}

func NewDistinctExecutor(child operator_execution.Executor) operator_execution.Executor {
	return &DistinctExecutor{
		child:      child,
		seenTuples: make(map[string]bool),
	}
}

func (e *DistinctExecutor) Init() error {
	if err := e.child.Init(); err != nil {
		return err
	}
	e.seenTuples = make(map[string]bool)
	return nil
}

func (e *DistinctExecutor) Next() (*operator_execution.Tuple, error) {
	for {
		// Получаем следующий tuple от дочернего executor'а
		tuple, err := e.child.Next()
		if err != nil {
			return nil, err
		}

		if tuple == nil {
			// Больше tuple нет
			return nil, nil
		}

		// Вычисляем ключ для tupleа (строковое представление всех значений)
		key := e.computeTupleKey(tuple)

		// Проверяем, видели ли мы уже этот tuple
		if e.seenTuples[key] {
			// Дубликат - пропускаем
			continue
		}

		// Новый уникальный tuple - добавляем в set и возвращаем
		e.seenTuples[key] = true
		return tuple, nil
	}
}

// computeTupleKey вычисляет уникальный ключ для tuple
// Использует строковое представление всех значений tuple
func (e *DistinctExecutor) computeTupleKey(tuple *operator_execution.Tuple) string {
	if tuple == nil {
		return ""
	}

	// Создаем строковое представление всех значений tuple
	var parts []string
	for _, value := range tuple.Values {
		parts = append(parts, e.valueToString(value))
	}

	// Объединяем все части
	return strings.Join(parts, "")
}

// valueToString преобразует значение в строку для сравнения
func (e *DistinctExecutor) valueToString(value operator_execution.Value) string {
	if value.IsNull {
		return "NULL"
	}

	switch value.Type {
	case operator_execution.ValueTypeInt:
		return fmt.Sprintf("INT:%d", value.IntVal)
	case operator_execution.ValueTypeText:
		escaped := strings.ReplaceAll(value.StrVal, "|", "||")
		return fmt.Sprintf("TEXT:%s", escaped)
	default:
		return "NULL"
	}
}

// Close освобождает ресурсы
func (e *DistinctExecutor) Close() error {
	// Очищаем хеш-таблицу
	e.seenTuples = make(map[string]bool)
	return e.child.Close()
}
