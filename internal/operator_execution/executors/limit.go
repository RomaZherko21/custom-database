package executors

import (
	"custom-database/internal/operator_execution"
	"custom-database/internal/query_planning"
)

// LimitExecutor ограничивает количество возвращаемых tuple
type LimitExecutor struct {
	child operator_execution.Executor
	plan  *query_planning.LimitPlan

	count int64 // Количество уже возвращенных tuple
}

// NewLimitExecutor создает новый LimitExecutor
func NewLimitExecutor(child operator_execution.Executor, plan *query_planning.LimitPlan) operator_execution.Executor {
	return &LimitExecutor{
		child: child,
		plan:  plan,
		count: 0,
	}
}

// Init инициализирует executor
func (e *LimitExecutor) Init() error {
	if err := e.child.Init(); err != nil {
		return err
	}
	e.count = 0
	return nil
}

// Next возвращает следующий tuple, если не достигнут лимит
func (e *LimitExecutor) Next() (*operator_execution.Tuple, error) {
	// Проверяем, достигнут ли лимит
	if e.count >= e.plan.Limit {
		return nil, nil
	}

	// Получаем следующий tuple от дочернего executor'а
	tuple, err := e.child.Next()
	if err != nil {
		return nil, err
	}

	if tuple == nil {
		return nil, nil
	}

	// Увеличиваем счетчик
	e.count++

	return tuple, nil
}

// Close освобождает ресурсы
func (e *LimitExecutor) Close() error {
	return e.child.Close()
}
