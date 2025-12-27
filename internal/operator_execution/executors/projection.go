package executors

import (
	"custom-database/internal/operator_execution"
	"custom-database/internal/query_planning"
	"fmt"
)

// ProjectionExecutor выполняет проекцию колонок (SELECT)
type ProjectionExecutor struct {
	child operator_execution.Executor
	plan  *query_planning.ProjectionPlan

	// Схема результата
	outputSchema *operator_execution.Schema
	firstTuple   *operator_execution.Tuple // Первый tuple, полученный в Init()
}

// NewProjectionExecutor создает новый ProjectionExecutor
func NewProjectionExecutor(child operator_execution.Executor, plan *query_planning.ProjectionPlan) operator_execution.Executor {
	return &ProjectionExecutor{
		child: child,
		plan:  plan,
	}
}

// Init инициализирует executor
func (e *ProjectionExecutor) Init() error {
	if err := e.child.Init(); err != nil {
		return err
	}

	// Получаем схему от дочернего executor'а
	// Для этого нужно получить первый tuple, чтобы узнать схему
	// В простой реализации мы будем получать схему из первого tuple
	firstTuple, err := e.child.Next()
	if err != nil {
		return err
	}

	// Создаем проекцию схемы
	if len(e.plan.Columns) == 0 {
		// SELECT * - возвращаем все колонки
		e.outputSchema = firstTuple.Schema
		e.firstTuple = firstTuple
	} else {
		// SELECT col1, col2, ... - возвращаем только указанные колонки
		projectedSchema, err := firstTuple.Schema.Project(e.plan.Columns)
		if err != nil {
			return fmt.Errorf("failed to project schema: %w", err)
		}
		e.outputSchema = projectedSchema

		// Проецируем первый tuple
		projectedValues := make([]operator_execution.Value, 0, len(e.plan.Columns))
		for _, colName := range e.plan.Columns {
			value := firstTuple.GetValueByName(colName)
			if value == nil {
				return fmt.Errorf("column '%s' not found in tuple", colName)
			}
			projectedValues = append(projectedValues, *value)
		}
		e.firstTuple = operator_execution.NewTuple(projectedValues, projectedSchema)
	}

	return nil
}

// Next возвращает следующий tuple с проекцией колонок
func (e *ProjectionExecutor) Next() (*operator_execution.Tuple, error) {
	// Возвращаем первый tuple, если он был сохранен
	if e.firstTuple != nil {
		result := e.firstTuple
		e.firstTuple = nil
		return result, nil
	}

	tuple, err := e.child.Next()
	if err != nil {
		return nil, err
	}

	if tuple == nil {
		return nil, nil
	}

	// Проецируем значения
	if len(e.plan.Columns) == 0 {
		// SELECT * - возвращаем все значения
		return tuple, nil
	}

	// SELECT col1, col2, ... - возвращаем только указанные колонки
	projectedValues := make([]operator_execution.Value, 0, len(e.plan.Columns))
	for _, colName := range e.plan.Columns {
		value := tuple.GetValueByName(colName)
		if value == nil {
			return nil, fmt.Errorf("column '%s' not found in tuple", colName)
		}
		projectedValues = append(projectedValues, *value)
	}

	return operator_execution.NewTuple(projectedValues, e.outputSchema), nil
}

// Close освобождает ресурсы
func (e *ProjectionExecutor) Close() error {
	return e.child.Close()
}
