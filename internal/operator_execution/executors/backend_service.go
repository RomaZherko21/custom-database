package executors

import (
	"custom-database/internal/buffer_bool"
	"custom-database/internal/operator_execution"
	"custom-database/internal/query_planning"
	"custom-database/internal/query_planning/parser"
	"fmt"
)

// Использует query_planning + operator_execution для выполнения запросов
type BackendService interface {
	// ExecuteSQL выполняет SQL запрос и возвращает результат в формате Table
	ExecuteSQL(sql string) (*operator_execution.Table, error)
}

type backendService struct {
	bufferPool      buffer_bool.BufferPoolInterface
	planner         query_planning.PlannerService
	executorService ExecutorService
}

// NewBackendService создает новый сервис для выполнения SQL запросов
func NewBackendService() (BackendService, error) {
	bufferPool, err := buffer_bool.NewBufferPool(100, 2)
	if err != nil {
		return nil, fmt.Errorf("failed to create buffer pool: %w", err)
	}

	return &backendService{
		bufferPool:      bufferPool,
		planner:         query_planning.NewPlanner(),
		executorService: NewExecutorService(bufferPool),
	}, nil
}

// ExecuteSQL выполняет SQL запрос (может содержать несколько statements через ;)
func (bs *backendService) ExecuteSQL(sql string) (*operator_execution.Table, error) {
	// Парсим запрос (может содержать несколько statements)
	parserService := parser.NewParser()
	statements, err := parserService.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	if len(statements) == 0 {
		return nil, fmt.Errorf("no statements found")
	}

	// Выполняем все statements, результат возвращаем только от последнего SELECT
	var lastResult *operator_execution.Table
	for _, stmtWithSQL := range statements {
		// Планируем запрос
		plan, err := bs.planner.Plan(stmtWithSQL)
		if err != nil {
			return nil, fmt.Errorf("failed to plan query '%s': %w", stmtWithSQL.SQL, err)
		}

		// Выполняем план
		results, err := ExecutePlan(bs.executorService, plan)
		if err != nil {
			return nil, fmt.Errorf("failed to execute plan: %w", err)
		}

		// Для DDL и INSERT возвращаем nil
		// Проверяем тип плана
		switch plan.(type) {
		case *query_planning.CreateTablePlan,
			*query_planning.DropTablePlan,
			*query_planning.CreateIndexPlan,
			*query_planning.DropIndexPlan,
			*query_planning.InsertPlan:
			// Продолжаем выполнение следующих statements
			continue
		}

		// Для SELECT преобразуем результаты в Table
		table, err := bs.convertResultsToTable(results)
		if err != nil {
			return nil, fmt.Errorf("failed to convert results: %w", err)
		}

		lastResult = table
	}

	return lastResult, nil
}

func (bs *backendService) convertResultsToTable(results []*operator_execution.Tuple) (*operator_execution.Table, error) {
	if len(results) == 0 {
		return &operator_execution.Table{
			Name:    "",
			Columns: []operator_execution.Column{},
			Rows:    [][]operator_execution.Cell{},
		}, nil
	}

	firstTuple := results[0]
	if firstTuple.Schema == nil {
		return nil, fmt.Errorf("tuple has no schema")
	}

	columns := make([]operator_execution.Column, len(firstTuple.Schema.Columns))
	for i, colInfo := range firstTuple.Schema.Columns {
		var colType operator_execution.ColumnType
		switch colInfo.Type {
		case operator_execution.ValueTypeInt:
			colType = operator_execution.IntType
		case operator_execution.ValueTypeText:
			colType = operator_execution.TextType
		default:
			colType = operator_execution.TextType
		}

		columns[i] = operator_execution.Column{
			Name: colInfo.Name,
			Type: colType,
		}
	}

	rows := make([][]operator_execution.Cell, len(results))
	for i, tuple := range results {
		cells := make([]operator_execution.Cell, len(tuple.Values))
		for j, value := range tuple.Values {
			if value.IsNull {
				cells[j] = operator_execution.MemoryCell("null")
			} else {
				switch value.Type {
				case operator_execution.ValueTypeInt:
					cells[j] = operator_execution.MemoryCell(fmt.Sprintf("%d", value.IntVal))
				case operator_execution.ValueTypeText:
					cells[j] = operator_execution.MemoryCell(value.StrVal)
				default:
					cells[j] = operator_execution.MemoryCell("null")
				}
			}
		}
		rows[i] = cells
	}

	return &operator_execution.Table{
		Name:    "",
		Columns: columns,
		Rows:    rows,
	}, nil
}
