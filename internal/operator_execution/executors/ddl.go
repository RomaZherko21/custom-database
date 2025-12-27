package executors

import (
	"custom-database/internal/buffer_bool"
	"custom-database/internal/disk_manager"
	"custom-database/internal/operator_execution"
	"custom-database/internal/query_planning"
	"fmt"
)

// CreateTableExecutor выполняет CREATE TABLE
type CreateTableExecutor struct {
	bufferPool buffer_bool.BufferPoolInterface
	plan       *query_planning.CreateTablePlan
	executed   bool
}

// NewCreateTableExecutor создает новый CreateTableExecutor
func NewCreateTableExecutor(bufferPool buffer_bool.BufferPoolInterface, plan *query_planning.CreateTablePlan) operator_execution.Executor {
	return &CreateTableExecutor{
		bufferPool: bufferPool,
		plan:       plan,
		executed:   false,
	}
}

// Init инициализирует executor
func (e *CreateTableExecutor) Init() error {
	return nil
}

// Next выполняет CREATE TABLE и возвращает nil (DDL не возвращает данные)
func (e *CreateTableExecutor) Next() (*operator_execution.Tuple, error) {
	if e.executed {
		return nil, nil
	}

	// Преобразуем колонки из плана в disk_manager.ColumnInfo
	columns := make([]disk_manager.ColumnInfo, len(e.plan.Columns))
	for i, col := range e.plan.Columns {
		var dt disk_manager.DataType
		switch col.DataType {
		case "INT":
			dt = disk_manager.INT_32_TYPE
		case "TEXT":
			dt = disk_manager.TEXT_TYPE
		default:
			return nil, fmt.Errorf("unsupported data type: %s", col.DataType)
		}

		columns[i] = disk_manager.ColumnInfo{
			ColumnNameLength: uint32(len(col.Name)),
			DataType:         dt,
			IsNullable:       0,
			IsPrimaryKey:     0,
			IsAutoIncrement:  0,
			DefaultValue:     0,
			ColumnName:       col.Name,
		}
	}

	// Создаем таблицу
	err := e.bufferPool.CreateTable(e.plan.TableName, columns)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	e.executed = true
	return nil, nil
}

// Close освобождает ресурсы
func (e *CreateTableExecutor) Close() error {
	return nil
}

// DropTableExecutor выполняет DROP TABLE
type DropTableExecutor struct {
	bufferPool buffer_bool.BufferPoolInterface
	plan       *query_planning.DropTablePlan
	executed   bool
}

// NewDropTableExecutor создает новый DropTableExecutor
func NewDropTableExecutor(bufferPool buffer_bool.BufferPoolInterface, plan *query_planning.DropTablePlan) operator_execution.Executor {
	return &DropTableExecutor{
		bufferPool: bufferPool,
		plan:       plan,
		executed:   false,
	}
}

// Init инициализирует executor
func (e *DropTableExecutor) Init() error {
	return nil
}

// Next выполняет DROP TABLE и возвращает nil (DDL не возвращает данные)
func (e *DropTableExecutor) Next() (*operator_execution.Tuple, error) {
	if e.executed {
		return nil, nil
	}

	err := e.bufferPool.DropTable(e.plan.TableName)
	if err != nil {
		return nil, fmt.Errorf("failed to drop table: %w", err)
	}

	e.executed = true
	return nil, nil
}

// Close освобождает ресурсы
func (e *DropTableExecutor) Close() error {
	return nil
}
