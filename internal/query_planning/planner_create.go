package query_planning

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// planCreateTable планирует CREATE TABLE
func (p *planner) planCreateTable(ddl *sqlparser.DDL) (PlanNode, error) {
	tableName := ddl.NewName.Name.String()
	if tableName == "" {
		return nil, fmt.Errorf("CREATE TABLE: table name is empty")
	}

	columns := []ColumnDefinition{}
	for _, col := range ddl.TableSpec.Columns {
		colDef := ColumnDefinition{
			Name:     col.Name.String(),
			DataType: strings.ToUpper(col.Type.Type),
		}
		columns = append(columns, colDef)
	}

	return &CreateTablePlan{
		TableName: tableName,
		Columns:   columns,
	}, nil
}

// planCreateIndex планирует CREATE INDEX
func (p *planner) planCreateIndex(ddl *sqlparser.DDL, originalSQL string) (PlanNode, error) {
	// Парсим CREATE INDEX из исходного SQL
	indexName, tableName, columnName, err := parseCreateIndexSQL(originalSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CREATE INDEX: %v", err)
	}

	return &CreateIndexPlan{
		IndexName:  indexName,
		TableName:  tableName,
		ColumnName: columnName,
	}, nil
}

// parseCreateIndexSQL парсит CREATE INDEX из SQL (вспомогательная функция)
func parseCreateIndexSQL(sql string) (indexName, tableName, columnName string, err error) {
	sql = strings.TrimSpace(sql)
	sqlUpper := strings.ToUpper(sql)

	if !strings.HasPrefix(sqlUpper, "CREATE INDEX") {
		return "", "", "", fmt.Errorf("not a CREATE INDEX statement")
	}

	// Простой парсинг через регулярное выражение
	// CREATE INDEX index_name ON table_name (column_name)
	parts := strings.Fields(sql)
	if len(parts) < 6 {
		return "", "", "", fmt.Errorf("invalid CREATE INDEX syntax")
	}

	indexName = parts[2]
	if strings.ToUpper(parts[3]) != "ON" {
		return "", "", "", fmt.Errorf("expected ON in CREATE INDEX")
	}
	tableName = parts[4]

	// Извлекаем имя колонки из скобок
	colPart := parts[5]
	colPart = strings.TrimPrefix(colPart, "(")
	colPart = strings.TrimSuffix(colPart, ")")
	columnName = colPart

	return indexName, tableName, columnName, nil
}

