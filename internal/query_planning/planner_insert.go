package query_planning

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

// planInsert планирует INSERT
func (p *planner) planInsert(ins *sqlparser.Insert) (PlanNode, error) {
	tableName := ins.Table.Name.String()
	if tableName == "" {
		return nil, fmt.Errorf("INSERT: table name is empty")
	}

	if ins.Rows == nil {
		return nil, fmt.Errorf("INSERT: no values provided")
	}

	values, ok := ins.Rows.(sqlparser.Values)
	if !ok {
		return nil, fmt.Errorf("INSERT: must use VALUES clause")
	}

	if len(values) == 0 {
		return nil, fmt.Errorf("INSERT: no rows to insert")
	}

	// Преобразуем значения
	rows := [][]Value{}
	for _, row := range values {
		rowValues := []Value{}
		for _, expr := range row {
			val, err := p.convertExprToValue(expr)
			if err != nil {
				return nil, err
			}
			rowValues = append(rowValues, val)
		}
		rows = append(rows, rowValues)
	}

	return &InsertPlan{
		TableName: tableName,
		Values:    rows,
	}, nil
}
