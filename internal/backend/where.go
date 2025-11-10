package backend

// import (
// 	"custom-database/internal/parser/ast"
// 	"custom-database/internal/parser/lex"
// 	"fmt"
// )

// func (mb *memoryBackend) filterRows(columns []Column, rows [][]interface{}, whereClause *ast.WhereClause) [][]interface{} {
// 	filteredRows := [][]interface{}{}
// 	for _, row := range rows {
// 		if mb.filterRow(columns, row, whereClause) {
// 			filteredRows = append(filteredRows, row)
// 		}
// 	}

// 	return filteredRows
// }

// func (mb *memoryBackend) filterRow(columns []Column, row []interface{}, whereClause *ast.WhereClause) bool {
// 	if whereClause == nil {
// 		return true
// 	}

// 	if whereClause.Token.Kind == lex.LogicalOperatorToken {
// 		if whereClause.Token.Value == string(lex.AndOperator) {
// 			return mb.filterRow(columns, row, whereClause.Left) && mb.filterRow(columns, row, whereClause.Right)
// 		}

// 		if whereClause.Token.Value == string(lex.OrOperator) {
// 			return mb.filterRow(columns, row, whereClause.Left) || mb.filterRow(columns, row, whereClause.Right)
// 		}
// 	}

// 	if whereClause.Token.Kind == lex.MathOperatorToken {
// 		leftVal, err := mb.getValueFromExpression(columns, row, whereClause.Left)
// 		if err != nil {
// 			return false
// 		}

// 		rightVal, err := mb.getValueFromExpression(columns, row, whereClause.Right)
// 		if err != nil {
// 			return false
// 		}

// 		return mb.evaluateCondition(leftVal, rightVal, whereClause.Token.Value)
// 	}

// 	return mb.filterRow(columns, row, whereClause)
// }

// func (mb *memoryBackend) getValueFromExpression(columns []Column, row []interface{}, expr *ast.WhereClause) (interface{}, error) {
// 	if expr.Token.Kind == lex.IdentifierToken {
// 		for i, column := range columns {
// 			if column.Name == expr.Token.Value {
// 				return row[i], nil
// 			}
// 		}
// 		return nil, fmt.Errorf("column not found: %s", expr.Token.Value)
// 	}
// 	return expr.Token.Value, nil
// }

// func (mb *memoryBackend) evaluateCondition(left, right interface{}, operator string) bool {
// 	switch operator {
// 	case string(lex.EqualOperator):
// 		return fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right)
// 	case string(lex.NotEqualOperator):
// 		return fmt.Sprintf("%v", left) != fmt.Sprintf("%v", right)
// 	case string(lex.GreaterThanOperator):
// 		return fmt.Sprintf("%v", left) > fmt.Sprintf("%v", right)
// 	case string(lex.LessThanOperator):
// 		return fmt.Sprintf("%v", left) < fmt.Sprintf("%v", right)
// 	default:
// 		return false
// 	}
// }
