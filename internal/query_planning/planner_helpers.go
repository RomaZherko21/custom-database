package query_planning

import (
	"fmt"
	"strconv"

	"github.com/xwb1989/sqlparser"
)

// convertExprToValue преобразует sqlparser.Expr в Value
func (p *planner) convertExprToValue(expr sqlparser.Expr) (Value, error) {
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		switch e.Type {
		case sqlparser.IntVal:
			val, err := strconv.Atoi(string(e.Val))
			if err != nil {
				return Value{}, err
			}
			return Value{
				Type:   ValueInt,
				IntVal: int32(val),
			}, nil
		case sqlparser.StrVal:
			return Value{
				Type:   ValueText,
				StrVal: string(e.Val),
			}, nil
		default:
			return Value{}, fmt.Errorf("unsupported value type: %v", e.Type)
		}
	case *sqlparser.NullVal:
		return Value{
			Type:   ValueNull,
			IsNull: true,
		}, nil
	default:
		// Пробуем извлечь значение из строки
		valStr := sqlparser.String(expr)
		if valStr == "null" || valStr == "NULL" {
			return Value{
				Type:   ValueNull,
				IsNull: true,
			}, nil
		}
		// Пробуем как число
		if val, err := strconv.Atoi(valStr); err == nil {
			return Value{
				Type:   ValueInt,
				IntVal: int32(val),
			}, nil
		}
		// Иначе как строку
		return Value{
			Type:   ValueText,
			StrVal: valStr,
		}, nil
	}
}

// extractColumnName извлекает имя колонки из выражения SELECT
func (p *planner) extractColumnName(expr sqlparser.Expr) (string, error) {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		return e.Name.String(), nil
	default:
		// Для других выражений возвращаем пустую строку
		// В будущем можно поддерживать выражения типа col1 + col2
		return "", nil
	}
}
