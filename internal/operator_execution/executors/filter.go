package executors

import (
	"custom-database/internal/operator_execution"
	"custom-database/internal/query_planning"
	"errors"
)

// FilterExecutor выполняет фильтрацию (WHERE)
type FilterExecutor struct {
	child operator_execution.Executor
	plan  *query_planning.FilterPlan

	initialized bool
}

// NewFilterExecutor создает новый FilterExecutor
func NewFilterExecutor(child operator_execution.Executor, plan *query_planning.FilterPlan) operator_execution.Executor {
	return &FilterExecutor{
		child: child,
		plan:  plan,
	}
}

// Init инициализирует executor
func (e *FilterExecutor) Init() error {
	if err := e.child.Init(); err != nil {
		return err
	}
	e.initialized = true
	return nil
}

// Next возвращает следующий tuple, прошедший фильтрацию
func (e *FilterExecutor) Next() (*operator_execution.Tuple, error) {
	if !e.initialized {
		if err := e.Init(); err != nil {
			return nil, err
		}
	}

	for {
		tuple, err := e.child.Next()
		if err != nil {
			return nil, err
		}

		if tuple == nil {
			return nil, nil
		}

		// Проверяем условие фильтрации
		matches, err := evaluateExpression(e.plan.Condition, tuple)
		if err != nil {
			return nil, err
		}

		if matches {
			return tuple, nil
		}
		// Если не совпадает, продолжаем искать следующий tuple
	}
}

// Close освобождает ресурсы
func (e *FilterExecutor) Close() error {
	return e.child.Close()
}

// evaluateExpression вычисляет выражение для кортежа
func evaluateExpression(expr query_planning.Expression, tuple *operator_execution.Tuple) (bool, error) {
	switch e := expr.(type) {
	case *query_planning.ComparisonExpression:
		return evaluateComparison(e, tuple)
	case *query_planning.LogicExpression:
		return evaluateLogic(e, tuple)
	default:
		return false, nil
	}
}

// evaluateComparison вычисляет выражение сравнения
func evaluateComparison(expr *query_planning.ComparisonExpression, tuple *operator_execution.Tuple) (bool, error) {
	leftVal, err := evaluateExpressionValue(expr.Left, tuple)
	if err != nil {
		return false, err
	}

	rightVal, err := evaluateExpressionValue(expr.Right, tuple)
	if err != nil {
		return false, err
	}

	// Сравниваем значения в зависимости от оператора
	switch expr.Operator {
	case "=":
		return compareValues(leftVal, rightVal) == 0, nil
	case "!=":
		return compareValues(leftVal, rightVal) != 0, nil
	case "<":
		return compareValues(leftVal, rightVal) < 0, nil
	case ">":
		return compareValues(leftVal, rightVal) > 0, nil
	case "<=":
		return compareValues(leftVal, rightVal) <= 0, nil
	case ">=":
		return compareValues(leftVal, rightVal) >= 0, nil
	default:
		return false, nil
	}
}

// evaluateLogic вычисляет логическое выражение
func evaluateLogic(expr *query_planning.LogicExpression, tuple *operator_execution.Tuple) (bool, error) {
	left, err := evaluateExpression(expr.Left, tuple)
	if err != nil {
		return false, err
	}

	right, err := evaluateExpression(expr.Right, tuple)
	if err != nil {
		return false, err
	}

	switch expr.Operator {
	case "AND":
		return left && right, nil
	case "OR":
		return left || right, nil
	default:
		return false, nil
	}
}

// evaluateExpressionValue вычисляет значение выражения (колонка или константа)
func evaluateExpressionValue(expr query_planning.Expression, tuple *operator_execution.Tuple) (*operator_execution.Value, error) {
	switch e := expr.(type) {
	case *query_planning.ColumnExpression:
		// Получаем значение колонки из кортежа
		value := tuple.GetValueByName(e.ColumnName)
		if value == nil {
			return nil, errors.New("column not found")
		}
		return value, nil
	case *query_planning.ConstantExpression:
		// Возвращаем константное значение
		return &operator_execution.Value{
			Type:   convertValueType(e.Value.Type),
			IntVal: e.Value.IntVal,
			StrVal: e.Value.StrVal,
			IsNull: e.Value.IsNull,
		}, nil
	default:
		return nil, nil
	}
}

// convertValueType преобразует query_planning.ValueType в operator_execution.ValueType
func convertValueType(vt query_planning.ValueType) operator_execution.ValueType {
	switch vt {
	case query_planning.ValueInt:
		return operator_execution.ValueTypeInt
	case query_planning.ValueText:
		return operator_execution.ValueTypeText
	case query_planning.ValueNull:
		return operator_execution.ValueTypeNull
	default:
		return operator_execution.ValueTypeNull
	}
}

// compareValues сравнивает два значения
// Возвращает: -1 если left < right, 0 если left == right, 1 если left > right
func compareValues(left, right *operator_execution.Value) int {
	// NULL сравнения
	if left.IsNull && right.IsNull {
		return 0
	}
	if left.IsNull {
		return -1
	}
	if right.IsNull {
		return 1
	}

	// Сравнение по типу
	if left.Type != right.Type {
		// Разные типы - сравниваем как строки
		return compareStrings(left.String(), right.String())
	}

	switch left.Type {
	case operator_execution.ValueTypeInt:
		if left.IntVal < right.IntVal {
			return -1
		}
		if left.IntVal > right.IntVal {
			return 1
		}
		return 0
	case operator_execution.ValueTypeText:
		return compareStrings(left.StrVal, right.StrVal)
	default:
		return 0
	}
}

// compareStrings сравнивает две строки
func compareStrings(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
