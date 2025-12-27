package query_planning

// ComparisonExpression представляет выражение сравнения (col = value, col > value и т.д.)
type ComparisonExpression struct {
	Left     Expression
	Right    Expression
	Operator string // "=", "!=", "<", ">", "<=", ">="
}

func (e *ComparisonExpression) GetType() ExpressionType {
	return ExprComparison
}

// LogicExpression представляет логическое выражение (AND, OR)
type LogicExpression struct {
	Left     Expression
	Right    Expression
	Operator string // "AND", "OR"
}

func (e *LogicExpression) GetType() ExpressionType {
	return ExprLogic
}

// ColumnExpression представляет ссылку на колонку
type ColumnExpression struct {
	ColumnName string
	TableName  string // Может быть пустым
}

func (e *ColumnExpression) GetType() ExpressionType {
	return ExprColumn
}

// ConstantExpression представляет константное значение
type ConstantExpression struct {
	Value Value
}

func (e *ConstantExpression) GetType() ExpressionType {
	return ExprConstant
}
