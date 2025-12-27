package query_planning

// PlanNode представляет узел плана выполнения
type PlanNode interface {
	// GetType возвращает тип узла плана
	GetType() PlanNodeType
}

// Expression представляет выражение для WHERE условия
type Expression interface {
	GetType() ExpressionType
}
