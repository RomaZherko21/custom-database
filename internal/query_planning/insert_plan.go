package query_planning

// InsertPlan представляет план вставки данных
type InsertPlan struct {
	TableName string
	Values    [][]Value
}

func (p *InsertPlan) GetType() PlanNodeType {
	return PlanInsert
}
