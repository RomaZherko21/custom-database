package query_planning

// CreateTablePlan представляет план создания таблицы
type CreateTablePlan struct {
	TableName string
	Columns   []ColumnDefinition
}

func (p *CreateTablePlan) GetType() PlanNodeType {
	return PlanCreateTable
}

// CreateIndexPlan представляет план создания индекса
type CreateIndexPlan struct {
	IndexName  string
	TableName  string
	ColumnName string
}

func (p *CreateIndexPlan) GetType() PlanNodeType {
	return PlanCreateIndex
}

