package query_planning

// DropTablePlan представляет план удаления таблицы
type DropTablePlan struct {
	TableName string
}

func (p *DropTablePlan) GetType() PlanNodeType {
	return PlanDropTable
}

// DropIndexPlan представляет план удаления индекса
type DropIndexPlan struct {
	IndexName string
}

func (p *DropIndexPlan) GetType() PlanNodeType {
	return PlanDropIndex
}

