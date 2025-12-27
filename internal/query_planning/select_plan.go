package query_planning

// SeqScanPlan представляет план последовательного сканирования таблицы
type SeqScanPlan struct {
	TableName string
}

func (p *SeqScanPlan) GetType() PlanNodeType {
	return PlanSeqScan
}

// FilterPlan представляет план фильтрации (WHERE)
type FilterPlan struct {
	Condition Expression
	Child     PlanNode
}

func (p *FilterPlan) GetType() PlanNodeType {
	return PlanFilter
}

// ProjectionPlan представляет план проекции колонок (SELECT)
type ProjectionPlan struct {
	Columns []string // Имена колонок для выборки, пустой массив = SELECT *
	Child   PlanNode
}

func (p *ProjectionPlan) GetType() PlanNodeType {
	return PlanProjection
}

// HashJoinPlan представляет план соединения таблиц через хеш-таблицу
type HashJoinPlan struct {
	LeftTable   string // Имя левой таблицы
	RightTable  string // Имя правой таблицы
	LeftColumn  string // Колонка из левой таблицы для JOIN
	RightColumn string // Колонка из правой таблицы для JOIN
	JoinType    string // "INNER", "LEFT", "RIGHT", "FULL"
	LeftChild   PlanNode
	RightChild  PlanNode
}

func (p *HashJoinPlan) GetType() PlanNodeType {
	return PlanHashJoin
}

// LimitPlan представляет план ограничения количества строк
type LimitPlan struct {
	Limit int64 // Количество строк для возврата
	Child PlanNode
}

func (p *LimitPlan) GetType() PlanNodeType {
	return PlanLimit
}

// SortPlan представляет план сортировки (ORDER BY)
type SortPlan struct {
	OrderBy []OrderByColumn // Колонки для сортировки
	Limit   int64           // Лимит для TOP-N Heap Sort (0 = использовать дефолт 20)
	Child   PlanNode
}

func (p *SortPlan) GetType() PlanNodeType {
	return PlanSort
}

// DistinctPlan представляет план удаления дубликатов
type DistinctPlan struct {
	Child PlanNode
}

func (p *DistinctPlan) GetType() PlanNodeType {
	return PlanDistinct
}
