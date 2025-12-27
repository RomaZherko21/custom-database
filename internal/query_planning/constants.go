package query_planning

// PlanNodeType определяет тип узла плана выполнения
type PlanNodeType string

const (
	// DDL операции
	PlanCreateTable PlanNodeType = "CREATE_TABLE"
	PlanDropTable   PlanNodeType = "DROP_TABLE"
	PlanCreateIndex PlanNodeType = "CREATE_INDEX"
	PlanDropIndex   PlanNodeType = "DROP_INDEX"

	// DML операции
	PlanInsert PlanNodeType = "INSERT"

	// Query операции
	PlanSeqScan    PlanNodeType = "SEQ_SCAN"
	PlanIndexScan  PlanNodeType = "INDEX_SCAN"
	PlanFilter     PlanNodeType = "FILTER"
	PlanProjection PlanNodeType = "PROJECTION"
	PlanHashJoin   PlanNodeType = "HASH_JOIN"
	PlanLimit      PlanNodeType = "LIMIT"
	PlanSort       PlanNodeType = "SORT"
	PlanDistinct   PlanNodeType = "DISTINCT"
)

// ExpressionType определяет тип выражения
type ExpressionType string

const (
	ExprComparison ExpressionType = "COMPARISON"
	ExprLogic      ExpressionType = "LOGIC"
	ExprColumn     ExpressionType = "COLUMN"
	ExprConstant   ExpressionType = "CONSTANT"
)

// ValueType определяет тип значения
type ValueType string

const (
	ValueInt  ValueType = "INT"
	ValueText ValueType = "TEXT"
	ValueNull ValueType = "NULL"
)
