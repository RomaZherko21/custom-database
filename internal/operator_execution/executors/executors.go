package executors

import (
	"custom-database/internal/buffer_bool"
	"custom-database/internal/operator_execution"
	"custom-database/internal/query_planning"
	"fmt"
)

type ExecutorService interface {
	// CreateExecutor создает executor дерево из query plan
	CreateExecutor(plan query_planning.PlanNode) (operator_execution.Executor, error)
}

type executorService struct {
	bufferPool buffer_bool.BufferPoolInterface
}

func NewExecutorService(bufferPool buffer_bool.BufferPoolInterface) ExecutorService {
	return &executorService{
		bufferPool: bufferPool,
	}
}

// ExecutePlan выполняет план и возвращает результаты
func ExecutePlan(executorService ExecutorService, plan query_planning.PlanNode) ([]*operator_execution.Tuple, error) {
	// Создаем executor дерево из плана
	executor, err := executorService.CreateExecutor(plan)
	if err != nil {
		return nil, fmt.Errorf("failed to create executor: %w", err)
	}

	// Инициализируем executor
	if err := executor.Init(); err != nil {
		return nil, fmt.Errorf("failed to init executor: %w", err)
	}
	defer executor.Close()

	// Собираем все результаты
	results := []*operator_execution.Tuple{}
	for {
		tuple, err := executor.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to get next tuple: %w", err)
		}

		if tuple == nil {
			// Больше данных нет
			break
		}

		results = append(results, tuple)
	}

	return results, nil
}

// CreateExecutor создает executor дерево из query plan
func (es *executorService) CreateExecutor(plan query_planning.PlanNode) (operator_execution.Executor, error) {
	switch p := plan.(type) {
	// CreateTable, DropTable операции
	case *query_planning.CreateTablePlan:
		return NewCreateTableExecutor(es.bufferPool, p), nil
	case *query_planning.DropTablePlan:
		return NewDropTableExecutor(es.bufferPool, p), nil

	// Insert операции
	case *query_planning.InsertPlan:
		return NewInsertExecutor(es.bufferPool, p), nil

		// Query операции (дерево executorов)
	case *query_planning.SeqScanPlan:
		return NewSeqScanExecutor(es.bufferPool, p), nil
	case *query_planning.ProjectionPlan:
		return es.createProjectionExecutor(p)
	case *query_planning.FilterPlan:
		return es.createFilterExecutor(p)
	case *query_planning.LimitPlan:
		return es.createLimitExecutor(p)
	case *query_planning.SortPlan:
		return es.createSortExecutor(p)
	case *query_planning.DistinctPlan:
		return es.createDistinctExecutor(p)
	case *query_planning.HashJoinPlan:
		return es.createHashJoinExecutor(p)

	default:
		return nil, fmt.Errorf("unsupported plan type: %T", plan)
	}
}

func (es *executorService) createProjectionExecutor(plan *query_planning.ProjectionPlan) (operator_execution.Executor, error) {
	child, err := es.CreateExecutor(plan.Child)
	if err != nil {
		return nil, err
	}
	return NewProjectionExecutor(child, plan), nil
}

func (es *executorService) createFilterExecutor(plan *query_planning.FilterPlan) (operator_execution.Executor, error) {
	child, err := es.CreateExecutor(plan.Child)
	if err != nil {
		return nil, err
	}
	return NewFilterExecutor(child, plan), nil
}

func (es *executorService) createLimitExecutor(plan *query_planning.LimitPlan) (operator_execution.Executor, error) {
	child, err := es.CreateExecutor(plan.Child)
	if err != nil {
		return nil, err
	}
	return NewLimitExecutor(child, plan), nil
}

func (es *executorService) createSortExecutor(plan *query_planning.SortPlan) (operator_execution.Executor, error) {
	child, err := es.CreateExecutor(plan.Child)
	if err != nil {
		return nil, err
	}
	return NewSortExecutor(child, plan), nil
}

func (es *executorService) createDistinctExecutor(plan *query_planning.DistinctPlan) (operator_execution.Executor, error) {
	child, err := es.CreateExecutor(plan.Child)
	if err != nil {
		return nil, err
	}
	return NewDistinctExecutor(child), nil
}

func (es *executorService) createHashJoinExecutor(plan *query_planning.HashJoinPlan) (operator_execution.Executor, error) {
	leftChild, err := es.CreateExecutor(plan.LeftChild)
	if err != nil {
		return nil, err
	}

	rightChild, err := es.CreateExecutor(plan.RightChild)
	if err != nil {
		return nil, err
	}

	return NewHashJoinExecutor(leftChild, rightChild, plan, es.bufferPool), nil
}
