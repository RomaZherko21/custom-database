package executors

import (
	"custom-database/internal/operator_execution"
	"custom-database/internal/query_planning"
	"fmt"
	"sort"
)

const defaultLimit = 20 // Дефолтный лимит, если LIMIT не указан

// SortExecutor реализует сортировку для ORDER BY + LIMIT
// Использует отсортированный слайс с проверкой на добавление
type SortExecutor struct {
	child operator_execution.Executor
	plan  *query_planning.SortPlan

	// Отсортированный слайс для хранения TOP-N tuples
	sorted []*operator_execution.Tuple

	maxSize int // Максимальный размер слайса (LIMIT)
}

// NewSortExecutor создает новый SortExecutor
func NewSortExecutor(child operator_execution.Executor, plan *query_planning.SortPlan) operator_execution.Executor {
	limit := plan.Limit
	if limit == 0 {
		limit = defaultLimit
	}

	return &SortExecutor{
		child:   child,
		plan:    plan,
		sorted:  make([]*operator_execution.Tuple, 0, limit),
		maxSize: int(limit),
	}
}

// Init инициализирует executor и собирает TOP-N элементов
func (e *SortExecutor) Init() error {
	if err := e.child.Init(); err != nil {
		return fmt.Errorf("failed to init child executor: %w", err)
	}

	// Собираем и сортируем TOP-N элементов
	if err := e.collectAndSort(); err != nil {
		return fmt.Errorf("failed to collect tuples: %w", err)
	}

	return nil
}

func (e *SortExecutor) collectAndSort() error {
	for {
		tuple, err := e.child.Next()
		if err != nil {
			return err
		}

		if tuple == nil {
			break
		}

		// Если слайс ещё не заполнен — добавляем и сортируем
		if len(e.sorted) < e.maxSize {
			e.sorted = append(e.sorted, tuple)
			e.sortSlice()
			continue
		}

		last := e.sorted[len(e.sorted)-1]
		comparison := e.compareTuples(tuple, last)

		if comparison < 0 {
			e.sorted = append(e.sorted, tuple)
			e.sortSlice()
			e.sorted = e.sorted[:e.maxSize]
		}
	}

	return nil
}

func (e *SortExecutor) sortSlice() {
	sort.Slice(e.sorted, func(i, j int) bool {
		return e.compareTuples(e.sorted[i], e.sorted[j]) < 0
	})
}

func (e *SortExecutor) compareTuples(t1, t2 *operator_execution.Tuple) int {
	return t1.Compare(t2, e.plan.OrderBy)
}

func (e *SortExecutor) Next() (*operator_execution.Tuple, error) {
	if len(e.sorted) == 0 {
		return nil, nil
	}

	tuple := e.sorted[0]
	e.sorted = e.sorted[1:]

	return tuple, nil
}

func (e *SortExecutor) Close() error {
	e.sorted = nil
	return e.child.Close()
}
