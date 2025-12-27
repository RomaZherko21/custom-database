package executors

import (
	"custom-database/internal/buffer_bool"
	"custom-database/internal/operator_execution"
	"custom-database/internal/query_planning"
	"fmt"
)

// HashJoinExecutor выполняет JOIN через хеш-таблицу
// Реализует алгоритм Hash Join с двумя фазами:
// Phase #1: Build - сканируем outer relation и заполняем хеш-таблицу
// Phase #2: Probe - сканируем inner relation и ищем совпадения
type HashJoinExecutor struct {
	leftChild  operator_execution.Executor
	rightChild operator_execution.Executor
	plan       *query_planning.HashJoinPlan
	bufferPool buffer_bool.BufferPoolInterface

	swapped bool // Флаг, указывающий, были ли поменяны местами left и right

	// Phase #1: Build
	hashTable   map[string][]*operator_execution.Tuple // Хеш-таблица: ключ -> список tupleей
	buildSchema *operator_execution.Schema             // Схема build таблицы
	buildChild  operator_execution.Executor            // Executor для build таблицы
	buildColumn string                                 // Имя колонки для build

	// Phase #2: Probe
	probeIterator     operator_execution.Executor // Итератор для probe таблицы
	currentProbeTuple *operator_execution.Tuple   // Текущий tuple из probe
	currentMatches    []*operator_execution.Tuple // Текущие совпадения из хеш-таблицы
	matchIndex        int                         // Индекс текущего совпадения
	probeColumn       string                      // Имя колонки для probe
}

// NewHashJoinExecutor создает новый HashJoinExecutor
func NewHashJoinExecutor(leftChild, rightChild operator_execution.Executor, plan *query_planning.HashJoinPlan, bufferPool buffer_bool.BufferPoolInterface) operator_execution.Executor {
	return &HashJoinExecutor{
		leftChild:  leftChild,
		rightChild: rightChild,
		plan:       plan,
		bufferPool: bufferPool,
		hashTable:  make(map[string][]*operator_execution.Tuple),
		matchIndex: -1,
		swapped:    false,
	}
}

// Init инициализирует executor и выполняет Phase #1: Build
func (e *HashJoinExecutor) Init() error {
	// Инициализируем дочерние executor'ы
	if err := e.leftChild.Init(); err != nil {
		return fmt.Errorf("failed to init left child: %w", err)
	}

	if err := e.rightChild.Init(); err != nil {
		return fmt.Errorf("failed to init right child: %w", err)
	}

	// Определяем, какая таблица меньше, чтобы использовать её для build
	// Получаем метаинформацию для обеих таблиц
	leftMetaInfo, err := e.bufferPool.ReadMetaInfo(e.plan.LeftTable)
	if err != nil {
		return fmt.Errorf("failed to read meta info for left table %s: %w", e.plan.LeftTable, err)
	}

	rightMetaInfo, err := e.bufferPool.ReadMetaInfo(e.plan.RightTable)
	if err != nil {
		return fmt.Errorf("failed to read meta info for right table %s: %w", e.plan.RightTable, err)
	}

	// Сравниваем размеры таблиц по количеству страниц
	leftSize := len(leftMetaInfo.PageDirectory.Entries)
	rightSize := len(rightMetaInfo.PageDirectory.Entries)

	// Если правая таблица меньше, меняем местами
	if rightSize < leftSize {
		e.swapped = true
		e.buildChild = e.rightChild
		e.probeIterator = e.leftChild
		e.buildColumn = e.plan.RightColumn
		e.probeColumn = e.plan.LeftColumn
	} else {
		e.swapped = false
		e.buildChild = e.leftChild
		e.probeIterator = e.rightChild
		e.buildColumn = e.plan.LeftColumn
		e.probeColumn = e.plan.RightColumn
	}

	// Phase #1: Build - заполняем хеш-таблицу из buildChild
	// Сканируем все tupleи из buildChild
	for {
		tuple, err := e.buildChild.Next()
		if err != nil {
			return fmt.Errorf("failed to get tuple from build relation: %w", err)
		}

		if tuple == nil {
			// Больше tupleей нет
			break
		}

		// Сохраняем схему build таблицы (берем из первого tupleа)
		if e.buildSchema == nil {
			e.buildSchema = tuple.Schema
		}

		// Получаем значение join колонки из build таблицы
		joinValue := e.getJoinValue(tuple, e.buildColumn)
		if joinValue == nil {
			// NULL значения пропускаем для INNER JOIN
			continue
		}

		// Вычисляем хеш-ключ
		hashKey := e.computeHashKey(joinValue)

		// Добавляем tuple в хеш-таблицу
		if e.hashTable[hashKey] == nil {
			e.hashTable[hashKey] = make([]*operator_execution.Tuple, 0)
		}
		e.hashTable[hashKey] = append(e.hashTable[hashKey], tuple)
	}

	// Закрываем buildChild, так как мы уже прочитали все данные
	if err := e.buildChild.Close(); err != nil {
		return fmt.Errorf("failed to close build child: %w", err)
	}

	// Phase #2: Probe - инициализируем probe iterator
	e.currentProbeTuple = nil
	e.currentMatches = nil
	e.matchIndex = -1

	return nil
}

// Next возвращает следующий объединенный tuple
func (e *HashJoinExecutor) Next() (*operator_execution.Tuple, error) {
	// Phase #2: Probe
	for {
		// Если у нас есть текущие совпадения, возвращаем следующее
		if e.currentMatches != nil && e.matchIndex >= 0 && e.matchIndex < len(e.currentMatches) {
			buildTuple := e.currentMatches[e.matchIndex]
			e.matchIndex++

			// Создаем объединенный tuple
			joinedTuple, err := e.joinTuples(buildTuple, e.currentProbeTuple)
			if err != nil {
				return nil, err
			}

			// Если все совпадения обработаны, сбрасываем
			if e.matchIndex >= len(e.currentMatches) {
				e.currentMatches = nil
				e.matchIndex = -1
			}

			return joinedTuple, nil
		}

		// Получаем следующий tuple из probe таблицы
		probeTuple, err := e.probeIterator.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to get tuple from probe relation: %w", err)
		}

		if probeTuple == nil {
			// Больше tupleей нет
			return nil, nil
		}

		e.currentProbeTuple = probeTuple

		// Получаем значение join колонки из probe таблицы
		joinValue := e.getJoinValue(probeTuple, e.probeColumn)
		if joinValue == nil {
			// NULL значения пропускаем для INNER JOIN
			continue
		}

		// Вычисляем хеш-ключ
		hashKey := e.computeHashKey(joinValue)

		// Ищем совпадения в хеш-таблице
		e.currentMatches = e.hashTable[hashKey]
		if len(e.currentMatches) == 0 {
			// Нет совпадений, переходим к следующему tupleу
			continue
		}

		// Начинаем с первого совпадения
		e.matchIndex = 0
	}
}

// getJoinValue получает значение join колонки из tuple
func (e *HashJoinExecutor) getJoinValue(tuple *operator_execution.Tuple, columnName string) *operator_execution.Value {
	if tuple == nil || tuple.Schema == nil {
		return nil
	}

	// Пытаемся найти колонку по имени
	value := tuple.GetValueByName(columnName)
	if value != nil {
		return value
	}

	// Если не нашли, возможно колонка может быть с префиксом таблицы (например, "users.id" или "u.id")
	// Пробуем найти по последней части после точки
	if idx := len(columnName) - 1; idx >= 0 {
		for i := len(columnName) - 1; i >= 0; i-- {
			if columnName[i] == '.' {
				// Нашли точку, берем часть после точки
				shortName := columnName[i+1:]
				value = tuple.GetValueByName(shortName)
				if value != nil {
					return value
				}
				break
			}
		}
	}

	return nil
}

// computeHashKey вычисляет хеш-ключ из значения
func (e *HashJoinExecutor) computeHashKey(value *operator_execution.Value) string {
	if value == nil || value.IsNull {
		return ""
	}

	// Используем строковое представление значения как ключ
	// В реальной реализации можно использовать более эффективный хеш
	return value.String()
}

// joinTuples объединяет два tupleа в один
func (e *HashJoinExecutor) joinTuples(buildTuple, probeTuple *operator_execution.Tuple) (*operator_execution.Tuple, error) {
	if buildTuple == nil || probeTuple == nil {
		return nil, fmt.Errorf("cannot join nil tuples")
	}

	// В зависимости от того, были ли поменяны местами таблицы,
	// порядок объединения может быть разным
	var leftTuple, rightTuple *operator_execution.Tuple
	if e.swapped {
		// Если поменяли местами, то build - это правая таблица, probe - левая
		leftTuple = probeTuple
		rightTuple = buildTuple
	} else {
		// Обычный порядок: build - левая, probe - правая
		leftTuple = buildTuple
		rightTuple = probeTuple
	}

	// Объединяем значения из обоих tupleей
	joinedValues := make([]operator_execution.Value, 0, len(leftTuple.Values)+len(rightTuple.Values))
	joinedValues = append(joinedValues, leftTuple.Values...)
	joinedValues = append(joinedValues, rightTuple.Values...)

	// Объединяем схемы
	joinedColumns := make([]operator_execution.ColumnInfo, 0, len(leftTuple.Schema.Columns)+len(rightTuple.Schema.Columns))
	joinedColumns = append(joinedColumns, leftTuple.Schema.Columns...)
	joinedColumns = append(joinedColumns, rightTuple.Schema.Columns...)

	joinedSchema := &operator_execution.Schema{
		Columns: joinedColumns,
	}

	return operator_execution.NewTuple(joinedValues, joinedSchema), nil
}

// Close освобождает ресурсы
func (e *HashJoinExecutor) Close() error {
	var err error

	if e.leftChild != nil {
		if closeErr := e.leftChild.Close(); closeErr != nil {
			err = closeErr
		}
	}

	if e.rightChild != nil {
		if closeErr := e.rightChild.Close(); closeErr != nil {
			err = closeErr
		}
	}

	// Очищаем хеш-таблицу
	e.hashTable = make(map[string][]*operator_execution.Tuple)

	return err
}
