package query_planning

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// planSelect планирует SELECT запрос
func (p *planner) planSelect(sel *sqlparser.Select) (PlanNode, error) {
	if len(sel.From) == 0 {
		return nil, fmt.Errorf("SELECT: FROM clause is required")
	}

	// Строим план c листьев к корню:
	// 1. SeqScan/IndexScan (листья)
	// 2. JOIN
	// 3. Filter (WHERE)
	// 4. Sort (ORDER BY) - может включать LIMIT для TOP-N Heap Sort
	// 5. Distinct (DISTINCT)
	// 6. Limit (LIMIT) - если не обработан в SortPlan
	// 7. Projection (конкретные колонки в SELECT запросе)

	// Обрабатываем FROM: может быть одна таблица или JOIN
	var currentPlan PlanNode
	tableExpr := sel.From[0]

	switch expr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		// Одна таблица
		tableName, ok := expr.Expr.(sqlparser.TableName)
		if !ok {
			return nil, fmt.Errorf("SELECT: expected TableName in FROM")
		}
		tableNameStr := tableName.Name.String()
		currentPlan = &SeqScanPlan{
			TableName: tableNameStr,
		}

	case *sqlparser.JoinTableExpr:
		// JOIN двух таблиц
		joinPlan, err := p.planJoin(expr)
		if err != nil {
			return nil, err
		}
		currentPlan = joinPlan

	default:
		return nil, fmt.Errorf("SELECT: unsupported table expression type: %T", tableExpr)
	}

	// Добавляем Filter, если есть WHERE
	if sel.Where != nil {
		condition, err := p.convertWhereExpr(sel.Where.Expr)
		if err != nil {
			return nil, err
		}

		currentPlan = &FilterPlan{
			Condition: condition,
			Child:     currentPlan,
		}
	}

	// Флаг, чтобы отследить, был ли LIMIT обработан в SortPlan
	limitProcessedInSort := false

	// Добавляем Sort, если есть ORDER BY
	if len(sel.OrderBy) > 0 {
		orderByCols := []OrderByColumn{}
		for _, order := range sel.OrderBy {
			colName, err := p.extractColumnName(order.Expr)
			if err != nil {
				return nil, err
			}
			direction := "ASC"
			if order.Direction == sqlparser.DescScr {
				direction = "DESC"
			}
			orderByCols = append(orderByCols, OrderByColumn{
				ColumnName: colName,
				Direction:  direction,
			})
		}

		// Извлекаем LIMIT, чтобы передать его в SortPlan
		var limitVal int64 = 0
		if sel.Limit != nil {
			var err error
			limitVal, err = p.extractLimitValue(sel.Limit.Rowcount)
			if err != nil {
				return nil, fmt.Errorf("LIMIT: invalid value: %v", err)
			}
			limitProcessedInSort = true
		}

		currentPlan = &SortPlan{
			OrderBy: orderByCols,
			Limit:   limitVal, // Передаем LIMIT в SortPlan для TOP-N Heap Sort
			Child:   currentPlan,
		}
	}

	// Добавляем Distinct, если есть DISTINCT
	if sel.Distinct != "" {
		currentPlan = &DistinctPlan{
			Child: currentPlan,
		}
	}

	// Добавляем LIMIT (если он еще не обработан в SortPlan)
	if sel.Limit != nil && !limitProcessedInSort {
		limitVal, err := p.extractLimitValue(sel.Limit.Rowcount)
		if err != nil {
			return nil, fmt.Errorf("LIMIT: invalid value: %v", err)
		}

		currentPlan = &LimitPlan{
			Limit: limitVal,
			Child: currentPlan,
		}
	}

	// Добавляем Projection для SELECT колонок
	columns := []string{}
	for _, expr := range sel.SelectExprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			// SELECT * - пустой массив означает все колонки
			columns = []string{}
		case *sqlparser.AliasedExpr:
			// Колонка или выражение
			colName, err := p.extractColumnName(e.Expr)
			if err != nil {
				return nil, err
			}
			if colName != "" {
				columns = append(columns, colName)
			}
		default:
			// Игнорируем другие типы выражений
		}
	}

	currentPlan = &ProjectionPlan{
		Columns: columns,
		Child:   currentPlan,
	}

	return currentPlan, nil
}

// convertWhereExpr преобразует WHERE выражение в наше Expression
func (p *planner) convertWhereExpr(expr sqlparser.Expr) (Expression, error) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		// Оператор сравнения
		left, err := p.convertWhereExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := p.convertWhereExpr(e.Right)
		if err != nil {
			return nil, err
		}

		return &ComparisonExpression{
			Left:     left,
			Right:    right,
			Operator: e.Operator,
		}, nil
	case *sqlparser.AndExpr:
		// AND
		left, err := p.convertWhereExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := p.convertWhereExpr(e.Right)
		if err != nil {
			return nil, err
		}

		return &LogicExpression{
			Left:     left,
			Right:    right,
			Operator: "AND",
		}, nil
	case *sqlparser.OrExpr:
		// OR
		left, err := p.convertWhereExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := p.convertWhereExpr(e.Right)
		if err != nil {
			return nil, err
		}

		return &LogicExpression{
			Left:     left,
			Right:    right,
			Operator: "OR",
		}, nil
	case *sqlparser.ParenExpr:
		// Скобки - просто рекурсивно обрабатываем внутреннее выражение
		// Приоритет операций уже учтен в AST парсера
		return p.convertWhereExpr(e.Expr)
	case *sqlparser.ColName:
		// Имя колонки
		return &ColumnExpression{
			ColumnName: e.Name.String(),
			TableName:  e.Qualifier.Name.String(),
		}, nil
	default:
		// Константа
		val, err := p.convertExprToValue(expr)
		if err != nil {
			return nil, err
		}
		return &ConstantExpression{
			Value: val,
		}, nil
	}
}

// planJoin планирует JOIN операцию
func (p *planner) planJoin(joinExpr *sqlparser.JoinTableExpr) (PlanNode, error) {
	// Получаем левую таблицу
	leftTable, err := p.extractTableName(joinExpr.LeftExpr)
	if err != nil {
		return nil, fmt.Errorf("JOIN: failed to extract left table: %v", err)
	}

	// Получаем правую таблицу
	rightTable, err := p.extractTableName(joinExpr.RightExpr)
	if err != nil {
		return nil, fmt.Errorf("JOIN: failed to extract right table: %v", err)
	}

	// Создаем планы сканирования для таблиц
	leftScan := &SeqScanPlan{TableName: leftTable}
	rightScan := &SeqScanPlan{TableName: rightTable}

	// Извлекаем условие JOIN
	leftCol, rightCol, err := p.extractJoinCondition(joinExpr.Condition)
	if err != nil {
		return nil, fmt.Errorf("JOIN: failed to extract condition: %v", err)
	}

	// Определяем тип JOIN
	joinType := "INNER"
	if strings.ToUpper(joinExpr.Join) != "JOIN" && strings.ToUpper(joinExpr.Join) != "INNER JOIN" {
		joinType = strings.ToUpper(joinExpr.Join)
	}

	return &HashJoinPlan{
		LeftTable:   leftTable,
		RightTable:  rightTable,
		LeftColumn:  leftCol,
		RightColumn: rightCol,
		JoinType:    joinType,
		LeftChild:   leftScan,
		RightChild:  rightScan,
	}, nil
}

// extractTableName извлекает имя таблицы из TableExpr
func (p *planner) extractTableName(expr sqlparser.TableExpr) (string, error) {
	switch e := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		tableIdent, ok := e.Expr.(sqlparser.TableName)
		if !ok {
			return "", fmt.Errorf("expected TableName")
		}
		return tableIdent.Name.String(), nil
	case *sqlparser.JoinTableExpr:
		// Рекурсивно обрабатываем вложенные JOIN (пока не поддерживаем)
		return "", fmt.Errorf("nested JOIN not supported")
	default:
		return "", fmt.Errorf("unsupported table expression type: %T", expr)
	}
}

// extractJoinCondition извлекает колонки из условия JOIN
func (p *planner) extractJoinCondition(condition sqlparser.JoinCondition) (string, string, error) {
	// Проверяем, есть ли ON условие
	if condition.On != nil {
		// ON left_col = right_col
		compExpr, ok := condition.On.(*sqlparser.ComparisonExpr)
		if !ok || compExpr.Operator != "=" {
			return "", "", fmt.Errorf("JOIN condition must be equality comparison")
		}

		leftCol, err := p.extractColumnName(compExpr.Left)
		if err != nil {
			return "", "", err
		}

		rightCol, err := p.extractColumnName(compExpr.Right)
		if err != nil {
			return "", "", err
		}

		return leftCol, rightCol, nil
	}

	// Проверяем USING (пока не поддерживаем)
	if len(condition.Using) > 0 {
		return "", "", fmt.Errorf("JOIN USING is not supported yet")
	}

	return "", "", fmt.Errorf("JOIN condition is required")
}

// extractLimitValue извлекает значение LIMIT из выражения
func (p *planner) extractLimitValue(expr sqlparser.Expr) (int64, error) {
	switch e := expr.(type) {
	case *sqlparser.SQLVal:
		if e.Type == sqlparser.IntVal {
			val, err := strconv.ParseInt(string(e.Val), 10, 64)
			if err != nil {
				return 0, err
			}
			return val, nil
		}
		return 0, fmt.Errorf("LIMIT must be an integer")
	default:
		return 0, fmt.Errorf("unsupported LIMIT expression type: %T", expr)
	}
}
