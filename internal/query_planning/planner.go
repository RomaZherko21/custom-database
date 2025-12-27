package query_planning

import (
	"fmt"
	"strings"

	"custom-database/internal/query_planning/parser"

	"github.com/xwb1989/sqlparser"
)

type PlannerService interface {
	Plan(stmtWithSQL parser.StatementWithSQL) (PlanNode, error)
}

type planner struct {
}

// NewPlanner создает новый планировщик запросов
func NewPlanner() PlannerService {
	return &planner{}
}

func (p *planner) Plan(stmtWithSQL parser.StatementWithSQL) (PlanNode, error) {
	stmt := stmtWithSQL.Statement
	originalSQL := stmtWithSQL.SQL

	// Планируем statement в зависимости от типа
	switch s := stmt.(type) {
	case *sqlparser.DDL:
		return p.planDDL(s, originalSQL)
	case *sqlparser.Insert:
		return p.planInsert(s)
	case *sqlparser.Select:
		return p.planSelect(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// planDDL планирует DDL операции (CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX)
func (p *planner) planDDL(ddl *sqlparser.DDL, originalSQL string) (PlanNode, error) {
	switch ddl.Action {
	case sqlparser.CreateStr:
		if ddl.TableSpec != nil {
			// CREATE TABLE
			return p.planCreateTable(ddl)
		} else {
			// CREATE INDEX (парсится как ALTER)
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(originalSQL)), "CREATE INDEX") {
				return p.planCreateIndex(ddl, originalSQL)
			}
			return nil, fmt.Errorf("unsupported CREATE statement")
		}
	case sqlparser.DropStr:
		if ddl.VindexSpec != nil {
			// DROP INDEX
			return p.planDropIndex(ddl)
		} else {
			// DROP TABLE
			return p.planDropTable(ddl)
		}
	case sqlparser.AlterStr:
		// CREATE INDEX парсится как ALTER
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(originalSQL)), "CREATE INDEX") {
			return p.planCreateIndex(ddl, originalSQL)
		}
		return nil, fmt.Errorf("ALTER statements are not supported")
	default:
		return nil, fmt.Errorf("unsupported DDL action: %s", ddl.Action)
	}
}
