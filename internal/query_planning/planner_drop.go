package query_planning

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

// planDropTable планирует DROP TABLE
func (p *planner) planDropTable(ddl *sqlparser.DDL) (PlanNode, error) {
	tableName := ddl.Table.Name.String()
	if tableName == "" {
		return nil, fmt.Errorf("DROP TABLE: table name is empty")
	}

	return &DropTablePlan{
		TableName: tableName,
	}, nil
}

// planDropIndex планирует DROP INDEX
func (p *planner) planDropIndex(ddl *sqlparser.DDL) (PlanNode, error) {
	if ddl.VindexSpec == nil {
		return nil, fmt.Errorf("DROP INDEX: VindexSpec is nil")
	}

	indexName := ddl.VindexSpec.Name.String()
	if indexName == "" {
		return nil, fmt.Errorf("DROP INDEX: index name is empty")
	}

	return &DropIndexPlan{
		IndexName: indexName,
	}, nil
}

