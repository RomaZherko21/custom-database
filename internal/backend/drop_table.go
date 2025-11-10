package backend

import "custom-database/internal/parser/ast"

func (mb *memoryBackend) dropTable(statement *ast.DropTableStatement) error {
	err := mb.bufferBool.DropTable(statement.Table.Value)
	if err != nil {
		return err
	}

	if mb.accessMethods.CheckIndexExists(statement.Table.Value) {
		err = mb.accessMethods.DropIndex(statement.Table.Value)
		if err != nil {
			return err
		}
	}

	return nil
}
