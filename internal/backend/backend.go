package backend

import (
	"custom-database/internal/access_methods"
	"custom-database/internal/buffer_bool"
	"custom-database/internal/parser/ast"
)

type MemoryBackendService interface {
	ExecuteStatement(*ast.Ast) (*Table, error)
}

type memoryBackend struct {
	bufferBool    buffer_bool.BufferPoolInterface
	accessMethods access_methods.AccessMethodsInterface
}

func NewMemoryBackend() (MemoryBackendService, error) {
	bufferBool, err := buffer_bool.NewBufferPool(3, 2)
	if err != nil {
		return nil, err
	}

	accessMethods, err := access_methods.NewAccessMethods()
	if err != nil {
		return nil, err
	}

	return &memoryBackend{
		bufferBool:    bufferBool,
		accessMethods: accessMethods,
	}, nil
}

func (mb *memoryBackend) ExecuteStatement(a *ast.Ast) (*Table, error) {
	var err error

	for _, stmt := range a.Statements {
		switch stmt.Kind {
		case ast.CreateTableKind:
			err = mb.createTable(stmt.CreateTableStatement)
			if err != nil {
				return nil, err
			}
		case ast.DropTableKind:
			err = mb.dropTable(stmt.DropTableStatement)
			if err != nil {
				return nil, err
			}

		case ast.CreateIndexKind:
			err = mb.createIndex(stmt.CreateIndexStatement)
			if err != nil {
				return nil, err
			}
		case ast.DropIndexKind:
			err = mb.dropIndex(stmt.DropIndexStatement)
			if err != nil {
				return nil, err
			}

		case ast.InsertKind:
			err = mb.insertIntoTable(stmt.InsertStatement)
			if err != nil {
				return nil, err
			}
		case ast.SelectKind:
			results, err := mb.selectFromTable(stmt.SelectStatement)
			if err != nil {
				return nil, err
			}
			return results, nil
		}
	}

	return nil, nil
}
