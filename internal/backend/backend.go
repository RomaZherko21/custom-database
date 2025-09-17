package backend

import (
	"custom-database/internal/models"
	"custom-database/internal/parser/ast"
	"custom-database/internal/storage"
)

type MemoryBackendService interface {
	ExecuteStatement(*ast.Ast) (*models.Table, error)
}

type memoryBackend struct {
	persistentStorage storage.PersistentStorageService
}

func NewMemoryBackend() (MemoryBackendService, error) {
	persistentStorage, err := storage.NewPersistentStorage()
	if err != nil {
		return nil, err
	}

	return &memoryBackend{
		persistentStorage: persistentStorage,
	}, nil
}

func (mb *memoryBackend) ExecuteStatement(a *ast.Ast) (*models.Table, error) {
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
