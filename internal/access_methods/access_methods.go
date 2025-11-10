package access_methods

import (
	"custom-database/internal/disk_manager"
	"errors"
)

// AccessMethodsInterface интерфейс для Access Methods
type AccessMethodsInterface interface {
	CreateIndex(tableName string, columnName string) error
	DropIndex(tableName string) error
	CheckIndexExists(tableName string) bool

	InsertTupleID(tableName string, key uint32, rowID disk_manager.TupleID) error
	DeleteTupleID(tableName string, key uint32) error

	GetTupleID(tableName string, key uint32) (*IndexEntry, bool)
	GetTupleIDs(tableName string, startKey, endKey uint32) ([]*IndexEntry, error)
}

// AccessMethods реализация Access Methods
type AccessMethods struct {
	Trees map[string]*BPlusTree
}

func NewAccessMethods() (AccessMethodsInterface, error) {
	return &AccessMethods{
		Trees: make(map[string]*BPlusTree),
	}, nil
}

// ========================== Index Functions ==========================

func (am *AccessMethods) CreateIndex(tableName string, columnName string) error {
	am.Trees[tableName] = NewBPlusTree()
	return nil
}

func (am *AccessMethods) DropIndex(tableName string) error {
	delete(am.Trees, tableName)
	return nil
}

func (am *AccessMethods) CheckIndexExists(tableName string) bool {
	tree, ok := am.Trees[tableName]
	if !ok {
		return false
	}
	// Проверяем, что дерево не nil и имеет корневой узел
	// Это гарантирует, что индекс не только создан, но и может быть использован
	return tree != nil && tree.Root != nil
}

// ========================== TupleID Functions ==========================

func (am *AccessMethods) GetTupleID(tableName string, key uint32) (*IndexEntry, bool) {
	tree, ok := am.Trees[tableName]
	if !ok {
		return nil, false
	}
	return tree.Get(key)
}

func (am *AccessMethods) GetTupleIDs(tableName string, startKey, endKey uint32) ([]*IndexEntry, error) {
	tree, ok := am.Trees[tableName]
	if !ok {
		return nil, errors.New("tree not found")
	}

	return tree.GetRange(startKey, endKey), nil
}

func (am *AccessMethods) InsertTupleID(tableName string, key uint32, rowID disk_manager.TupleID) error {
	tree, ok := am.Trees[tableName]
	if !ok {
		return errors.New("tree not found")
	}
	tree.Insert(key, rowID)
	return nil
}

func (am *AccessMethods) DeleteTupleID(tableName string, key uint32) error {
	tree, ok := am.Trees[tableName]
	if !ok {
		return errors.New("tree not found")
	}
	tree.Delete(key)
	return nil
}
