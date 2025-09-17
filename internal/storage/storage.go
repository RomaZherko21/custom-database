package storage

import (
	"custom-database/internal/models"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const TABLES_DIR = "tables"

type PersistentStorageService interface {
	CreateTable(tableName string, columns []models.Column) error
	DropTable(tableName string) error
	Insert(tableName string, values []interface{}) error
	Select(tableName string) (*table, error)
	GetTableColumns(tableName string) ([]models.Column, error)
}

type persistentStorage struct {
	dir string
}

func NewPersistentStorage() (PersistentStorageService, error) {
	if err := os.MkdirAll(TABLES_DIR, 0755); err != nil {
		return nil, fmt.Errorf("NewPersistentStorage(): failed to create persistent storage: %w", err)
	}

	return &persistentStorage{
		dir: TABLES_DIR,
	}, nil
}

func (ps *persistentStorage) CreateTable(tableName string, columns []models.Column) error {
	filename := filepath.Join(ps.dir, tableName+".json")

	if _, err := os.Stat(filename); err == nil {
		return fmt.Errorf("CreateTable(): table already exists: %w", err)
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("CreateTable(): failed to create table file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(models.Table{
		Name:    tableName,
		Columns: columns,
		Cells:   [][]models.Cell{},
	}); err != nil {
		return fmt.Errorf("CreateTable(): failed to encode table: %w", err)
	}

	return nil
}

func (ps *persistentStorage) DropTable(tableName string) error {
	filename := filepath.Join(ps.dir, tableName+".json")

	if _, err := os.Stat(filename); err != nil {
		return fmt.Errorf("DropTable(): table does not exist: %w", err)
	}

	if err := os.Remove(filename); err != nil {
		return fmt.Errorf("DropTable(): failed to remove table file: %w", err)
	}

	return nil
}

type table struct {
	Name    string          `json:"name"`
	Columns []models.Column `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
}

func (ps *persistentStorage) Insert(tableName string, values []interface{}) error {
	filename := filepath.Join(ps.dir, tableName+".json")

	if _, err := os.Stat(filename); err != nil {
		return fmt.Errorf("Insert(): table does not exist: %w", err)
	}

	file, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("Insert(): failed to open table file: %w", err)
	}
	defer file.Close()

	var tableData table
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&tableData); err != nil {
		return fmt.Errorf("failed to decode table data: %w", err)
	}

	tableData.Rows = append(tableData.Rows, values)

	file.Seek(0, 0)

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(tableData); err != nil {
		return fmt.Errorf("Insert(): failed to encode new values: %w", err)
	}

	return nil
}

func (ps *persistentStorage) Select(tableName string) (*table, error) {
	filename := filepath.Join(ps.dir, tableName+".json")

	if _, err := os.Stat(filename); err != nil {
		return nil, fmt.Errorf("Select(): table does not exist: %w", err)
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("Select(): failed to open table file: %w", err)
	}
	defer file.Close()

	var tableData table
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&tableData); err != nil {
		return nil, fmt.Errorf("failed to decode table data: %w", err)
	}

	return &tableData, nil
}

func (ps *persistentStorage) GetTableColumns(tableName string) ([]models.Column, error) {
	filename := filepath.Join(ps.dir, tableName+".json")

	if _, err := os.Stat(filename); err != nil {
		return nil, fmt.Errorf("GetTableColumns(): table does not exist: %w", err)
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("GetTableColumns(): failed to open table file: %w", err)
	}
	defer file.Close()

	var tableData table
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&tableData); err != nil {
		return nil, fmt.Errorf("GetTableColumns(): failed to decode table data: %w", err)
	}

	return tableData.Columns, nil
}
