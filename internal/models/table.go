package models

type ColumnType string

type Column struct {
	Name string     `json:"name"`
	Type ColumnType `json:"type"`
}

// Column types
var TextType ColumnType = "TEXT"
var Int32Type ColumnType = "INT_32"

type Cell interface {
	AsText() string
	AsInt() int32
	IsNull() bool
}

type Table struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
	Cells   [][]Cell `json:"cells"`
}
