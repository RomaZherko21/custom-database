package backend

import (
	"bytes"
	"encoding/binary"
)

type ColumnType uint

type Column struct {
	Name string     `json:"name"`
	Type ColumnType `json:"type"`
}

const (
	TextType ColumnType = iota
	IntType
)

type Cell interface {
	AsText() string
	AsInt() int32
	IsNull() bool
}

type Table struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
	Rows    [][]Cell `json:"rows"`
}

type MemoryCell []byte

func (mc MemoryCell) AsInt() int32 {
	var i int32
	err := binary.Read(bytes.NewBuffer(mc), binary.BigEndian, &i)
	if err != nil {
		panic(err)
	}

	return i
}

func (mc MemoryCell) AsText() string {
	return string(mc)
}

func (mc MemoryCell) IsNull() bool {
	return string(mc) == "null"
}
