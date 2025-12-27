package operator_execution

import (
	"custom-database/internal/disk_manager"
	"custom-database/internal/query_planning"
	"errors"
	"fmt"
)

type Tuple struct {
	Values []Value
	Schema *Schema
}

type Value struct {
	Type   ValueType
	IntVal int32
	StrVal string
	IsNull bool
}

type ValueType uint8

const (
	ValueTypeInt ValueType = iota
	ValueTypeText
	ValueTypeNull
)

func (v *Value) String() string {
	if v.IsNull {
		return "null"
	}
	switch v.Type {
	case ValueTypeInt:
		return fmt.Sprintf("%d", v.IntVal)
	case ValueTypeText:
		return v.StrVal
	default:
		return "null"
	}
}

// NewTuple создает новый tuple из значений и схемы
func NewTuple(values []Value, schema *Schema) *Tuple {
	return &Tuple{
		Values: values,
		Schema: schema,
	}
}

func (t *Tuple) GetValue(columnIndex int) *Value {
	if columnIndex < 0 || columnIndex >= len(t.Values) {
		return nil
	}
	return &t.Values[columnIndex]
}

func (t *Tuple) GetValueByName(columnName string) *Value {
	if t.Schema == nil {
		return nil
	}

	index := t.Schema.GetColumnIndex(columnName)
	if index == -1 {
		return nil
	}

	return t.GetValue(index)
}

func ConvertFromDiskRow(row disk_manager.Row, schema *Schema) (*Tuple, error) {
	if len(row) != len(schema.Columns) {
		return nil, errors.New("schema mismatch: number of values does not match schema")
	}

	values := make([]Value, len(row))
	for i, cell := range row {
		if cell.IsNull {
			values[i] = Value{
				Type:   ValueTypeNull,
				IsNull: true,
			}
		} else {
			switch cell.DataType {
			case disk_manager.INT_32_TYPE:
				var intVal int32
				if val, ok := cell.Data.(int32); ok {
					intVal = val
				} else if val, ok := cell.Data.(uint32); ok {
					intVal = int32(val)
				} else {
					return nil, errors.New("invalid value type")
				}
				values[i] = Value{
					Type:   ValueTypeInt,
					IntVal: intVal,
					IsNull: false,
				}
			case disk_manager.TEXT_TYPE:
				strVal, ok := cell.Data.(string)
				if !ok {
					return nil, errors.New("invalid value type")
				}
				values[i] = Value{
					Type:   ValueTypeText,
					StrVal: strVal,
					IsNull: false,
				}
			default:
				return nil, errors.New("unsupported data type")
			}
		}
	}

	return NewTuple(values, schema), nil
}

func (t *Tuple) Compare(other *Tuple, orderBy []query_planning.OrderByColumn) int {
	if t == nil || other == nil {
		if t == nil && other == nil {
			return 0
		}
		if t == nil {
			return -1
		}
		return 1
	}

	for _, orderCol := range orderBy {
		tVal := t.GetValueByName(orderCol.ColumnName)
		otherVal := other.GetValueByName(orderCol.ColumnName)

		comparison := compareValues(tVal, otherVal)

		if comparison != 0 {
			if orderCol.Direction == "DESC" {
				return -comparison
			}
			return comparison
		}
	}

	return 0
}

func compareValues(v1, v2 *Value) int {
	if v1 == nil && v2 == nil {
		return 0
	}
	if v1 == nil || v1.IsNull {
		return -1
	}
	if v2 == nil || v2.IsNull {
		return 1
	}

	if v1.Type != v2.Type {
		if v1.Type < v2.Type {
			return -1
		}
		return 1
	}

	switch v1.Type {
	case ValueTypeInt:
		if v1.IntVal < v2.IntVal {
			return -1
		}
		if v1.IntVal > v2.IntVal {
			return 1
		}
		return 0
	case ValueTypeText:
		if v1.StrVal < v2.StrVal {
			return -1
		}
		if v1.StrVal > v2.StrVal {
			return 1
		}
		return 0
	default:
		return 0
	}
}

type Schema struct {
	Columns []ColumnInfo
}

type ColumnInfo struct {
	Name string
	Type ValueType
}

func NewSchema(columns []disk_manager.ColumnInfo) *Schema {
	schema := &Schema{
		Columns: make([]ColumnInfo, len(columns)),
	}

	for i, col := range columns {
		var valueType ValueType
		switch col.DataType {
		case disk_manager.INT_32_TYPE:
			valueType = ValueTypeInt
		case disk_manager.TEXT_TYPE:
			valueType = ValueTypeText
		default:
			valueType = ValueTypeNull
		}

		schema.Columns[i] = ColumnInfo{
			Name: col.ColumnName,
			Type: valueType,
		}
	}

	return schema
}

func (s *Schema) GetColumnIndex(columnName string) int {
	for i, col := range s.Columns {
		if col.Name == columnName {
			return i
		}
	}
	return -1
}

func (s *Schema) Project(columnNames []string) (*Schema, error) {
	if len(columnNames) == 0 {
		return s, nil
	}

	projectedColumns := make([]ColumnInfo, 0, len(columnNames))
	for _, name := range columnNames {
		index := s.GetColumnIndex(name)
		if index == -1 {
			return nil, fmt.Errorf("column '%s' not found in schema", name)
		}
		projectedColumns = append(projectedColumns, s.Columns[index])
	}

	return &Schema{Columns: projectedColumns}, nil
}
