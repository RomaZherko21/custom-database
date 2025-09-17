package backend

import (
	"bytes"
	"custom-database/internal/models"
	"custom-database/internal/parser/ast"
	"custom-database/internal/parser/lex"
	"encoding/binary"
	"fmt"
	"strconv"
)

func (mb *memoryBackend) insertIntoTable(statement *ast.InsertStatement) error {
	if statement.Values == nil {
		return nil
	}

	row := []MemoryCell{}
	for _, value := range *statement.Values {
		if value.Kind != ast.LiteralKind {
			fmt.Println("Skipping non-literal.")
			continue
		}

		row = append(row, mb.tokenToCell(value.Literal))
	}

	interfaceCells := make([]interface{}, len(row))
	columns, err := mb.persistentStorage.GetTableColumns(statement.Table.Value)
	if err != nil {
		return err
	}

	for i, cell := range row {
		// if cell == nil {
		// 	return fmt.Errorf("failed to convert cell to interface: nil not allowed")
		// }

		column := columns[i]

		if column.Type == models.Int32Type {
			if cell.IsNull() {
				interfaceCells[i] = nil
			} else {
				interfaceCells[i] = cell.AsInt()
			}
		}

		if column.Type == models.TextType {
			if cell.IsNull() {
				interfaceCells[i] = nil
			} else {
				interfaceCells[i] = cell.AsText()
			}
		}

	}

	err = mb.persistentStorage.Insert(statement.Table.Value, interfaceCells)
	if err != nil {
		return err
	}

	cells := make([]models.Cell, len(row))
	for i, cell := range row {
		cells[i] = cell
	}

	return nil
}

func (mb *memoryBackend) tokenToCell(t *lex.Token) MemoryCell {
	if t.Kind == lex.NumericToken {
		buf := new(bytes.Buffer)
		i, err := strconv.Atoi(t.Value)
		if err != nil {
			panic(err)
		}

		err = binary.Write(buf, binary.BigEndian, int32(i))
		if err != nil {
			panic(err)
		}
		return MemoryCell(buf.Bytes())
	}

	if t.Kind == lex.StringToken {
		return MemoryCell(t.Value)
	}

	if t.Kind == lex.NullToken {
		return MemoryCell(t.Value)
	}

	return nil
}
