package ast

import "custom-database/internal/parser/lex"

// parseCreateTableStatement парсит CREATE TABLE statement
func parseCreateTableStatement(tokens []*lex.Token, initialPointer uint) (*CreateTableStatement, uint, bool) {
	pointer := initialPointer

	// Ожидаем ключевое слово CREATE
	if !expectToken(tokens, pointer, tokenFromKeyword(lex.CreateKeyword)) {
		return nil, initialPointer, false
	}
	pointer++

	// Ожидаем ключевое слово TABLE
	if !expectToken(tokens, pointer, tokenFromKeyword(lex.TableKeyword)) {
		return nil, initialPointer, false
	}
	pointer++

	// Парсим имя таблицы
	tableName, newCursor, ok := parseToken(tokens, pointer, lex.IdentifierToken)
	if !ok {
		helpMessage(tokens, pointer, "Expected table name")
		return nil, initialPointer, false
	}
	pointer = newCursor

	// Ожидаем открывающую скобку
	if !expectToken(tokens, pointer, tokenFromSymbol(lex.LeftparenSymbol)) {
		helpMessage(tokens, pointer, "Expected left parenthesis")
		return nil, initialPointer, false
	}
	pointer++

	// Парсим определения колонок
	columns, newCursor, ok := parseColumnDefinitions(tokens, pointer, tokenFromSymbol(lex.RightparenSymbol))
	if !ok {
		return nil, initialPointer, false
	}
	pointer = newCursor

	// Ожидаем закрывающую скобку
	if !expectToken(tokens, pointer, tokenFromSymbol(lex.RightparenSymbol)) {
		helpMessage(tokens, pointer, "Expected right parenthesis")
		return nil, initialPointer, false
	}
	pointer++

	// Ожидаем точку с запятой
	if !expectToken(tokens, pointer, tokenFromSymbol(lex.SemicolonSymbol)) {
		helpMessage(tokens, pointer, "Expected semicolon")
		return nil, initialPointer, false
	}

	return &CreateTableStatement{
		Table:   *tableName,
		Columns: columns,
	}, pointer, true
}

// parseColumnDefinitions парсит список определений колонок
func parseColumnDefinitions(tokens []*lex.Token, initialPointer uint, endDelimiter lex.Token) (*[]*columnDefinition, uint, bool) {
	pointer := initialPointer
	columnDefs := []*columnDefinition{}

	// Парсим определения колонок до встречи с разделителем
	for {
		// Проверяем границы
		if pointer >= uint(len(tokens)) {
			return nil, initialPointer, false
		}

		// Проверяем, не встретили ли разделитель
		current := tokens[pointer]
		if endDelimiter.Equals(current) {
			break
		}

		// Если это не первое определение, ожидаем запятую
		if len(columnDefs) > 0 {
			if !expectToken(tokens, pointer, tokenFromSymbol(lex.CommaSymbol)) {
				helpMessage(tokens, pointer, "Expected comma")
				return nil, initialPointer, false
			}
			pointer++
		}

		// Парсим имя колонки
		columnName, newCursor, ok := parseToken(tokens, pointer, lex.IdentifierToken)
		if !ok {
			helpMessage(tokens, pointer, "Expected column name")
			return nil, initialPointer, false
		}
		pointer = newCursor

		// Парсим тип данных колонки
		columnType, newCursor, ok := parseToken(tokens, pointer, lex.KeywordToken)
		if !ok {
			helpMessage(tokens, pointer, "Expected column type")
			return nil, initialPointer, false
		}
		pointer = newCursor

		// Добавляем определение колонки
		columnDefs = append(columnDefs, &columnDefinition{
			Name:     *columnName,
			Datatype: *columnType,
		})
	}

	return &columnDefs, pointer, true
}
