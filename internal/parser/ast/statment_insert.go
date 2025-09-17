package ast

import "custom-database/internal/parser/lex"

// parseInsertStatement парсит INSERT statement
func parseInsertStatement(tokens []*lex.Token, initialPointer uint) (*InsertStatement, uint, bool) {
	pointer := initialPointer

	// Ожидаем ключевое слово INSERT
	if !expectToken(tokens, pointer, tokenFromKeyword(lex.InsertKeyword)) {
		return nil, initialPointer, false
	}
	pointer++

	// Ожидаем ключевое слово INTO
	if !expectToken(tokens, pointer, tokenFromKeyword(lex.IntoKeyword)) {
		helpMessage(tokens, pointer, "Expected into")
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

	// Ожидаем ключевое слово VALUES
	if !expectToken(tokens, pointer, tokenFromKeyword(lex.ValuesKeyword)) {
		helpMessage(tokens, pointer, "Expected VALUES")
		return nil, initialPointer, false
	}
	pointer++

	// Ожидаем открывающую скобку
	if !expectToken(tokens, pointer, tokenFromSymbol(lex.LeftparenSymbol)) {
		helpMessage(tokens, pointer, "Expected left paren")
		return nil, initialPointer, false
	}
	pointer++

	// Парсим список значений
	values, newCursor, ok := parseExpressions(tokens, pointer, []lex.Token{tokenFromSymbol(lex.RightparenSymbol)})
	if !ok {
		return nil, initialPointer, false
	}
	pointer = newCursor

	// Ожидаем закрывающую скобку
	if !expectToken(tokens, pointer, tokenFromSymbol(lex.RightparenSymbol)) {
		helpMessage(tokens, pointer, "Expected right paren")
		return nil, initialPointer, false
	}
	pointer++

	// Ожидаем точку с запятой
	if !expectToken(tokens, pointer, tokenFromSymbol(lex.SemicolonSymbol)) {
		helpMessage(tokens, pointer, "Expected semicolon")
		return nil, initialPointer, false
	}

	return &InsertStatement{
		Table:  *tableName,
		Values: values,
	}, pointer, true
}
