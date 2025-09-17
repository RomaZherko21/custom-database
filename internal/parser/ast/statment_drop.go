package ast

import "custom-database/internal/parser/lex"

// parseDropTableStatement парсит DROP TABLE statement
func parseDropTableStatement(tokens []*lex.Token, initialPointer uint) (*DropTableStatement, uint, bool) {
	pointer := initialPointer

	// Ожидаем ключевое слово DROP
	if !expectToken(tokens, pointer, tokenFromKeyword(lex.DropKeyword)) {
		return nil, initialPointer, false
	}
	pointer++

	// Ожидаем ключевое слово TABLE
	if !expectToken(tokens, pointer, tokenFromKeyword(lex.TableKeyword)) {
		helpMessage(tokens, pointer, "Expected table")
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

	// Ожидаем точку с запятой
	if !expectToken(tokens, pointer, tokenFromSymbol(lex.SemicolonSymbol)) {
		helpMessage(tokens, pointer, "Expected semicolon")
		return nil, initialPointer, false
	}

	return &DropTableStatement{
		Table: *tableName,
	}, pointer, true
}
