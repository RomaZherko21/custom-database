package ast

import (
	"custom-database/internal/parser/lex"
)

// parseSelectStatement парсит SELECT statement
func parseSelectStatement(tokens []*lex.Token, initialPointer uint) (*SelectStatement, uint, bool) {
	statement := &SelectStatement{
		SelectedColumns: []*Expression{},
	}
	pointer := initialPointer

	// Ожидаем ключевое слово SELECT
	if !expectToken(tokens, pointer, tokenFromKeyword(lex.SelectKeyword)) {
		return nil, initialPointer, false
	}
	pointer++

	// Проверяем, не используется ли SELECT * (все колонки)
	if expectToken(tokens, pointer, tokenFromSymbol(lex.AsteriskSymbol)) {
		// SELECT * - выбираем все колонки
		statement.SelectedColumns = []*Expression{}
		pointer++
	} else {
		// Парсим список конкретных колонок
		expressions, newCursor, ok := parseExpressions(tokens, pointer, []lex.Token{
			tokenFromKeyword(lex.FromKeyword),
			tokenFromSymbol(lex.SemicolonSymbol),
		})
		if !ok {
			return nil, initialPointer, false
		}
		pointer = newCursor
		statement.SelectedColumns = *expressions
	}

	// Парсим FROM clause (опционально)
	if !expectToken(tokens, pointer, tokenFromKeyword(lex.FromKeyword)) {
		return statement, pointer, true
	}
	pointer++

	// Парсим имя таблицы после FROM
	tableName, newCursor, ok := parseToken(tokens, pointer, lex.IdentifierToken)
	if !ok {
		helpMessage(tokens, pointer, "Expected table name after FROM")
		return nil, initialPointer, false
	}
	statement.Table = *tableName
	pointer = newCursor

	// Ожидаем точку с запятой
	if !expectToken(tokens, pointer, tokenFromSymbol(lex.SemicolonSymbol)) {
		helpMessage(tokens, pointer, "Expected semicolon")
		return nil, initialPointer, false
	}

	return statement, pointer, true
}
