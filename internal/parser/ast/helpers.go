package ast

import (
	"custom-database/internal/parser/lex"
	"fmt"
)

// parseExpressions парсит список выражений, разделенных запятыми
func parseExpressions(tokens []*lex.Token, initialPointer uint, delimiters []lex.Token) (*[]*Expression, uint, bool) {
	pointer := initialPointer
	expressions := []*Expression{}

	// Парсим выражения до встречи с разделителем
	for {
		// Проверяем, не вышли ли за границы токенов
		if pointer >= uint(len(tokens)) {
			return nil, initialPointer, false
		}

		// Проверяем, не встретили ли разделитель
		current := tokens[pointer]
		for _, delimiter := range delimiters {
			if delimiter.Equals(current) {
				return &expressions, pointer, true
			}
		}

		// Если это не первое выражение, ожидаем запятую
		if len(expressions) > 0 {
			if !expectToken(tokens, pointer, tokenFromSymbol(lex.CommaSymbol)) {
				helpMessage(tokens, pointer, "Expected comma")
				return nil, initialPointer, false
			}
			pointer++
		}

		// Парсим выражение
		expression, newCursor, ok := parseExpression(tokens, pointer, tokenFromSymbol(lex.CommaSymbol))
		if !ok {
			helpMessage(tokens, pointer, "Expected expression")
			return nil, initialPointer, false
		}
		pointer = newCursor

		expressions = append(expressions, expression)
	}
}

// parseExpression парсит одно выражение (идентификатор, число, строка, NULL)
func parseExpression(tokens []*lex.Token, initialPointer uint, _ lex.Token) (*Expression, uint, bool) {
	pointer := initialPointer

	// Пробуем парсить различные типы токенов как выражения
	validKinds := []lex.TokenKind{
		lex.IdentifierToken, // Имя колонки
		lex.NumericToken,    // Число
		lex.StringToken,     // Строка
		lex.NullToken,       // NULL
	}

	for _, kind := range validKinds {
		if token, newCursor, ok := parseToken(tokens, pointer, kind); ok {
			return &Expression{
				Literal: token,
				Kind:    LiteralKind,
			}, newCursor, true
		}
	}

	return nil, initialPointer, false
}

// tokenFromKeyword создает токен из ключевого слова
func tokenFromKeyword(k lex.Keyword) lex.Token {
	return lex.Token{
		Kind:  lex.KeywordToken,
		Value: string(k),
	}
}

// tokenFromSymbol создает токен из символа
func tokenFromSymbol(s lex.Symbol) lex.Token {
	return lex.Token{
		Kind:  lex.SymbolToken,
		Value: string(s),
	}
}

// parseToken парсит токен определенного типа
func parseToken(tokens []*lex.Token, initialPointer uint, kind lex.TokenKind) (*lex.Token, uint, bool) {
	pointer := initialPointer

	// Проверяем границы
	if pointer >= uint(len(tokens)) {
		return nil, initialPointer, false
	}

	current := tokens[pointer]
	if current.Kind == kind {
		return current, pointer + 1, true
	}

	return nil, initialPointer, false
}

// expectToken проверяет, соответствует ли токен на текущей позиции ожидаемому
func expectToken(tokens []*lex.Token, pointer uint, expected lex.Token) bool {
	if pointer >= uint(len(tokens)) {
		return false
	}

	return expected.Equals(tokens[pointer])
}

// helpMessage выводит сообщение об ошибке парсинга
func helpMessage(tokens []*lex.Token, pointer uint, msg string) {
	var current *lex.Token
	if pointer < uint(len(tokens)) {
		current = tokens[pointer]
	} else {
		current = tokens[pointer-1]
	}

	fmt.Printf("[%s, got: %s\n", msg, current.Value)
}
