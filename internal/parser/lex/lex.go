package lex

import (
	"fmt"
)

type Lexer interface {
	Lex(source string) ([]*Token, error)
}

type lex struct {
}

func NewLexer() Lexer {
	return &lex{}
}

// lexerFunc тип функции для парсинга токенов
type lexerFunc func(string, uint) (*Token, uint, bool)

// Example: CREATE TABLE users (id INT, name TEXT);
func (l *lex) Lex(source string) ([]*Token, error) {
	tokens := []*Token{}
	pointer := uint(0)

	// Список всех функций-лексеров в порядке приоритета
	lexers := []lexerFunc{
		lexKeyword,      // Ключевые слова (CREATE, SELECT и т.д.)
		lexSymbol,       // Символы (скобки, запятые и т.д.)
		lexNull,         // NULL
		lexMathOperator, // Математические операторы (=, <, >, !=)
		lexString,       // Строковые литералы
		lexNumeric,      // Числовые литералы
		lexIdentifier,   // Идентификаторы (имена таблиц, колонок)
	}

	// Проходим по всей строке, парся токены
	for pointer < uint(len(source)) {
		tokenFound := false

		// Пробуем каждый лексер по очереди
		for _, lexFunc := range lexers {
			token, newPointer, ok := lexFunc(source, pointer)

			if ok {
				pointer = newPointer
				tokenFound = true

				// Добавляем токен в список (nil токены игнорируются - это пробелы)
				if token != nil {
					tokens = append(tokens, token)
				}

				break // Переходим к следующей позиции
			}
		}

		// Если ни один лексер не смог распарсить символ
		if !tokenFound {
			hint := ""
			if len(tokens) > 0 {
				hint = " after " + tokens[len(tokens)-1].Value
			}
			return nil, fmt.Errorf("unable to lex token%s, at position %d", hint, pointer)
		}
	}

	return tokens, nil
}
