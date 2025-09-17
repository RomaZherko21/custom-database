package lex

import "strings"

// lexIdentifier парсит идентификаторы (имена таблиц, колонок и т.д.)
func lexIdentifier(source string, startPointer uint) (*Token, uint, bool) {
	// Проверяем, что не вышли за пределы длинны sql запроса
	if startPointer >= uint(len(source)) {
		return nil, startPointer, false
	}

	// Сначала проверяем, не является ли это идентификатором в двойных кавычках
	if token, newPointer, ok := lexCharacterDelimited(source, startPointer, '"'); ok {
		// Меняем тип токена с StringToken на IdentifierToken
		token.Kind = IdentifierToken
		return token, newPointer, true
	}

	pointer := startPointer
	currentChar := source[pointer]

	// Первый символ должен быть буквой (A-Z, a-z)
	isAlphabetical := (currentChar >= 'A' && currentChar <= 'Z') || (currentChar >= 'a' && currentChar <= 'z')
	if !isAlphabetical {
		return nil, startPointer, false
	}

	// Начинаем накапливать символы идентификатора
	value := []byte{currentChar}
	pointer++

	// Продолжаем накапливать символы до тех пор, пока они валидны
	for ; pointer < uint(len(source)); pointer++ {
		currentChar = source[pointer]

		isAlphabetical := (currentChar >= 'A' && currentChar <= 'Z') || (currentChar >= 'a' && currentChar <= 'z')
		isNumeric := currentChar >= '0' && currentChar <= '9'

		// Валидные символы: буквы, цифры, $, _
		if isAlphabetical || isNumeric || currentChar == '$' || currentChar == '_' {
			value = append(value, currentChar)
			continue
		}

		// Встретили невалидный символ - прекращаем парсинг
		break
	}

	return &Token{
		// Нецитируемые идентификаторы нечувствительны к регистру
		Value: strings.ToLower(string(value)),
		Kind:  IdentifierToken,
	}, pointer, true
}
