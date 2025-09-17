package lex

// lexSymbol парсит символы SQL (скобки, запятые, точки с запятой и т.д.)
func lexSymbol(source string, startPointer uint) (*Token, uint, bool) {
	// Проверяем, что не вышли за пределы длинны sql запроса
	if startPointer >= uint(len(source)) {
		return nil, startPointer, false
	}

	currentChar := source[startPointer]

	// Обрабатываем пробельные символы - они игнорируются, но считаются успешным парсингом
	if currentChar == '\n' || currentChar == '\t' || currentChar == ' ' {
		return nil, startPointer + 1, true
	}

	// Создаем список всех возможных символов
	options := SymbolsToStrings(symbols)

	// Ищем самое длинное совпадение среди символов
	match := longestMatch(source, startPointer, options)
	if match == "" {
		return nil, startPointer, false
	}

	// Вычисляем новую позицию указателя после найденного символа
	newPointer := startPointer + uint(len(match))

	return &Token{
		Value: match,
		Kind:  SymbolToken,
	}, newPointer, true
}
