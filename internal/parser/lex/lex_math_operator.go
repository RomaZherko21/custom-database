package lex

// lexMathOperator парсит математические операторы (=, <, >, != и т.д.)
func lexMathOperator(source string, startPointer uint) (*Token, uint, bool) {
	// Проверяем, что не вышли за пределы длинны sql запроса
	if startPointer >= uint(len(source)) {
		return nil, startPointer, false
	}

	// Создаем список всех возможных математических операторов
	options := MathOperatorsToStrings(mathOperators)

	// Ищем самое длинное совпадение среди операторов
	match := longestMatch(source, startPointer, options)
	if match == "" {
		return nil, startPointer, false
	}

	// Вычисляем новую позицию указателя после найденного оператора
	newPointer := startPointer + uint(len(match))

	return &Token{
		Value: match,
		Kind:  MathOperatorToken,
	}, newPointer, true
}
