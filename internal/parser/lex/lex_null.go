package lex

// lexNull парсит ключевое слово NULL
func lexNull(source string, startPointer uint) (*Token, uint, bool) {
	// Проверяем, что не вышли за пределы длинны sql запроса
	if startPointer >= uint(len(source)) {
		return nil, startPointer, false
	}

	// Ищем совпадение с ключевым словом NULL
	match := longestMatch(source, startPointer, []string{string(NullValueKeyword)})
	if match == "" {
		return nil, startPointer, false
	}

	// Вычисляем новую позицию указателя после найденного NULL
	newPointer := startPointer + uint(len(match))

	return &Token{
		Value: match,
		Kind:  NullToken,
	}, newPointer, true
}
