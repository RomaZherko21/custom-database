package lex

// lexKeyword парсит ключевые слова SQL (CREATE, TABLE, SELECT и т.д.)
func lexKeyword(source string, startPointer uint) (*Token, uint, bool) {
	// Проверяем, что не вышли за пределы длинны sql запроса
	if startPointer >= uint(len(source)) {
		return nil, startPointer, false
	}

	// Создаем список всех возможных ключевых слов
	options := KeywordsToStrings(Keywords)

	// Ищем самое длинное совпадение среди ключевых слов
	match := longestMatch(source, startPointer, options)
	if match == "" {
		return nil, startPointer, false
	}

	// Вычисляем новую позицию указателя после найденного ключевого слова
	newPointer := startPointer + uint(len(match))

	return &Token{
		Value: match,
		Kind:  KeywordToken,
	}, newPointer, true
}
