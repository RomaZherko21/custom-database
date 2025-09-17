package lex

// lexCharacterDelimited парсит строку, ограниченную разделителями.
// Поддерживает экранирование разделителя путем его дублирования.
func lexCharacterDelimited(source string, startPointer uint, delimiter byte) (*Token, uint, bool) {
	// Проверяем, что не вышли за пределы длинны sql запроса
	if startPointer >= uint(len(source)) {
		return nil, startPointer, false
	}

	// Проверяем, что первый символ - это разделитель
	if source[startPointer] != delimiter {
		return nil, startPointer, false
	}

	// Начинаем с символа после разделителя
	pointer := startPointer + 1
	var value []byte

	// Проходим по символам до конца строки
	for pointer < uint(len(source)) {
		currentChar := source[pointer]

		// Если встретили разделитель
		if currentChar == delimiter {
			// Проверяем, не экранирован ли он (следующий символ тоже разделитель)
			if pointer+1 < uint(len(source)) && source[pointer+1] == delimiter {
				// Экранированный разделитель - добавляем оба символа и пропускаем следующий
				value = append(value, delimiter, delimiter)
				pointer += 2
			} else {
				// Конец строки - возвращаем токен
				return &Token{
					Value: string(value),
					Kind:  StringToken,
				}, pointer + 1, true
			}
		} else {
			// Обычный символ - добавляем к значению
			value = append(value, currentChar)
			pointer++
		}
	}

	// Если дошли до конца строки, но не нашли закрывающий разделитель
	return nil, startPointer, false
}
