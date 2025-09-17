package lex

// lexNumeric парсит числовые литералы (целые числа, числа с плавающей точкой, экспоненциальная запись)
func lexNumeric(source string, startPointer uint) (*Token, uint, bool) {
	// Проверяем, что не вышли за пределы длинны sql запроса
	if startPointer >= uint(len(source)) {
		return nil, startPointer, false
	}

	pointer := startPointer
	periodFound := false
	expMarkerFound := false

	// Проходим по символам, проверяя валидность числового литерала
	for ; pointer < uint(len(source)); pointer++ {
		currentChar := source[pointer]

		isDigit := currentChar >= '0' && currentChar <= '9'
		isPeriod := currentChar == '.'
		isExpMarker := currentChar == 'e'

		// Первый символ должен быть цифрой или точкой
		if pointer == startPointer {
			if !isDigit && !isPeriod {
				return nil, startPointer, false
			}
			periodFound = isPeriod
			continue
		}

		// Обработка точки (десятичного разделителя)
		if isPeriod {
			if periodFound {
				return nil, startPointer, false // Двойная точка недопустима
			}
			periodFound = true
			continue
		}

		// Обработка экспоненциального маркера (e)
		if isExpMarker {
			if expMarkerFound {
				return nil, startPointer, false // Двойной экспоненциальный маркер недопустим
			}

			// После экспоненциального маркера точки недопустимы
			periodFound = true
			expMarkerFound = true

			// Экспоненциальный маркер должен быть не последним символом
			if pointer == uint(len(source)-1) {
				return nil, startPointer, false
			}

			// Проверяем знак экспоненты (+ или -)
			nextChar := source[pointer+1]
			if nextChar == '-' || nextChar == '+' {
				pointer++ // Пропускаем знак
			}
			continue
		}

		// Если символ не является цифрой, прекращаем парсинг
		if !isDigit {
			break
		}
	}

	// Проверяем, что хотя бы один символ был обработан
	if pointer == startPointer {
		return nil, startPointer, false
	}

	return &Token{
		Value: source[startPointer:pointer],
		Kind:  NumericToken,
	}, pointer, true
}
