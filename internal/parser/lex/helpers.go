package lex

import (
	"strings"
)

// longestMatch находит самую длинную подходящую строку среди опций,
// начиная с указанной позиции в исходной строке
func longestMatch(source string, startPointer uint, options []string) string {
	var currentValue []byte
	var match string
	maxOptionLength := maxOptionLength(options)

	// Проходим по символам исходной строки начиная с startPointer
	for pointer := startPointer; pointer < uint(len(source)); pointer++ {
		// Добавляем текущий символ к накапливаемому значению (в нижнем регистре)
		currentValue = append(currentValue, strings.ToLower(string(source[pointer]))...)

		// Проверяем каждую опцию на совпадение
		for _, option := range options {
			// Если текущее значение точно совпадает с опцией
			if option == string(currentValue) {
				// Обновляем самую длинную найденную строку
				if len(option) > len(match) {
					match = option
				}
			}
		}

		// Если текущее значение стало длиннее всех опций, прекращаем поиск
		if len(currentValue) > maxOptionLength {
			break
		}
	}

	return match
}

// maxOptionLength возвращает максимальную длину среди всех опций
func maxOptionLength(options []string) int {
	maxLen := 0
	for _, option := range options {
		if len(option) > maxLen {
			maxLen = len(option)
		}
	}
	return maxLen
}

// KeywordsToStrings преобразует слайс Keyword в слайс строк
func KeywordsToStrings(keywords []Keyword) []string {
	result := make([]string, len(keywords))
	for i, k := range keywords {
		result[i] = string(k)
	}
	return result
}

// SymbolsToStrings преобразует слайс Symbol в слайс строк
func SymbolsToStrings(symbols []Symbol) []string {
	result := make([]string, len(symbols))
	for i, s := range symbols {
		result[i] = string(s)
	}
	return result
}

// MathOperatorsToStrings преобразует слайс MathOperator в слайс строк
func MathOperatorsToStrings(operators []MathOperator) []string {
	result := make([]string, len(operators))
	for i, op := range operators {
		result[i] = string(op)
	}
	return result
}
