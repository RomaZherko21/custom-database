package lex

// lexString парсит строковые литералы, ограниченные одинарными кавычками
func lexString(source string, startPointer uint) (*Token, uint, bool) {
	return lexCharacterDelimited(source, startPointer, '\'')
}
