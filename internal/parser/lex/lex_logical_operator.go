package lex

func lexLogicalOperator(source string, startPointer uint) (*Token, uint, bool) {
	options := LogicalOperatorsToStrings(logicalOperators)

	match := longestMatch(source, startPointer, options)
	if match == "" {
		return nil, startPointer, false
	}

	newPointer := startPointer + uint(len(match))

	return &Token{
		Value: match,
		Kind:  LogicalOperatorToken,
	}, newPointer, true
}
