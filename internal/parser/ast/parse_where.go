package ast

import (
	"custom-database/internal/parser/lex"
)

type tokenWithPrior struct {
	Token    *lex.Token
	Priority uint
}

func parseWhereClause(tokens []*lex.Token, initialCursor uint, delimiter lex.Token) (*WhereClause, uint, bool) {
	cursor := initialCursor

	if !expectToken(tokens, cursor, tokenFromKeyword(lex.WhereKeyword)) {
		return nil, initialCursor, true
	}
	cursor++

	whereExps, newCursor, ok := parseWhereExpression(tokens, cursor, []lex.Token{delimiter})
	if !ok {
		return nil, initialCursor, false
	}

	return whereExps, newCursor, true
}

func parseWhereExpression(tokens []*lex.Token, initialCursor uint, delimiters []lex.Token) (*WhereClause, uint, bool) {
	// find first delimiter cursor
	firstDelimiterCursor := uint(0)
loop:
	for i := initialCursor; i < uint(len(tokens)); i++ {
		for _, delimiter := range delimiters {
			if tokens[i].Value == delimiter.Value {
				firstDelimiterCursor = i
				break loop
			}
		}
	}
	if firstDelimiterCursor == 0 {
		return nil, initialCursor, false
	}

	whereTokens := addTokensPriority(tokens[initialCursor:firstDelimiterCursor])
	whereTree := parseTree(whereTokens)

	return whereTree, firstDelimiterCursor, true
}

func parseTree(tokens []*tokenWithPrior) *WhereClause {
	if len(tokens) == 0 {
		return nil
	}

	root := &WhereClause{}

	index := 0
	minPriorToken := tokens[0]
	for i, token := range tokens {
		if token.Priority < minPriorToken.Priority {
			minPriorToken = token
			index = i
		}
	}

	root.Token = minPriorToken.Token

	root.Left = parseTree(tokens[:index])
	root.Right = parseTree(tokens[index+1:])

	return root
}

func addTokensPriority(tokens []*lex.Token) []*tokenWithPrior {
	mappedTokens := []*tokenWithPrior{}

	openedParentheses := 0
	identPrior := 0
	for _, token := range tokens {
		if token.Kind == lex.SymbolToken && token.Value == "(" {
			openedParentheses++
			continue
		}
		if token.Kind == lex.SymbolToken && token.Value == ")" {
			openedParentheses--
			continue
		}

		if token.Kind == lex.IdentifierToken || token.Kind == lex.StringToken || token.Kind == lex.NumericToken || token.Kind == lex.NullToken {
			identPrior++
		}

		mappedTokens = append(mappedTokens, &tokenWithPrior{
			Token:    token,
			Priority: getPriority(token, uint(openedParentheses*100), uint(identPrior)),
		})
	}

	return mappedTokens
}

func getPriority(token *lex.Token, subPriority uint, identPrior uint) uint {

	if token.Kind == lex.IdentifierToken || token.Kind == lex.StringToken || token.Kind == lex.NumericToken || token.Kind == lex.NullToken {
		return 40 + identPrior + subPriority
	}

	switch token.Value {
	case string(lex.OrOperator):
		return 20 + subPriority
	case string(lex.AndOperator):
		return 30 + subPriority
	case string(lex.EqualOperator), string(lex.NotEqualOperator), string(lex.GreaterThanOperator), string(lex.LessThanOperator):
		return 40 + subPriority
	}

	return 0
}
