package ast

import (
	"custom-database/internal/parser/lex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMapWhereTokens(t *testing.T) {
	t.Run("простой случай с идентификатором", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.IdentifierToken, Value: "name"},
		}
		expected := []*tokenWithPrior{
			{Token: &lex.Token{Kind: lex.IdentifierToken, Value: "name"}, Priority: 41},
		}

		result := addTokensPriority(tokens)
		require.Len(t, result, len(expected))
		for i, expected := range expected {
			require.Equal(t, expected.Token.Kind, result[i].Token.Kind)
			require.Equal(t, expected.Token.Value, result[i].Token.Value)
			require.Equal(t, expected.Priority, result[i].Priority)
		}
	})

	t.Run("выражение с операторами", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.IdentifierToken, Value: "age"},
			{Kind: lex.MathOperatorToken, Value: ">"},
			{Kind: lex.NumericToken, Value: "18"},
		}
		expected := []*tokenWithPrior{
			{Token: &lex.Token{Kind: lex.IdentifierToken, Value: "age"}, Priority: 41},
			{Token: &lex.Token{Kind: lex.MathOperatorToken, Value: ">"}, Priority: 40},
			{Token: &lex.Token{Kind: lex.NumericToken, Value: "18"}, Priority: 42},
		}

		result := addTokensPriority(tokens)
		require.Len(t, result, len(expected))
		for i, expected := range expected {
			require.Equal(t, expected.Token.Kind, result[i].Token.Kind)
			require.Equal(t, expected.Token.Value, result[i].Token.Value)
			require.Equal(t, expected.Priority, result[i].Priority)
		}
	})

	t.Run("выражение со скобками", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.SymbolToken, Value: "("},
			{Kind: lex.IdentifierToken, Value: "name"},
			{Kind: lex.MathOperatorToken, Value: "="},
			{Kind: lex.StringToken, Value: "'John'"},
			{Kind: lex.SymbolToken, Value: ")"},
		}
		expected := []*tokenWithPrior{
			{Token: &lex.Token{Kind: lex.IdentifierToken, Value: "name"}, Priority: 141},
			{Token: &lex.Token{Kind: lex.MathOperatorToken, Value: "="}, Priority: 140},
			{Token: &lex.Token{Kind: lex.StringToken, Value: "'John'"}, Priority: 142},
		}

		result := addTokensPriority(tokens)
		require.Len(t, result, len(expected))
		for i, expected := range expected {
			require.Equal(t, expected.Token.Kind, result[i].Token.Kind)
			require.Equal(t, expected.Token.Value, result[i].Token.Value)
			require.Equal(t, expected.Priority, result[i].Priority)
		}
	})

	t.Run("выражение с булевым значением", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.IdentifierToken, Value: "is_active"},
			{Kind: lex.MathOperatorToken, Value: "="},
		}
		expected := []*tokenWithPrior{
			{Token: &lex.Token{Kind: lex.IdentifierToken, Value: "is_active"}, Priority: 41},
			{Token: &lex.Token{Kind: lex.MathOperatorToken, Value: "="}, Priority: 40},
		}

		result := addTokensPriority(tokens)
		require.Len(t, result, len(expected))
		for i, expected := range expected {
			require.Equal(t, expected.Token.Kind, result[i].Token.Kind)
			require.Equal(t, expected.Token.Value, result[i].Token.Value)
			require.Equal(t, expected.Priority, result[i].Priority)
		}
	})
}

func TestGetPriority(t *testing.T) {
	t.Run("идентификатор", func(t *testing.T) {
		token := &lex.Token{Kind: lex.IdentifierToken, Value: "name"}
		subPriority := uint(0)
		identPrior := uint(0)
		expected := uint(40)

		result := getPriority(token, subPriority, identPrior)
		require.Equal(t, expected, result)
	})

	t.Run("оператор OR", func(t *testing.T) {
		token := &lex.Token{Kind: lex.SymbolToken, Value: string(lex.OrOperator)}
		subPriority := uint(100)
		identPrior := uint(0)
		expected := uint(120)

		result := getPriority(token, subPriority, identPrior)
		require.Equal(t, expected, result)
	})

	t.Run("оператор AND", func(t *testing.T) {
		token := &lex.Token{Kind: lex.SymbolToken, Value: string(lex.AndOperator)}
		subPriority := uint(100)
		identPrior := uint(0)
		expected := uint(130)

		result := getPriority(token, subPriority, identPrior)
		require.Equal(t, expected, result)
	})

	t.Run("оператор сравнения", func(t *testing.T) {
		token := &lex.Token{Kind: lex.SymbolToken, Value: string(lex.EqualOperator)}
		subPriority := uint(100)
		identPrior := uint(0)
		expected := uint(140)

		result := getPriority(token, subPriority, identPrior)
		require.Equal(t, expected, result)
	})

	t.Run("неизвестный оператор", func(t *testing.T) {
		token := &lex.Token{Kind: lex.SymbolToken, Value: "unknown"}
		subPriority := uint(100)
		identPrior := uint(0)
		expected := uint(0)

		result := getPriority(token, subPriority, identPrior)
		require.Equal(t, expected, result)
	})

}

func TestParseTree(t *testing.T) {
	t.Run("пустой список токенов", func(t *testing.T) {
		tokens := []*tokenWithPrior{}
		expected := (*WhereClause)(nil)

		result := parseTree(tokens)
		require.Equal(t, expected, result)
	})

	t.Run("один токен", func(t *testing.T) {
		tokens := []*tokenWithPrior{
			{Token: &lex.Token{Kind: lex.IdentifierToken, Value: "name"}, Priority: 40},
		}
		expected := &WhereClause{
			Token: &lex.Token{Kind: lex.IdentifierToken, Value: "name"},
		}

		result := parseTree(tokens)
		require.True(t, compareTrees(result, expected))
	})

	t.Run("простое сравнение", func(t *testing.T) {
		tokens := []*tokenWithPrior{
			{Token: &lex.Token{Kind: lex.IdentifierToken, Value: "age"}, Priority: 41},
			{Token: &lex.Token{Kind: lex.SymbolToken, Value: ">"}, Priority: 40},
			{Token: &lex.Token{Kind: lex.NumericToken, Value: "18"}, Priority: 42},
		}
		expected := &WhereClause{
			Token: &lex.Token{Kind: lex.SymbolToken, Value: ">"},
			Left: &WhereClause{
				Token: &lex.Token{Kind: lex.IdentifierToken, Value: "age"},
			},
			Right: &WhereClause{
				Token: &lex.Token{Kind: lex.NumericToken, Value: "18"},
			},
		}

		result := parseTree(tokens)
		require.True(t, compareTrees(result, expected))
	})

	t.Run("сложное выражение с AND", func(t *testing.T) {
		tokens := []*tokenWithPrior{
			{Token: &lex.Token{Kind: lex.IdentifierToken, Value: "age"}, Priority: 41},
			{Token: &lex.Token{Kind: lex.SymbolToken, Value: ">"}, Priority: 40},
			{Token: &lex.Token{Kind: lex.NumericToken, Value: "18"}, Priority: 42},
			{Token: &lex.Token{Kind: lex.SymbolToken, Value: "AND"}, Priority: 30},
			{Token: &lex.Token{Kind: lex.IdentifierToken, Value: "name"}, Priority: 41},
			{Token: &lex.Token{Kind: lex.SymbolToken, Value: "="}, Priority: 40},
			{Token: &lex.Token{Kind: lex.StringToken, Value: "'John'"}, Priority: 42},
		}
		expected := &WhereClause{
			Token: &lex.Token{Kind: lex.SymbolToken, Value: "AND"},
			Left: &WhereClause{
				Token: &lex.Token{Kind: lex.SymbolToken, Value: ">"},
				Left: &WhereClause{
					Token: &lex.Token{Kind: lex.IdentifierToken, Value: "age"},
				},
				Right: &WhereClause{
					Token: &lex.Token{Kind: lex.NumericToken, Value: "18"},
				},
			},
			Right: &WhereClause{
				Token: &lex.Token{Kind: lex.SymbolToken, Value: "="},
				Left: &WhereClause{
					Token: &lex.Token{Kind: lex.IdentifierToken, Value: "name"},
				},
				Right: &WhereClause{
					Token: &lex.Token{Kind: lex.StringToken, Value: "'John'"},
				},
			},
		}

		result := parseTree(tokens)
		require.True(t, compareTrees(result, expected))
	})

}

func compareTrees(a, b *WhereClause) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	if a.Token.Kind != b.Token.Kind || a.Token.Value != b.Token.Value {
		return false
	}

	return compareTrees(a.Left, b.Left) && compareTrees(a.Right, b.Right)
}
