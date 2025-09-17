package ast

import (
	"custom-database/internal/parser/lex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSelectStatement(t *testing.T) {

	t.Run("valid SELECT statement with *", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.KeywordToken, Value: "select"},
			{Kind: lex.SymbolToken, Value: "*"},
			{Kind: lex.KeywordToken, Value: "from"},
			{Kind: lex.IdentifierToken, Value: "users"},
			{Kind: lex.SymbolToken, Value: ";"},
		}

		result, pointer, ok := parseSelectStatement(tokens, 0)

		require.True(t, ok)
		require.Equal(t, uint(4), pointer)
		require.Len(t, result.SelectedColumns, 0)
		require.Equal(t, "users", result.Table.Value)
	})

	t.Run("valid SELECT statement with FROM", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.KeywordToken, Value: "select"},
			{Kind: lex.IdentifierToken, Value: "id"},
			{Kind: lex.SymbolToken, Value: ","},
			{Kind: lex.IdentifierToken, Value: "name"},
			{Kind: lex.SymbolToken, Value: ","},
			{Kind: lex.IdentifierToken, Value: "is_active"},
			{Kind: lex.SymbolToken, Value: ","},
			{Kind: lex.IdentifierToken, Value: "registered_at"},
			{Kind: lex.KeywordToken, Value: "from"},
			{Kind: lex.IdentifierToken, Value: "users"},
			{Kind: lex.SymbolToken, Value: ";"},
		}

		result, pointer, ok := parseSelectStatement(tokens, 0)

		require.True(t, ok)
		require.Equal(t, uint(10), pointer)
		require.Len(t, result.SelectedColumns, 4)
		require.Equal(t, "id", result.SelectedColumns[0].Literal.Value)
		require.Equal(t, "name", result.SelectedColumns[1].Literal.Value)
		require.Equal(t, "is_active", result.SelectedColumns[2].Literal.Value)
		require.Equal(t, "registered_at", result.SelectedColumns[3].Literal.Value)
		require.Equal(t, "users", result.Table.Value)
	})

	t.Run("invalid SELECT statement - missing SELECT keyword", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.IdentifierToken, Value: "id"},
			{Kind: lex.SymbolToken, Value: ","},
			{Kind: lex.IdentifierToken, Value: "name"},
		}

		result, pointer, ok := parseSelectStatement(tokens, 0)

		require.False(t, ok)
		require.Equal(t, uint(0), pointer)
		require.Nil(t, result)
	})

	t.Run("invalid SELECT statement - missing expressions", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.KeywordToken, Value: "select"},
		}

		result, pointer, ok := parseSelectStatement(tokens, 0)

		require.False(t, ok)
		require.Equal(t, uint(0), pointer)
		require.Nil(t, result)
	})

	t.Run("invalid SELECT statement - missing table name after FROM", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.KeywordToken, Value: "select"},
			{Kind: lex.IdentifierToken, Value: "id"},
			{Kind: lex.KeywordToken, Value: "from"},
		}

		result, pointer, ok := parseSelectStatement(tokens, 0)

		require.False(t, ok)
		require.Equal(t, uint(0), pointer)
		require.Nil(t, result)
	})

	t.Run("invalid SELECT statement - incomplete WHERE clause", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.KeywordToken, Value: "select"},
			{Kind: lex.IdentifierToken, Value: "id"},
			{Kind: lex.KeywordToken, Value: "from"},
			{Kind: lex.IdentifierToken, Value: "users"},
			{Kind: lex.KeywordToken, Value: "where"},
			{Kind: lex.IdentifierToken, Value: "age"},
			{Kind: lex.MathOperatorToken, Value: "="},
			{Kind: lex.NumericToken, Value: "10"},
		}

		result, pointer, ok := parseSelectStatement(tokens, 0)

		require.False(t, ok)
		require.Equal(t, uint(0), pointer)
		require.Nil(t, result)
	})
}
