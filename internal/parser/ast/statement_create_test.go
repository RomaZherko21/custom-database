package ast

import (
	"custom-database/internal/parser/lex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCreateTableStatement(t *testing.T) {
	t.Run("valid CREATE TABLE statement", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.KeywordToken, Value: "create"},
			{Kind: lex.KeywordToken, Value: "table"},
			{Kind: lex.IdentifierToken, Value: "users"},
			{Kind: lex.SymbolToken, Value: "("},
			{Kind: lex.IdentifierToken, Value: "id"},
			{Kind: lex.KeywordToken, Value: "int"},
			{Kind: lex.SymbolToken, Value: ","},
			{Kind: lex.IdentifierToken, Value: "name"},
			{Kind: lex.KeywordToken, Value: "text"},
			{Kind: lex.SymbolToken, Value: ","},
			{Kind: lex.IdentifierToken, Value: "is_active"},
			{Kind: lex.KeywordToken, Value: "boolean"},
			{Kind: lex.SymbolToken, Value: ","},
			{Kind: lex.IdentifierToken, Value: "registered_at"},
			{Kind: lex.KeywordToken, Value: "timestamp"},
			{Kind: lex.SymbolToken, Value: ")"},
			{Kind: lex.SymbolToken, Value: ";"},
		}

		result, pointer, ok := parseCreateTableStatement(tokens, 0)

		require.True(t, ok)
		require.Equal(t, uint(16), pointer)
		require.Equal(t, "users", result.Table.Value)
		require.Len(t, *result.Columns, 4)
		require.Equal(t, "id", (*result.Columns)[0].Name.Value)
		require.Equal(t, "int", (*result.Columns)[0].Datatype.Value)
		require.Equal(t, "name", (*result.Columns)[1].Name.Value)
		require.Equal(t, "text", (*result.Columns)[1].Datatype.Value)
		require.Equal(t, "is_active", (*result.Columns)[2].Name.Value)
		require.Equal(t, "boolean", (*result.Columns)[2].Datatype.Value)
		require.Equal(t, "registered_at", (*result.Columns)[3].Name.Value)
		require.Equal(t, "timestamp", (*result.Columns)[3].Datatype.Value)
	})

	t.Run("invalid CREATE statement - missing TABLE keyword", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.KeywordToken, Value: "create"},
			{Kind: lex.IdentifierToken, Value: "users"},
		}

		result, pointer, ok := parseCreateTableStatement(tokens, 0)

		require.False(t, ok)
		require.Equal(t, uint(0), pointer)
		require.Nil(t, result)
	})

	t.Run("invalid CREATE TABLE statement - missing table name", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.KeywordToken, Value: "create"},
			{Kind: lex.KeywordToken, Value: "table"},
		}

		result, pointer, ok := parseCreateTableStatement(tokens, 0)

		require.False(t, ok)
		require.Equal(t, uint(0), pointer)
		require.Nil(t, result)
	})

	t.Run("invalid CREATE TABLE statement - missing left parenthesis", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.KeywordToken, Value: "create"},
			{Kind: lex.KeywordToken, Value: "table"},
			{Kind: lex.IdentifierToken, Value: "users"},
		}

		result, pointer, ok := parseCreateTableStatement(tokens, 0)

		require.False(t, ok)
		require.Equal(t, uint(0), pointer)
		require.Nil(t, result)
	})
}

func TestParseColumnDefinitions(t *testing.T) {
	t.Run("valid column definitions", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.IdentifierToken, Value: "id"},
			{Kind: lex.KeywordToken, Value: "int"},
			{Kind: lex.SymbolToken, Value: ","},
			{Kind: lex.IdentifierToken, Value: "name"},
			{Kind: lex.KeywordToken, Value: "text"},
			{Kind: lex.SymbolToken, Value: ")"},
		}
		endDelimiter := lex.Token{Kind: lex.SymbolToken, Value: ")"}

		cols, pointer, ok := parseColumnDefinitions(tokens, 0, endDelimiter)

		require.True(t, ok)
		require.Equal(t, uint(5), pointer)
		require.Len(t, *cols, 2)
		require.Equal(t, "id", (*cols)[0].Name.Value)
		require.Equal(t, "int", (*cols)[0].Datatype.Value)
		require.Equal(t, "name", (*cols)[1].Name.Value)
		require.Equal(t, "text", (*cols)[1].Datatype.Value)
	})

	t.Run("invalid column definition - missing column type", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.IdentifierToken, Value: "id"},
			{Kind: lex.SymbolToken, Value: ","},
		}
		endDelimiter := lex.Token{Kind: lex.SymbolToken, Value: ")"}

		cols, pointer, ok := parseColumnDefinitions(tokens, 0, endDelimiter)

		require.False(t, ok)
		require.Equal(t, uint(0), pointer)
		require.Nil(t, cols)
	})

	t.Run("invalid column definition - missing comma between columns", func(t *testing.T) {
		tokens := []*lex.Token{
			{Kind: lex.IdentifierToken, Value: "id"},
			{Kind: lex.KeywordToken, Value: "int"},
			{Kind: lex.IdentifierToken, Value: "name"},
			{Kind: lex.KeywordToken, Value: "text"},
		}
		endDelimiter := lex.Token{Kind: lex.SymbolToken, Value: ")"}

		cols, pointer, ok := parseColumnDefinitions(tokens, 0, endDelimiter)

		require.False(t, ok)
		require.Equal(t, uint(0), pointer)
		require.Nil(t, cols)
	})
}
