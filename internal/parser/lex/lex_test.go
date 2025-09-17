package lex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLex(t *testing.T) {
	t.Run("CREATE TABLE command", func(t *testing.T) {
		input := "CREATE TABLE users (id INT, name TEXT);"
		want := []*Token{
			{Kind: KeywordToken, Value: "create"},
			{Kind: KeywordToken, Value: "table"},
			{Kind: IdentifierToken, Value: "users"},
			{Kind: SymbolToken, Value: "("},
			{Kind: IdentifierToken, Value: "id"},
			{Kind: KeywordToken, Value: "int"},
			{Kind: SymbolToken, Value: ","},
			{Kind: IdentifierToken, Value: "name"},
			{Kind: KeywordToken, Value: "text"},
			{Kind: SymbolToken, Value: ")"},
			{Kind: SymbolToken, Value: ";"},
		}

		got, err := NewLexer().Lex(input)

		require.NoError(t, err)

		for i, token := range want {
			if token.Kind != got[i].Kind || token.Value != got[i].Value {
				t.Errorf("\nОшибка в токене %d:\nОжидалось: {Kind: %v, Value: %q}\nПолучено:  {Kind: %v, Value: %q}",
					i, token.Kind, token.Value, got[i].Kind, got[i].Value)
			}
		}
	})

	t.Run("INSERT INTO command", func(t *testing.T) {
		input := "INSERT INTO users (id, name) VALUES (1, 'Phil');"
		want := []*Token{
			{Kind: KeywordToken, Value: "insert"},
			{Kind: KeywordToken, Value: "into"},
			{Kind: IdentifierToken, Value: "users"},
			{Kind: SymbolToken, Value: "("},
			{Kind: IdentifierToken, Value: "id"},
			{Kind: SymbolToken, Value: ","},
			{Kind: IdentifierToken, Value: "name"},
			{Kind: SymbolToken, Value: ")"},
			{Kind: KeywordToken, Value: "values"},
			{Kind: SymbolToken, Value: "("},
			{Kind: NumericToken, Value: "1"},
			{Kind: SymbolToken, Value: ","},
			{Kind: StringToken, Value: "Phil"},
			{Kind: SymbolToken, Value: ")"},
			{Kind: SymbolToken, Value: ";"},
		}

		got, err := NewLexer().Lex(input)

		require.NoError(t, err)

		for i, token := range want {
			if token.Kind != got[i].Kind || token.Value != got[i].Value {
				t.Errorf("\nОшибка в токене %d:\nОжидалось: {Kind: %v, Value: %q}\nПолучено:  {Kind: %v, Value: %q}",
					i, token.Kind, token.Value, got[i].Kind, got[i].Value)
			}
		}
	})

	t.Run("SELECT command", func(t *testing.T) {
		input := "SELECT id, name FROM users;"
		want := []*Token{
			{Kind: KeywordToken, Value: "select"},
			{Kind: IdentifierToken, Value: "id"},
			{Kind: SymbolToken, Value: ","},
			{Kind: IdentifierToken, Value: "name"},
			{Kind: KeywordToken, Value: "from"},
			{Kind: IdentifierToken, Value: "users"},
			{Kind: SymbolToken, Value: ";"},
		}

		got, err := NewLexer().Lex(input)

		require.NoError(t, err)

		for i, token := range want {
			if token.Kind != got[i].Kind || token.Value != got[i].Value {
				t.Errorf("\nОшибка в токене %d:\nОжидалось: {Kind: %v, Value: %q}\nПолучено:  {Kind: %v, Value: %q}",
					i, token.Kind, token.Value, got[i].Kind, got[i].Value)
			}
		}
	})

	t.Run("DROP TABLE command", func(t *testing.T) {
		input := "DROP TABLE users;"
		want := []*Token{
			{Kind: KeywordToken, Value: "drop"},
			{Kind: KeywordToken, Value: "table"},
			{Kind: IdentifierToken, Value: "users"},
			{Kind: SymbolToken, Value: ";"},
		}

		got, err := NewLexer().Lex(input)

		require.NoError(t, err)

		for i, token := range want {
			if token.Kind != got[i].Kind || token.Value != got[i].Value {
				t.Errorf("\nОшибка в токене %d:\nОжидалось: {Kind: %v, Value: %q}\nПолучено:  {Kind: %v, Value: %q}",
					i, token.Kind, token.Value, got[i].Kind, got[i].Value)
			}
		}
	})

	t.Run("invalid SQL", func(t *testing.T) {
		input := "SELECT #;"

		_, err := NewLexer().Lex(input)

		require.Error(t, err)
	})

	t.Run("invalid input", func(t *testing.T) {
		input := "SELECT * FROM users WHERE id = @1;"

		_, err := NewLexer().Lex(input)

		require.Error(t, err)
	})
}
