package lex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLexCharacterDelimited(t *testing.T) {
	t.Run("valid delimited string", func(t *testing.T) {
		input := "'hello world'"
		want := "hello world"
		startPointer := uint(0)

		got, newPointer, isValid := lexCharacterDelimited(input, startPointer, '\'')

		require.True(t, isValid)
		require.Equal(t, want, got.Value)
		require.NotEqual(t, startPointer, newPointer)
	})

	t.Run("empty string", func(t *testing.T) {
		input := "''"
		startPointer := uint(0)

		got, newPointer, isValid := lexCharacterDelimited(input, startPointer, '\'')

		require.True(t, isValid)
		require.Equal(t, "", got.Value)
		require.NotEqual(t, startPointer, newPointer)
	})

	t.Run("unclosed string", func(t *testing.T) {
		input := "'hello world"
		startPointer := uint(0)

		_, _, isValid := lexCharacterDelimited(input, startPointer, '\'')

		require.False(t, isValid)
	})

	t.Run("wrong delimiter", func(t *testing.T) {
		input := "'hello world'"
		startPointer := uint(0)

		_, _, isValid := lexCharacterDelimited(input, startPointer, '?')

		require.False(t, isValid)
	})

	t.Run("double delimiter", func(t *testing.T) {
		input := "'he''llo'"
		startPointer := uint(0)

		got, _, isValid := lexCharacterDelimited(input, startPointer, '\'')

		require.True(t, isValid)
		require.Equal(t, "he''llo", got.Value)
	})
}
