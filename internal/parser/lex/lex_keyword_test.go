package lex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLexKeyword(t *testing.T) {
	t.Run("valid keyword", func(t *testing.T) {
		input := "SELECT"
		want := "select"
		startPointer := uint(0)

		got, newPointer, isValid := lexKeyword(input, startPointer)

		require.True(t, isValid)
		require.Equal(t, want, got.Value)
		require.NotEqual(t, startPointer, newPointer)
	})

	t.Run("invalid keyword", func(t *testing.T) {
		input := "not a keyword"
		startPointer := uint(0)

		_, _, isValid := lexKeyword(input, startPointer)

		require.False(t, isValid)
	})

	t.Run("empty input", func(t *testing.T) {
		input := ""
		startPointer := uint(0)

		_, _, isValid := lexKeyword(input, startPointer)

		require.False(t, isValid)
	})
}
