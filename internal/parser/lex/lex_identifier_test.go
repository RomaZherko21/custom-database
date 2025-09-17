package lex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLexIdentifier(t *testing.T) {
	t.Run("valid identifier", func(t *testing.T) {
		input := "table_name"
		want := "table_name"
		startPointer := uint(0)

		got, newPointer, isValid := lexIdentifier(input, startPointer)

		require.True(t, isValid)
		require.Equal(t, want, got.Value)
		require.NotEqual(t, startPointer, newPointer)
	})

	t.Run("invalid identifier", func(t *testing.T) {
		input := "123table"
		startPointer := uint(0)

		_, _, isValid := lexIdentifier(input, startPointer)

		require.False(t, isValid)
	})

}
