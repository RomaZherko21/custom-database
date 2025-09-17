package lex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLexSymbol(t *testing.T) {
	t.Run("valid symbol", func(t *testing.T) {
		input := ";"
		want := ";"
		startPointer := uint(0)

		got, newPointer, isValid := lexSymbol(input, startPointer)

		require.True(t, isValid)
		require.Equal(t, want, got.Value)
		require.NotEqual(t, startPointer, newPointer)
	})

	t.Run("invalid symbol", func(t *testing.T) {
		input := "not a symbol"
		startPointer := uint(0)

		_, _, isValid := lexSymbol(input, startPointer)

		require.False(t, isValid)
	})

}
