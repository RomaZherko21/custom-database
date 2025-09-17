package lex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLexNumeric(t *testing.T) {
	t.Run("valid integer", func(t *testing.T) {
		input := "42"
		want := "42"
		startPointer := uint(0)

		got, newPointer, isValid := lexNumeric(input, startPointer)

		require.True(t, isValid)
		require.Equal(t, want, got.Value)
		require.NotEqual(t, startPointer, newPointer)
	})

	t.Run("valid float", func(t *testing.T) {
		input := "3.14"
		want := "3.14"
		startPointer := uint(0)

		got, newPointer, isValid := lexNumeric(input, startPointer)

		require.True(t, isValid)
		require.Equal(t, want, got.Value)
		require.NotEqual(t, startPointer, newPointer)
	})

	t.Run("invalid number", func(t *testing.T) {
		input := "not a number"
		startPointer := uint(0)

		_, _, isValid := lexNumeric(input, startPointer)

		require.False(t, isValid)
	})

	t.Run("empty input", func(t *testing.T) {
		input := ""
		startPointer := uint(0)

		_, _, isValid := lexNumeric(input, startPointer)

		require.False(t, isValid)
	})
}
