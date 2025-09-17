package lex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLexString(t *testing.T) {
	t.Run("valid string", func(t *testing.T) {
		input := "'hello world'"
		want := "hello world"
		startPointer := uint(0)

		got, newPointer, isValid := lexString(input, startPointer)

		require.True(t, isValid)
		require.Equal(t, want, got.Value)
		require.NotEqual(t, startPointer, newPointer)
	})

	t.Run("empty string", func(t *testing.T) {
		input := "''"
		startPointer := uint(0)

		got, newPointer, isValid := lexString(input, startPointer)

		require.True(t, isValid)
		require.Equal(t, "", got.Value)
		require.NotEqual(t, startPointer, newPointer)
	})

	t.Run("unclosed string", func(t *testing.T) {
		input := "'hello world"
		startPointer := uint(0)

		_, _, isValid := lexString(input, startPointer)

		require.False(t, isValid)
	})

	t.Run("invalid string", func(t *testing.T) {
		input := "hello world"
		startPointer := uint(0)

		_, _, isValid := lexString(input, startPointer)

		require.False(t, isValid)
	})
}
