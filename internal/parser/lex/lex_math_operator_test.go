package lex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLexOperator(t *testing.T) {
	t.Run("invalid operator", func(t *testing.T) {
		input := "not an operator"
		startPointer := uint(0)

		_, _, isValid := lexMathOperator(input, startPointer)

		require.False(t, isValid)
	})

	t.Run("multiple valid operators", func(t *testing.T) {
		tests := []struct {
			input       string
			want        string
			wantPointer uint
		}{
			{"=", string(EqualOperator), 1},
			{"<", string(LessThanOperator), 1},
			{">", string(GreaterThanOperator), 1},
			{"!=", string(NotEqualOperator), 2},
		}

		for _, tt := range tests {
			t.Run(tt.input, func(t *testing.T) {
				startPointer := uint(0)
				got, newPointer, isValid := lexMathOperator(tt.input, startPointer)

				require.True(t, isValid)
				require.Equal(t, tt.want, got.Value)
				require.Equal(t, tt.wantPointer, newPointer)
			})
		}
	})

	t.Run("multiple strange strings", func(t *testing.T) {
		tests := []struct {
			input       string
			want        string
			wantPointer uint
		}{
			{"====", string(EqualOperator), 1},
			{"<=-", string(LessThanOperator), 1},
			{">--=", string(GreaterThanOperator), 1},
			{"!===", string(NotEqualOperator), 2},
		}

		for _, tt := range tests {
			t.Run(tt.input, func(t *testing.T) {
				startPointer := uint(0)
				got, newPointer, isValid := lexMathOperator(tt.input, startPointer)

				require.True(t, isValid)
				require.Equal(t, tt.want, got.Value)
				require.Equal(t, tt.wantPointer, newPointer)
			})
		}
	})
}
