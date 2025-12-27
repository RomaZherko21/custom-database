package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseMultipleStatements(t *testing.T) {
	p := NewParser()

	t.Run("Single statement", func(t *testing.T) {
		statements, err := p.Parse("INSERT INTO test VALUES (1, 'A');")
		assert.NoError(t, err)
		assert.Len(t, statements, 1)
		assert.Equal(t, "INSERT INTO test VALUES (1, 'A')", statements[0].SQL)
	})

	t.Run("Multiple statements", func(t *testing.T) {
		query := "INSERT INTO test VALUES (1, 'A'); INSERT INTO test VALUES (2, 'B');"
		statements, err := p.Parse(query)
		assert.NoError(t, err)
		assert.Len(t, statements, 2)
		assert.Equal(t, "INSERT INTO test VALUES (1, 'A')", statements[0].SQL)
		assert.Equal(t, "INSERT INTO test VALUES (2, 'B')", statements[1].SQL)
	})

	t.Run("Multiple statements with spaces", func(t *testing.T) {
		query := "INSERT INTO test VALUES (1, 'Name1'); INSERT INTO test VALUES (2, 'Name2');"
		statements, err := p.Parse(query)
		assert.NoError(t, err)
		assert.Len(t, statements, 2)
	})

	t.Run("Statement without semicolon", func(t *testing.T) {
		statements, err := p.Parse("INSERT INTO test VALUES (1, 'A')")
		assert.NoError(t, err)
		assert.Len(t, statements, 1)
	})

	t.Run("Empty query", func(t *testing.T) {
		statements, err := p.Parse("")
		assert.NoError(t, err)
		assert.Len(t, statements, 0)
	})
}

