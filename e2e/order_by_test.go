package e2e

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderBy(t *testing.T) {
	t.Run("Create Table", func(t *testing.T) {
		query := "CREATE TABLE test_order_by (id INT, name TEXT, age INT, score INT);"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})

	t.Run("Insert Data", func(t *testing.T) {
		queries := []string{
			"INSERT INTO test_order_by VALUES (1, 'Alice', 25, 100);",
			"INSERT INTO test_order_by VALUES (2, 'Bob', 30, 90);",
			"INSERT INTO test_order_by VALUES (3, 'Charlie', 20, 95);",
			"INSERT INTO test_order_by VALUES (4, 'David', 25, 85);",
			"INSERT INTO test_order_by VALUES (5, 'Eve', 30, 110);",
		}

		query := queries[0] + ";"
		for i := 1; i < len(queries); i++ {
			query += " " + queries[i] + ";"
		}

		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})

	t.Run("ORDER BY single column ASC", func(t *testing.T) {
		query := "SELECT id, name, age FROM test_order_by ORDER BY age ASC;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		// Должно быть отсортировано по возрасту: 20, 25, 25, 30, 30
		assert.Contains(t, resultStr, "Charlie") // age 20
		assert.Contains(t, resultStr, "Alice")   // age 25
		assert.Contains(t, resultStr, "David")   // age 25
		assert.Contains(t, resultStr, "Bob")     // age 30
		assert.Contains(t, resultStr, "Eve")     // age 30
	})

	t.Run("ORDER BY single column DESC", func(t *testing.T) {
		query := "SELECT id, name, age FROM test_order_by ORDER BY age DESC;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		// Должно быть отсортировано по убыванию: 30, 30, 25, 25, 20
		assert.Contains(t, resultStr, "Bob")
		assert.Contains(t, resultStr, "Eve")
		assert.Contains(t, resultStr, "Alice")
		assert.Contains(t, resultStr, "David")
		assert.Contains(t, resultStr, "Charlie")
	})

	t.Run("ORDER BY multiple columns", func(t *testing.T) {
		query := "SELECT id, name, age, score FROM test_order_by ORDER BY age ASC, score DESC;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		// Сначала по возрасту, потом по score
		assert.Contains(t, resultStr, "Charlie") // age 20, score 95
		assert.Contains(t, resultStr, "Alice")   // age 25, score 100
		assert.Contains(t, resultStr, "David")   // age 25, score 85
		assert.Contains(t, resultStr, "Eve")     // age 30, score 110
		assert.Contains(t, resultStr, "Bob")     // age 30, score 90
	})

	t.Run("ORDER BY with WHERE", func(t *testing.T) {
		query := "SELECT id, name, score FROM test_order_by WHERE age = 25 ORDER BY score DESC;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		// Только записи с age = 25, отсортированные по score
		assert.Contains(t, resultStr, "Alice") // score 100
		assert.Contains(t, resultStr, "David") // score 85
		assert.NotContains(t, resultStr, "Bob")
		assert.NotContains(t, resultStr, "Charlie")
		assert.NotContains(t, resultStr, "Eve")
	})

	t.Run("ORDER BY with LIMIT", func(t *testing.T) {
		query := "SELECT id, name, score FROM test_order_by ORDER BY score DESC LIMIT 3;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		// Топ-3 по score: 110, 100, 95
		assert.Contains(t, resultStr, "Eve")      // score 110
		assert.Contains(t, resultStr, "Alice")    // score 100
		assert.Contains(t, resultStr, "Charlie")  // score 95
		assert.NotContains(t, resultStr, "Bob")   // score 90
		assert.NotContains(t, resultStr, "David") // score 85
	})

	t.Run("ORDER BY text column", func(t *testing.T) {
		query := "SELECT id, name FROM test_order_by ORDER BY name ASC;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		// Должно быть отсортировано по имени: Alice, Bob, Charlie, David, Eve
		assert.Contains(t, resultStr, "Alice")
		assert.Contains(t, resultStr, "Bob")
		assert.Contains(t, resultStr, "Charlie")
		assert.Contains(t, resultStr, "David")
		assert.Contains(t, resultStr, "Eve")
	})

	t.Run("Drop Table", func(t *testing.T) {
		query := "DROP TABLE test_order_by;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})
}
