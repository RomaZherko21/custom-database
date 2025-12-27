package e2e

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDistinct(t *testing.T) {
	t.Run("Create Table", func(t *testing.T) {
		query := "CREATE TABLE test_distinct (id INT, name TEXT, city TEXT);"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})

	t.Run("Insert Data with Duplicates", func(t *testing.T) {
		queries := []string{
			"INSERT INTO test_distinct VALUES (1, 'Alice', 'Moscow');",
			"INSERT INTO test_distinct VALUES (2, 'Bob', 'Moscow');",
			"INSERT INTO test_distinct VALUES (3, 'Charlie', 'SPB');",
			"INSERT INTO test_distinct VALUES (4, 'Alice', 'Moscow');",
			"INSERT INTO test_distinct VALUES (5, 'Bob', 'SPB');",
			"INSERT INTO test_distinct VALUES (6, 'David', 'Moscow');",
		}

		query := queries[0] + ";"
		for i := 1; i < len(queries); i++ {
			query += " " + queries[i] + ";"
		}

		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})

	t.Run("DISTINCT on single column", func(t *testing.T) {
		query := "SELECT DISTINCT city FROM test_distinct;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		// Должно быть 2 уникальных города
		assert.Contains(t, resultStr, "Moscow")
		assert.Contains(t, resultStr, "SPB")
	})

	t.Run("DISTINCT on multiple columns", func(t *testing.T) {
		query := "SELECT DISTINCT name, city FROM test_distinct;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		// Проверяем уникальные комбинации
		assert.Contains(t, resultStr, "Alice")
		assert.Contains(t, resultStr, "Bob")
		assert.Contains(t, resultStr, "Charlie")
		assert.Contains(t, resultStr, "David")
	})

	t.Run("DISTINCT with WHERE", func(t *testing.T) {
		query := "SELECT DISTINCT name FROM test_distinct WHERE city = 'Moscow';"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		assert.Contains(t, resultStr, "Alice")
		assert.Contains(t, resultStr, "Bob")
		assert.Contains(t, resultStr, "David")
		// Charlie не должен быть (он в SPB)
		assert.NotContains(t, resultStr, "Charlie")
	})

	t.Run("DISTINCT on all columns", func(t *testing.T) {
		query := "SELECT DISTINCT * FROM test_distinct;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		// Все записи должны быть уникальными (все 6 записей)
		assert.Contains(t, resultStr, "Alice")
		assert.Contains(t, resultStr, "Bob")
		assert.Contains(t, resultStr, "Charlie")
		assert.Contains(t, resultStr, "David")
	})

	t.Run("Drop Table", func(t *testing.T) {
		query := "DROP TABLE test_distinct;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})
}
