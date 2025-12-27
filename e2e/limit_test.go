package e2e

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLimit(t *testing.T) {
	t.Run("Create Table", func(t *testing.T) {
		query := "CREATE TABLE test_limit (id INT, name TEXT);"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})

	t.Run("Insert Data", func(t *testing.T) {
		var queries []string
		for i := 1; i <= 20; i++ {
			queries = append(queries, fmt.Sprintf("INSERT INTO test_limit VALUES (%d, 'Name%d');", i, i))
		}

		query := queries[0] + ";"
		for i := 1; i < len(queries); i++ {
			query += " " + queries[i] + ";"
		}

		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})

	t.Run("LIMIT 5", func(t *testing.T) {
		query := "SELECT id, name FROM test_limit LIMIT 5;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		// Проверяем, что есть 5 записей
		assert.Contains(t, resultStr, "Name1")
		assert.Contains(t, resultStr, "Name5")
		// Не должно быть больше 5
		assert.NotContains(t, resultStr, "Name6")
	})

	t.Run("LIMIT 10", func(t *testing.T) {
		query := "SELECT id, name FROM test_limit LIMIT 10;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		assert.Contains(t, resultStr, "Name1")
		assert.Contains(t, resultStr, "Name10")
		assert.NotContains(t, resultStr, "Name11")
	})

	t.Run("LIMIT with WHERE", func(t *testing.T) {
		query := "SELECT id, name FROM test_limit WHERE id > 10 LIMIT 3;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		// Должно быть максимум 3 записи с id > 10
		assert.NotContains(t, resultStr, "Name10")
	})

	t.Run("LIMIT больше чем записей", func(t *testing.T) {
		query := "SELECT id, name FROM test_limit LIMIT 100;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		// Должны вернуться все 20 записей
		assert.Contains(t, resultStr, "Name20")
		assert.NotContains(t, resultStr, "Name21")
	})

	t.Run("Drop Table", func(t *testing.T) {
		query := "DROP TABLE test_limit;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})
}
