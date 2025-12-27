package e2e

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJoin(t *testing.T) {
	t.Run("Create Tables", func(t *testing.T) {
		queries := []string{
			"CREATE TABLE test_join_users (id INT, name TEXT);",
			"CREATE TABLE test_join_posts (id INT, user_id INT, title TEXT);",
		}

		for _, query := range queries {
			response := executeQuery(t, query)
			assert.Empty(t, response.Error)
		}
	})

	t.Run("Insert Data", func(t *testing.T) {
		queries := []string{
			"INSERT INTO test_join_users VALUES (1, 'Alice');",
			"INSERT INTO test_join_users VALUES (2, 'Bob');",
			"INSERT INTO test_join_users VALUES (3, 'Charlie');",
			"INSERT INTO test_join_posts VALUES (1, 1, 'Post 1');",
			"INSERT INTO test_join_posts VALUES (2, 1, 'Post 2');",
			"INSERT INTO test_join_posts VALUES (3, 2, 'Post 3');",
			"INSERT INTO test_join_posts VALUES (4, 3, 'Post 4');",
		}

		for _, query := range queries {
			response := executeQuery(t, query)
			assert.Empty(t, response.Error)
		}
	})

	t.Run("Simple JOIN", func(t *testing.T) {
		query := "SELECT u.id, u.name, p.title FROM test_join_users u JOIN test_join_posts p ON u.id = p.user_id;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		// Проверяем, что получили 4 строки (каждый пост соединен с пользователем)
		assert.Contains(t, response.Result, "rows")
		// Детальная проверка структуры результата
		resultStr := fmt.Sprintf("%v", response.Result)
		assert.Contains(t, resultStr, "Alice")
		assert.Contains(t, resultStr, "Bob")
		assert.Contains(t, resultStr, "Charlie")
		assert.Contains(t, resultStr, "Post 1")
		assert.Contains(t, resultStr, "Post 2")
		assert.Contains(t, resultStr, "Post 3")
		assert.Contains(t, resultStr, "Post 4")
	})

	t.Run("JOIN with WHERE", func(t *testing.T) {
		query := "SELECT u.name, p.title FROM test_join_users u JOIN test_join_posts p ON u.id = p.user_id WHERE u.id = 1;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		resultStr := fmt.Sprintf("%v", response.Result)
		assert.Contains(t, resultStr, "Alice")
		assert.Contains(t, resultStr, "Post 1")
		assert.Contains(t, resultStr, "Post 2")
		// Не должно быть постов других пользователей
		assert.NotContains(t, resultStr, "Post 3")
	})

	t.Run("Cleanup", func(t *testing.T) {
		queries := []string{
			"DROP TABLE test_join_posts;",
			"DROP TABLE test_join_users;",
		}

		for _, query := range queries {
			response := executeQuery(t, query)
			assert.Empty(t, response.Error)
		}
	})
}
