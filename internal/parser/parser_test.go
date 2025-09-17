package parser

import (
	"custom-database/internal/parser/ast"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Run("valid CREATE TABLE statement", func(t *testing.T) {
		source := "CREATE TABLE users (id INT, name TEXT);"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.NoError(t, err)
		require.Len(t, result.Statements, 1)
		require.Equal(t, ast.CreateTableKind, result.Statements[0].Kind)
		require.Equal(t, "users", result.Statements[0].CreateTableStatement.Table.Value)
		require.Len(t, *result.Statements[0].CreateTableStatement.Columns, 2)
		require.Equal(t, "id", (*result.Statements[0].CreateTableStatement.Columns)[0].Name.Value)
		require.Equal(t, "int", (*result.Statements[0].CreateTableStatement.Columns)[0].Datatype.Value)
		require.Equal(t, "name", (*result.Statements[0].CreateTableStatement.Columns)[1].Name.Value)
		require.Equal(t, "text", (*result.Statements[0].CreateTableStatement.Columns)[1].Datatype.Value)
	})

	t.Run("valid INSERT statement", func(t *testing.T) {
		source := "INSERT INTO users VALUES (1, 'Phil', true);"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.NoError(t, err)
		require.Len(t, result.Statements, 1)
		require.Equal(t, ast.InsertKind, result.Statements[0].Kind)
		require.Equal(t, "users", result.Statements[0].InsertStatement.Table.Value)
		require.Len(t, *result.Statements[0].InsertStatement.Values, 3)
		require.Equal(t, "1", (*result.Statements[0].InsertStatement.Values)[0].Literal.Value)
		require.Equal(t, "Phil", (*result.Statements[0].InsertStatement.Values)[1].Literal.Value)
		require.Equal(t, "true", (*result.Statements[0].InsertStatement.Values)[2].Literal.Value)
	})

	t.Run("valid SELECT statement", func(t *testing.T) {
		source := "SELECT id, name, is_active FROM users;"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.NoError(t, err)
		require.Len(t, result.Statements, 1)
		require.Equal(t, "users", result.Statements[0].SelectStatement.Table.Value)
		require.Equal(t, ast.SelectKind, result.Statements[0].Kind)
		require.Len(t, result.Statements[0].SelectStatement.SelectedColumns, 3)
		require.Equal(t, "id", result.Statements[0].SelectStatement.SelectedColumns[0].Literal.Value)
		require.Equal(t, "name", result.Statements[0].SelectStatement.SelectedColumns[1].Literal.Value)
		require.Equal(t, "is_active", result.Statements[0].SelectStatement.SelectedColumns[2].Literal.Value)
	})

	t.Run("valid DROP TABLE statement", func(t *testing.T) {
		source := "DROP TABLE users;"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.NoError(t, err)
		require.Len(t, result.Statements, 1)
		require.Equal(t, ast.DropTableKind, result.Statements[0].Kind)
		require.Equal(t, "users", result.Statements[0].DropTableStatement.Table.Value)
	})

	t.Run("valid multiple statements", func(t *testing.T) {
		source := "CREATE TABLE users (id INT, name TEXT); INSERT INTO users VALUES (1, 'Phil');"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.NoError(t, err)
		require.Len(t, result.Statements, 2)

		// Проверяем CREATE TABLE
		require.Equal(t, ast.CreateTableKind, result.Statements[0].Kind)
		require.Equal(t, "users", result.Statements[0].CreateTableStatement.Table.Value)
		require.Len(t, *result.Statements[0].CreateTableStatement.Columns, 2)

		// Проверяем INSERT
		require.Equal(t, ast.InsertKind, result.Statements[1].Kind)
		require.Equal(t, "users", result.Statements[1].InsertStatement.Table.Value)
		require.Len(t, *result.Statements[1].InsertStatement.Values, 2)
	})

	t.Run("invalid statement - missing semicolon", func(t *testing.T) {
		source := "CREATE TABLE users (id INT, name TEXT)"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("invalid statement - unknown statement type", func(t *testing.T) {
		source := "UNKNOWN users;"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("invalid statement - malformed CREATE TABLE", func(t *testing.T) {
		source := "CREATE TABLE users (id INT name TEXT);"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("invalid statement - malformed INSERT", func(t *testing.T) {
		source := "INSERT INTO users VALUES 1, 'Phil');"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("invalid statement - malformed SELECT", func(t *testing.T) {
		source := "SELECT id name FROM users;"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("invalid statement - malformed DROP TABLE", func(t *testing.T) {
		source := "DROP TABLE;"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
	})

	// Тесты валидатора - семантические ошибки
	t.Run("validator - CREATE TABLE with duplicate column names", func(t *testing.T) {
		source := "CREATE TABLE users (id INT, id TEXT);"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "Duplicate column name: id")
	})

	t.Run("validator - CREATE TABLE with invalid data type", func(t *testing.T) {
		source := "CREATE TABLE users (id INVALID_TYPE);"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		// Это синтаксическая ошибка - парсер не может распознать INVALID_TYPE как тип данных
		require.Contains(t, err.Error(), "failed to parse")
	})

	t.Run("validator - CREATE TABLE with keyword as table name", func(t *testing.T) {
		source := "CREATE TABLE SELECT (id INT);"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		// Это синтаксическая ошибка - парсер не может распознать SELECT как имя таблицы
		require.Contains(t, err.Error(), "failed to parse")
	})

	t.Run("validator - CREATE TABLE with keyword as column name", func(t *testing.T) {
		source := "CREATE TABLE users (SELECT INT);"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		// Это синтаксическая ошибка - парсер не может распознать SELECT как имя колонки
		require.Contains(t, err.Error(), "failed to parse")
	})

	t.Run("validator - CREATE TABLE with invalid identifier characters", func(t *testing.T) {
		source := "CREATE TABLE test table (id INT);"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		// Это синтаксическая ошибка - парсер не может распознать "test table" как имя таблицы
		require.Contains(t, err.Error(), "failed to parse")
	})

	t.Run("validator - SELECT without FROM clause", func(t *testing.T) {
		source := "SELECT id, name;"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "SELECT statement must have FROM clause")
	})

	t.Run("validator - SELECT with keyword as table name", func(t *testing.T) {
		source := "SELECT id FROM SELECT;"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		// Это синтаксическая ошибка - парсер не может распознать SELECT как имя таблицы
		require.Contains(t, err.Error(), "failed to parse")
	})

	t.Run("validator - SELECT with keyword as column name", func(t *testing.T) {
		source := "SELECT SELECT FROM users;"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		// Это синтаксическая ошибка - парсер не может распознать SELECT как имя колонки
		require.Contains(t, err.Error(), "failed to parse")
	})

	t.Run("validator - INSERT with keyword as table name", func(t *testing.T) {
		source := "INSERT INTO SELECT VALUES (1, 'test');"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		// Это синтаксическая ошибка - парсер не может распознать SELECT как имя таблицы
		require.Contains(t, err.Error(), "failed to parse")
	})

	t.Run("validator - DROP TABLE with keyword as table name", func(t *testing.T) {
		source := "DROP TABLE SELECT;"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		// Это синтаксическая ошибка - парсер не может распознать SELECT как имя таблицы
		require.Contains(t, err.Error(), "failed to parse")
	})

	t.Run("validator - CREATE TABLE with empty table name", func(t *testing.T) {
		source := "CREATE TABLE (id INT);"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		// Это синтаксическая ошибка, но валидатор тоже должен её поймать
	})

	t.Run("validator - CREATE TABLE with empty column name", func(t *testing.T) {
		source := "CREATE TABLE users ( INT);"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		// Это синтаксическая ошибка
	})

	t.Run("validator - CREATE TABLE with empty data type", func(t *testing.T) {
		source := "CREATE TABLE users (id );"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		// Это синтаксическая ошибка
	})

	t.Run("validator - INSERT with empty table name", func(t *testing.T) {
		source := "INSERT INTO VALUES (1, 'test');"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		// Это синтаксическая ошибка
	})

	t.Run("validator - DROP TABLE with empty table name", func(t *testing.T) {
		source := "DROP TABLE ;"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		// Это синтаксическая ошибка
	})

	// Дополнительные тесты для семантических ошибок валидатора
	t.Run("validator - CREATE TABLE with empty column list", func(t *testing.T) {
		source := "CREATE TABLE users ();"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "CREATE TABLE statement must specify at least one column")
	})

	t.Run("validator - INSERT with empty values", func(t *testing.T) {
		source := "INSERT INTO users VALUES ();"
		parser := NewParser()

		result, err := parser.Parse(source)

		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "INSERT statement must specify values")
	})

	// Тесты с валидными запросами из test.txt
	t.Run("validator - valid queries from test.txt", func(t *testing.T) {
		validQueries := []string{
			"CREATE TABLE users (id INT, name TEXT);",
			"CREATE TABLE posts (id INT, title TEXT, content TEXT);",
			"INSERT INTO users VALUES (1, 'Joffrey');",
			"INSERT INTO users VALUES (2, 'Walter');",
			"INSERT INTO users VALUES (3, null);",
			"SELECT id, name FROM users;",
			"SELECT name FROM users;",
			"SELECT id FROM users;",
			"DROP TABLE users;",
		}

		parser := NewParser()

		for i, query := range validQueries {
			t.Run(fmt.Sprintf("query_%d", i+1), func(t *testing.T) {
				result, err := parser.Parse(query)
				require.NoError(t, err, "Query should be valid: %s", query)
				require.NotNil(t, result)
			})
		}
	})
}
