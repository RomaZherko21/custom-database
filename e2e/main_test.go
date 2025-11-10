package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreateAndQueryTable(t *testing.T) {
	t.Run("Create Table", func(t *testing.T) {
		query := "CREATE TABLE test_table (id INT, name TEXT);"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})

	t.Run("Insert Data", func(t *testing.T) {
		queries := []string{
			"INSERT INTO test_table VALUES (1, 'Rick');",
			"INSERT INTO test_table VALUES (2, 'Morty');",
			"INSERT INTO test_table VALUES (3, 'John');",
		}

		for _, query := range queries {
			response := executeQuery(t, query)
			assert.Empty(t, response.Error)
		}
	})

	t.Run("Select Data", func(t *testing.T) {
		query := "SELECT id, name FROM test_table;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		want := `{"name":"test_table","columns":[{"name":"id","type":1},{"name":"name","type":0}],"rows":[["1","Rick"],["2","Morty"],["3","John"]]}`

		assert.Equal(t, want, response.Result)
	})

	t.Run("Drop Table", func(t *testing.T) {
		query := "DROP TABLE test_table;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})
}

func TestCreateAndQueryTableWithWhere(t *testing.T) {
	t.Run("Create Table", func(t *testing.T) {
		query := "CREATE TABLE test_table_with_where (id INT, name TEXT);"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})

	t.Run("Insert Data", func(t *testing.T) {
		queries := []string{
			"INSERT INTO test_table_with_where VALUES (1, 'Rick');",
			"INSERT INTO test_table_with_where VALUES (2, 'Morty');",
			"INSERT INTO test_table_with_where VALUES (3, 'John');",
		}

		for _, query := range queries {
			response := executeQuery(t, query)
			assert.Empty(t, response.Error)
		}
	})

	t.Run("Select Data", func(t *testing.T) {
		query := "SELECT id, name FROM test_table_with_where WHERE id = 2;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		want := `{"name":"test_table_with_where","columns":[{"name":"id","type":1},{"name":"name","type":0}],"rows":[["2","Morty"]]}`

		assert.Equal(t, want, response.Result)
	})

	t.Run("Drop Table", func(t *testing.T) {
		query := "DROP TABLE test_table_with_where;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})
}

func TestCreateAndQueryTableWithIndex(t *testing.T) {
	t.Run("Create Table", func(t *testing.T) {
		query := "CREATE TABLE test_table_with_index (id INT, name TEXT);"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})

	t.Run("Create Index", func(t *testing.T) {
		query := "CREATE INDEX idx_test_table_with_index_id ON test_table_with_index (id);"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})

	t.Run("Insert Data", func(t *testing.T) {
		queries := []string{
			"INSERT INTO test_table_with_index VALUES (1, 'Rick');",
			"INSERT INTO test_table_with_index VALUES (2, 'Morty');",
			"INSERT INTO test_table_with_index VALUES (3, 'John');",
		}

		for _, query := range queries {
			response := executeQuery(t, query)
			assert.Empty(t, response.Error)
		}
	})

	t.Run("Select Data", func(t *testing.T) {
		query := "SELECT id, name FROM test_table_with_index WHERE id = 2;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		want := `{"name":"test_table_with_index","columns":[{"name":"id","type":1},{"name":"name","type":0}],"rows":[["2","Morty"]]}`

		assert.Equal(t, want, response.Result)
	})

	t.Run("Drop Table", func(t *testing.T) {
		query := "DROP TABLE test_table_with_index;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})
}

func TestLoadTestWithHugeDataWithIndex(t *testing.T) {
	const batchCount = 100
	const insertsPerBatch = 100
	const tryToFindRecord = 10000

	const totalRecords = batchCount * insertsPerBatch

	t.Run("Create Table", func(t *testing.T) {
		query := "CREATE TABLE test_table_huge_data (id INT, name TEXT);"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})

	t.Run("Create Index", func(t *testing.T) {
		query := "CREATE INDEX idx_test_table_huge_data_id ON test_table_huge_data (id);"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})

	t.Run("Insert 10000 Records", func(t *testing.T) {
		startTime := time.Now()

		for batch := 0; batch < batchCount; batch++ {
			var queries []string
			startID := batch*insertsPerBatch + 1
			endID := (batch + 1) * insertsPerBatch

			for i := startID; i <= endID; i++ {
				queries = append(queries, fmt.Sprintf("INSERT INTO test_table_huge_data VALUES (%d, 'Name%d')", i, i))
			}

			// Объединяем все INSERT'ы в один SQL запрос
			query := fmt.Sprintf("%s;", queries[0])
			for i := 1; i < len(queries); i++ {
				query += fmt.Sprintf(" %s;", queries[i])
			}

			response := executeQuery(t, query)
			assert.Empty(t, response.Error, "Failed to insert batch %d (records %d-%d)", batch+1, startID, endID)
		}

		insertDuration := time.Since(startTime)
		t.Logf("Время вставки %d записей (%d HTTP запросов по %d INSERT): %v", totalRecords, batchCount, insertsPerBatch, insertDuration)
		t.Logf("Среднее время на один HTTP запрос: %v", insertDuration/batchCount)
		t.Logf("Среднее время на одну запись: %v", insertDuration/totalRecords)
	})

	t.Run("Select Single Record", func(t *testing.T) {
		startTime := time.Now()

		query := fmt.Sprintf("SELECT id, name FROM test_table_huge_data WHERE id = %d;", tryToFindRecord)
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		selectDuration := time.Since(startTime)
		t.Logf("Время поиска одной записи: %v", selectDuration)

		// Проверяем, что найдена правильная запись
		want := fmt.Sprintf(`{"name":"test_table_huge_data","columns":[{"name":"id","type":1},{"name":"name","type":0}],"rows":[["%d","Name%d"]]}`, tryToFindRecord, tryToFindRecord)
		assert.Equal(t, want, response.Result)
	})

	t.Run("Drop Table", func(t *testing.T) {
		query := "DROP TABLE test_table_huge_data;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})
}

func TestLoadTestWithHugeDataWithoutIndex(t *testing.T) {
	const batchCount = 100
	const insertsPerBatch = 100
	const tryToFindRecord = 10000

	const totalRecords = batchCount * insertsPerBatch

	t.Run("Create Table", func(t *testing.T) {
		query := "CREATE TABLE test_table_huge (id INT, name TEXT);"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})

	t.Run("Insert 10000 Records", func(t *testing.T) {
		startTime := time.Now()

		// 100 HTTP запросов по 100 INSERT в каждом
		for batch := 0; batch < batchCount; batch++ {
			var queries []string
			startID := batch*insertsPerBatch + 1
			endID := (batch + 1) * insertsPerBatch

			// Формируем батч из 100 INSERT запросов
			for i := startID; i <= endID; i++ {
				queries = append(queries, fmt.Sprintf("INSERT INTO test_table_huge VALUES (%d, 'Name%d')", i, i))
			}

			// Объединяем все INSERT'ы в один SQL запрос
			query := fmt.Sprintf("%s;", queries[0])
			for i := 1; i < len(queries); i++ {
				query += fmt.Sprintf(" %s;", queries[i])
			}

			response := executeQuery(t, query)
			assert.Empty(t, response.Error, "Failed to insert batch %d (records %d-%d)", batch+1, startID, endID)
		}

		insertDuration := time.Since(startTime)
		t.Logf("Время вставки %d записей (%d HTTP запросов по %d INSERT): %v", totalRecords, batchCount, insertsPerBatch, insertDuration)
		t.Logf("Среднее время на один HTTP запрос: %v", insertDuration/batchCount)
		t.Logf("Среднее время на одну запись: %v", insertDuration/totalRecords)
	})

	t.Run("Select Single Record", func(t *testing.T) {
		startTime := time.Now()

		query := fmt.Sprintf("SELECT id, name FROM test_table_huge WHERE id = %d;", tryToFindRecord)
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)

		selectDuration := time.Since(startTime)
		t.Logf("Время поиска одной записи: %v", selectDuration)

		// Проверяем, что найдена правильная запись
		want := fmt.Sprintf(`{"name":"test_table_huge","columns":[{"name":"id","type":1},{"name":"name","type":0}],"rows":[["%d","Name%d"]]}`, tryToFindRecord, tryToFindRecord)
		assert.Equal(t, want, response.Result)
	})

	t.Run("Drop Table", func(t *testing.T) {
		query := "DROP TABLE test_table_huge;"
		response := executeQuery(t, query)
		assert.Empty(t, response.Error)
	})
}
