package parser

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

type StatementWithSQL struct {
	Statement sqlparser.Statement
	SQL       string
}

type ParserService interface {
	Parse(query string) ([]StatementWithSQL, error)
}

type parser struct{}

func NewParser() ParserService {
	return &parser{}
}

func (p *parser) Parse(query string) ([]StatementWithSQL, error) {
	// Разбиваем запрос на отдельные statements по точке с запятой
	statements := []StatementWithSQL{}

	// Убираем лишние пробелы
	query = strings.TrimSpace(query)
	if query == "" {
		return statements, nil
	}

	// Разбиваем по точке с запятой, но учитываем, что точка с запятой может быть внутри строк
	parts := splitStatements(query)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Парсим каждый statement отдельно
		stmt, err := sqlparser.Parse(part)
		if err != nil {
			return nil, fmt.Errorf("failed to parse statement '%s': %v", part, err)
		}

		statements = append(statements, StatementWithSQL{
			Statement: stmt,
			SQL:       part,
		})
	}

	return statements, nil
}

// splitStatements разбивает SQL запрос на отдельные statements по точке с запятой
// Учитывает, что точка с запятой может быть внутри строковых литералов
func splitStatements(query string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(query); i++ {
		char := query[i]

		// Обрабатываем строковые литералы
		if !inString && (char == '\'' || char == '"') {
			inString = true
			stringChar = char
			current.WriteByte(char)
			continue
		}

		if inString {
			current.WriteByte(char)
			if char == stringChar {
				// Проверяем, не экранированная ли это кавычка
				if i+1 < len(query) && query[i+1] == stringChar {
					// Экранированная кавычка
					current.WriteByte(query[i+1])
					i++
					continue
				}
				inString = false
				stringChar = 0
			}
			continue
		}

		// Если встречаем точку с запятой вне строки - это разделитель statements
		if char == ';' {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			continue
		}

		current.WriteByte(char)
	}

	// Добавляем последний statement, если он есть
	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}
