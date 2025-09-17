package ast

import (
	"custom-database/internal/parser/lex"
	"errors"
)

type AstService interface {
	Parse(query string) (*Ast, error)
}

type ast struct {
}

func NewAst() AstService {
	return &ast{}
}

// Parse выполняет парсинг SQL-запроса в абстрактное синтаксическое дерево
// Пример: INSERT INTO users VALUES (1, 'Joffrey');INSERT INTO users VALUES (2, 'Walter');
func (a *ast) Parse(query string) (*Ast, error) {
	// Сначала токенизируем запрос
	lexer := lex.NewLexer()
	tokens, err := lexer.Lex(query)
	if err != nil {
		return nil, err
	}

	astResult := Ast{}
	// Указатель на текущий токен
	pointer := uint(0)

	// Парсим все statement'ы в запросе
	for pointer < uint(len(tokens)) {
		// Парсим один statement
		statement, newCursor, ok := parseStatement(tokens, pointer)
		if !ok {
			helpMessage(tokens, pointer, "Expected statement")
			return nil, errors.New("failed to parse, expected statement")
		}
		pointer = newCursor

		// Добавляем statement в результат
		astResult.Statements = append(astResult.Statements, statement)

		// Пропускаем все точки с запятой после statement
		semicolonFound := false
		for expectToken(tokens, pointer, tokenFromSymbol(lex.SemicolonSymbol)) {
			pointer++
			semicolonFound = true
		}

		// Требуем хотя бы одну точку с запятой между statement'ами
		if !semicolonFound {
			helpMessage(tokens, pointer, "Expected semi-colon delimiter between statements")
			return nil, errors.New("missing semi-colon between statements")
		}
	}

	return &astResult, nil
}

// parseStatement парсит один SQL statement из списка токенов
func parseStatement(tokens []*lex.Token, initialPointer uint) (*AstStatement, uint, bool) {
	pointer := initialPointer

	// Пробуем парсить SELECT statement
	if selectStmt, newCursor, ok := parseSelectStatement(tokens, pointer); ok {
		return &AstStatement{
			Kind:            SelectKind,
			SelectStatement: selectStmt,
		}, newCursor, true
	}

	// Пробуем парсить INSERT statement
	if insertStmt, newCursor, ok := parseInsertStatement(tokens, pointer); ok {
		return &AstStatement{
			Kind:            InsertKind,
			InsertStatement: insertStmt,
		}, newCursor, true
	}

	// Пробуем парсить CREATE TABLE statement
	if createStmt, newCursor, ok := parseCreateTableStatement(tokens, pointer); ok {
		return &AstStatement{
			Kind:                 CreateTableKind,
			CreateTableStatement: createStmt,
		}, newCursor, true
	}

	// Пробуем парсить DROP TABLE statement
	if dropStmt, newCursor, ok := parseDropTableStatement(tokens, pointer); ok {
		return &AstStatement{
			Kind:               DropTableKind,
			DropTableStatement: dropStmt,
		}, newCursor, true
	}

	return nil, initialPointer, false
}
