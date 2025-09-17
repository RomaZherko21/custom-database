package ast

import "custom-database/internal/parser/lex"

// Ast представляет абстрактное синтаксическое дерево SQL-запроса
type Ast struct {
	Statements []*AstStatement // Список всех statement'ов в запросе (может быть несколько INSERT например)
}

// AstStatmentKind тип для определения вида AST statement'а
type AstStatmentKind string

const (
	SelectKind      AstStatmentKind = "SELECT"       // SELECT запрос
	CreateTableKind AstStatmentKind = "CREATE_TABLE" // CREATE TABLE запрос
	InsertKind      AstStatmentKind = "INSERT"       // INSERT запрос
	DropTableKind   AstStatmentKind = "DROP_TABLE"   // DROP TABLE запрос
)

// AstStatement представляет один SQL statement
type AstStatement struct {
	Kind AstStatmentKind // Тип statement'а

	SelectStatement      *SelectStatement      // SELECT FROM statement
	CreateTableStatement *CreateTableStatement // CREATE TABLE statement
	InsertStatement      *InsertStatement      // INSERT INTO statement
	DropTableStatement   *DropTableStatement   // DROP TABLE statement
}

// ExpressionKind тип для определения вида выражения
type ExpressionKind string

const (
	LiteralKind ExpressionKind = "LITERAL" // Литеральное значение (строка, число, NULL)
	// Могут быть и другие типы выражений (BINARY, FUNCTION_CALL, AGGREGATE_FUNCTION), но они не используются в текущей реализации
)

// Expression представляет выражение в SQL (колонка, значение и т.д.)
type Expression struct {
	Literal *lex.Token // Литеральное значение (строка, число, NULL)
	Kind    ExpressionKind
}

type CreateTableStatement struct {
	Table   lex.Token            // Имя таблицы
	Columns *[]*columnDefinition // Определения колонок
}

// columnDefinition представляет определение колонки в CREATE TABLE
type columnDefinition struct {
	Name     lex.Token // Имя колонки
	Datatype lex.Token // Тип данных колонки
}

type DropTableStatement struct {
	Table lex.Token // Имя таблицы
}

type InsertStatement struct {
	Table  lex.Token      // Имя таблицы
	Values *[]*Expression // Значения для вставки
}

type SelectStatement struct {
	Table           lex.Token     // Имя таблицы
	SelectedColumns []*Expression // Выбранные колонки
}
