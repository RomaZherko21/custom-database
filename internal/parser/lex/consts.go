package lex

// Keyword тип для ключевых слов SQL
type Keyword string

const (
	// Основные операторы SQL
	CreateKeyword Keyword = "create" // CREATE TABLE
	DropKeyword   Keyword = "drop"   // DROP TABLE
	SelectKeyword Keyword = "select" // SELECT
	InsertKeyword Keyword = "insert" // INSERT INTO

	// Вспомогательные ключевые слова
	FromKeyword   Keyword = "from"   // FROM table
	TableKeyword  Keyword = "table"  // CREATE TABLE
	IntoKeyword   Keyword = "into"   // INSERT INTO
	ValuesKeyword Keyword = "values" // VALUES (...)

	// Типы данных
	IntKeyword  Keyword = "int"  // INTEGER
	TextKeyword Keyword = "text" // TEXT
)

// Keywords список всех ключевых слов для парсинга
var Keywords = []Keyword{
	// Основные операторы
	SelectKeyword,
	InsertKeyword,
	CreateKeyword,
	DropKeyword,
	// Вспомогательные ключевые слова
	ValuesKeyword,
	TableKeyword,
	FromKeyword,
	IntoKeyword,
	// Типы данных
	IntKeyword,
	TextKeyword,
}

// Symbol тип для символов SQL
type Symbol string

const (
	SemicolonSymbol  Symbol = ";" // Конец запроса
	AsteriskSymbol   Symbol = "*" // SELECT *
	CommaSymbol      Symbol = "," // Разделитель списков
	LeftparenSymbol  Symbol = "("
	RightparenSymbol Symbol = ")"
)

// symbols список всех символов для парсинга
var symbols = []Symbol{
	CommaSymbol,
	LeftparenSymbol,
	RightparenSymbol,
	SemicolonSymbol,
	AsteriskSymbol,
}

// NullKeyword тип для ключевого слова NULL
type NullKeyword string

const (
	NullValueKeyword NullKeyword = "null"
)

// MathOperator тип для математических операторов
type MathOperator string

const (
	EqualOperator       MathOperator = "="
	NotEqualOperator    MathOperator = "!="
	GreaterThanOperator MathOperator = ">"
	LessThanOperator    MathOperator = "<"
)

// mathOperators список всех математических операторов для парсинга
var mathOperators = []MathOperator{
	EqualOperator,
	NotEqualOperator,
	GreaterThanOperator,
	LessThanOperator,
}
