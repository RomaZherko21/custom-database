package lex

// TokenKind тип для определения вида токена
type TokenKind uint

const (
	KeywordToken         TokenKind = iota // Ключевые слова: CREATE, SELECT, TABLE и т.д.
	SymbolToken                           // Символы: (, ), ,, ;, *
	IdentifierToken                       // Идентификаторы: имена таблиц, колонок
	StringToken                           // Строковые литералы: 'text'
	NumericToken                          // Числовые литералы: 123, 45.67, 1e5
	NullToken                             // NULL значение
	MathOperatorToken                     // Математические операторы: =, <, >, !=
	LogicalOperatorToken                  // Логические операторы: AND, OR, NOT
)

// Token представляет один токен в SQL-запросе
type Token struct {
	Value string    // Значение токена
	Kind  TokenKind // Тип токена
}

// Equals сравнивает два токена на равенство (t==other)
func (t *Token) Equals(other *Token) bool {
	return t.Value == other.Value && t.Kind == other.Kind
}
