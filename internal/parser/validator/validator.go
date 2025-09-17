package validator

import (
	"custom-database/internal/parser/ast"
	"fmt"
	"strings"
)

// ValidationError представляет ошибку валидации
type ValidationError struct {
	Message string // Сообщение об ошибке
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("Validation error: %s", e.Message)
}

type ValidatorService interface {
	ValidateAST(ast *ast.Ast) error
}

type validator struct {
}

// NewValidator создает новый экземпляр валидатора
func NewValidator() ValidatorService {
	return &validator{}
}

// ValidateAST проверяет AST дерево на семантические ошибки
func (v *validator) ValidateAST(ast *ast.Ast) error {
	if ast == nil {
		return &ValidationError{
			Message: "AST is nil",
		}
	}

	if len(ast.Statements) == 0 {
		return &ValidationError{
			Message: "No statements found",
		}
	}

	// Валидация каждого оператора
	for _, statement := range ast.Statements {
		if err := v.validateStatement(statement); err != nil {
			return err
		}
	}

	return nil
}

// validateStatement проверяет отдельный оператор
func (v *validator) validateStatement(statement *ast.AstStatement) error {
	if statement == nil {
		return &ValidationError{
			Message: "Statement is nil",
		}
	}

	switch statement.Kind {
	case ast.SelectKind:
		return v.validateSelectStatement(statement.SelectStatement)
	case ast.InsertKind:
		return v.validateInsertStatement(statement.InsertStatement)
	case ast.CreateTableKind:
		return v.validateCreateTableStatement(statement.CreateTableStatement)
	case ast.DropTableKind:
		return v.validateDropTableStatement(statement.DropTableStatement)
	default:
		return &ValidationError{
			Message: fmt.Sprintf("Unknown statement type: %s", statement.Kind),
		}
	}
}

// validateSelectStatement проверяет SELECT оператор
func (v *validator) validateSelectStatement(stmt *ast.SelectStatement) error {
	if stmt == nil {
		return &ValidationError{
			Message: "SELECT statement is nil",
		}
	}

	// Проверка наличия таблицы в FROM
	if stmt.Table.Value == "" {
		return &ValidationError{
			Message: "SELECT statement must have FROM clause",
		}
	}

	// Проверка имени таблицы
	if err := v.validateIdentifier(stmt.Table.Value, "table name"); err != nil {
		return err
	}

	// Проверка выбранных колонок
	if len(stmt.SelectedColumns) == 0 {
		return &ValidationError{
			Message: "SELECT statement must specify columns or use *",
		}
	}

	// Валидация каждой колонки
	for i, col := range stmt.SelectedColumns {
		if col == nil || col.Literal == nil {
			return &ValidationError{
				Message: fmt.Sprintf("Column %d is invalid", i+1),
			}
		}
		if err := v.validateIdentifier(col.Literal.Value, "column name"); err != nil {
			return err
		}
	}

	return nil
}

// validateInsertStatement проверяет INSERT оператор
func (v *validator) validateInsertStatement(stmt *ast.InsertStatement) error {
	if stmt == nil {
		return &ValidationError{
			Message: "INSERT statement is nil",
		}
	}

	// Проверка имени таблицы
	if stmt.Table.Value == "" {
		return &ValidationError{
			Message: "INSERT statement must specify table name",
		}
	}

	if err := v.validateIdentifier(stmt.Table.Value, "table name"); err != nil {
		return err
	}

	// Проверка значений
	if stmt.Values == nil || len(*stmt.Values) == 0 {
		return &ValidationError{
			Message: "INSERT statement must specify values",
		}
	}

	// Валидация каждого значения
	for i, value := range *stmt.Values {
		if value == nil || value.Literal == nil {
			return &ValidationError{
				Message: fmt.Sprintf("Value %d is invalid", i+1),
			}
		}
	}

	return nil
}

// validateCreateTableStatement проверяет CREATE TABLE оператор
func (v *validator) validateCreateTableStatement(stmt *ast.CreateTableStatement) error {
	if stmt == nil {
		return &ValidationError{
			Message: "CREATE TABLE statement is nil",
		}
	}

	// Проверка имени таблицы
	if stmt.Table.Value == "" {
		return &ValidationError{
			Message: "CREATE TABLE statement must specify table name",
		}
	}

	if err := v.validateIdentifier(stmt.Table.Value, "table name"); err != nil {
		return err
	}

	// Проверка колонок
	if stmt.Columns == nil || len(*stmt.Columns) == 0 {
		return &ValidationError{
			Message: "CREATE TABLE statement must specify at least one column",
		}
	}

	// Валидация каждой колонки
	columnNames := make(map[string]bool)
	for i, col := range *stmt.Columns {
		if col == nil {
			return &ValidationError{
				Message: fmt.Sprintf("Column definition %d is invalid", i+1),
			}
		}

		// Проверка имени колонки
		if col.Name.Value == "" {
			return &ValidationError{
				Message: fmt.Sprintf("Column %d must have a name", i+1),
			}
		}

		if err := v.validateIdentifier(col.Name.Value, "column name"); err != nil {
			return err
		}

		// Проверка на дублирование имен колонок
		if columnNames[col.Name.Value] {
			return &ValidationError{
				Message: fmt.Sprintf("Duplicate column name: %s", col.Name.Value),
			}
		}
		columnNames[col.Name.Value] = true

		// Проверка типа данных
		if col.Datatype.Value == "" {
			return &ValidationError{
				Message: fmt.Sprintf("Column %s must have a data type", col.Name.Value),
			}
		}

		if err := v.validateDataType(col.Datatype.Value); err != nil {
			return err
		}
	}

	return nil
}

// validateDropTableStatement проверяет DROP TABLE оператор
func (v *validator) validateDropTableStatement(stmt *ast.DropTableStatement) error {
	if stmt == nil {
		return &ValidationError{
			Message: "DROP TABLE statement is nil",
		}
	}

	// Проверка имени таблицы
	if stmt.Table.Value == "" {
		return &ValidationError{
			Message: "DROP TABLE statement must specify table name",
		}
	}

	return v.validateIdentifier(stmt.Table.Value, "table name")
}

// validateIdentifier проверяет корректность идентификатора
func (v *validator) validateIdentifier(value, context string) error {
	if value == "" {
		return &ValidationError{
			Message: fmt.Sprintf("%s cannot be empty", context),
		}
	}

	// Проверка на ключевые слова
	keywords := []string{"SELECT", "FROM", "INSERT", "INTO", "VALUES", "CREATE", "TABLE", "DROP", "INT", "TEXT", "NULL"}
	for _, keyword := range keywords {
		if strings.ToUpper(value) == keyword {
			return &ValidationError{
				Message: fmt.Sprintf("%s cannot be a keyword: %s", context, value),
			}
		}
	}

	// Проверка на специальные символы
	if strings.ContainsAny(value, " \t\n\r()[]{},;") {
		return &ValidationError{
			Message: fmt.Sprintf("%s contains invalid characters: %s", context, value),
		}
	}

	return nil
}

// validateDataType проверяет корректность типа данных
func (v *validator) validateDataType(dataType string) error {
	validTypes := []string{"INT", "TEXT"}

	for _, validType := range validTypes {
		if strings.ToUpper(dataType) == validType {
			return nil
		}
	}

	return &ValidationError{
		Message: fmt.Sprintf("Invalid data type: %s. Valid types: %s", dataType, strings.Join(validTypes, ", ")),
	}
}
