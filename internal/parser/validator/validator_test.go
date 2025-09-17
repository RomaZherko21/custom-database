package validator

import (
	"testing"
)

func TestValidationError_Error(t *testing.T) {
	err := &ValidationError{
		Message: "Test error",
	}

	expected := "Validation error: Test error"
	if err.Error() != expected {
		t.Errorf("ValidationError.Error() = %v, want %v", err.Error(), expected)
	}
}

func TestValidator_ValidateAST(t *testing.T) {
	validator := NewValidator()

	tests := []struct {
		name    string
		ast     interface{}
		wantErr bool
	}{
		{
			name:    "Nil AST",
			ast:     nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateAST(nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validator.ValidateAST() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidator_validateIdentifier(t *testing.T) {
	validator := &validator{}

	tests := []struct {
		name    string
		value   string
		context string
		line    int
		wantErr bool
	}{
		{
			name:    "Valid identifier",
			value:   "users",
			context: "table name",
			line:    1,
			wantErr: false,
		},
		{
			name:    "Empty identifier",
			value:   "",
			context: "table name",
			line:    1,
			wantErr: true,
		},
		{
			name:    "Keyword as identifier",
			value:   "SELECT",
			context: "table name",
			line:    1,
			wantErr: true,
		},
		{
			name:    "Identifier with spaces",
			value:   "test table",
			context: "table name",
			line:    1,
			wantErr: true,
		},
		{
			name:    "Identifier with parentheses",
			value:   "test()",
			context: "table name",
			line:    1,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.validateIdentifier(tt.value, tt.context)
			if (err != nil) != tt.wantErr {
				t.Errorf("validator.validateIdentifier() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidator_validateDataType(t *testing.T) {
	validator := &validator{}

	tests := []struct {
		name     string
		dataType string
		line     int
		wantErr  bool
	}{
		{
			name:     "Valid INT type",
			dataType: "INT",
			line:     1,
			wantErr:  false,
		},
		{
			name:     "Valid TEXT type",
			dataType: "TEXT",
			line:     1,
			wantErr:  false,
		},
		{
			name:     "Case insensitive INT",
			dataType: "int",
			line:     1,
			wantErr:  false,
		},
		{
			name:     "Invalid type",
			dataType: "INVALID",
			line:     1,
			wantErr:  true,
		},
		{
			name:     "Empty type",
			dataType: "",
			line:     1,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.validateDataType(tt.dataType)
			if (err != nil) != tt.wantErr {
				t.Errorf("validator.validateDataType() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
