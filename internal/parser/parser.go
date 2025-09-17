package parser

import (
	"custom-database/internal/parser/ast"
	"custom-database/internal/parser/validator"
)

type ParserService interface {
	Parse(query string) (*ast.Ast, error)
}

type parser struct {
}

func NewParser() ParserService {
	return &parser{}
}

func (p *parser) Parse(query string) (*ast.Ast, error) {
	astService := ast.NewAst()
	validator := validator.NewValidator()

	astResult, err := astService.Parse(query)
	if err != nil {
		return nil, err
	}

	err = validator.ValidateAST(astResult)
	if err != nil {
		return nil, err
	}

	return astResult, nil
}
