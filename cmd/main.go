package main

import (
	"custom-database/cmd/mode"
	"custom-database/internal/parser"
)

func main() {
	parser := parser.NewParser()

	mode.RunConsoleMode(parser)
}
