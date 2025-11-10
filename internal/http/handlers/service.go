package handlers

import (
	"custom-database/internal/backend"
	"custom-database/internal/parser"

	"github.com/gin-gonic/gin"
)

type HttpHandlers interface {
	HandleSqlQuery(c *gin.Context)
}

type handlers struct {
	parser parser.ParserService
	mb     backend.MemoryBackendService
}

func NewHttpHandlers(parser parser.ParserService, mb backend.MemoryBackendService) HttpHandlers {
	return &handlers{
		parser: parser,
		mb:     mb,
	}
}
