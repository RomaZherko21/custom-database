package handlers

import (
	"custom-database/internal/operator_execution/executors"

	"github.com/gin-gonic/gin"
)

type HttpHandlers interface {
	HandleSqlQuery(c *gin.Context)
}

type handlers struct {
	backend executors.BackendService
}

func NewHttpHandlers(backend executors.BackendService) HttpHandlers {
	return &handlers{
		backend: backend,
	}
}
