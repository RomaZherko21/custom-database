package handlers

import (
	"custom-database/internal/backend"
	"encoding/json"

	"github.com/gin-gonic/gin"
)

type SqlQueryRequest struct {
	Query string `json:"query" binding:"required" example:"SELECT id, name FROM users;"`
}

type SqlQueryResponse struct {
	Success bool   `json:"success" example:"true"`
	Result  string `json:"result,omitempty" example:"Query executed successfully"`
	Error   string `json:"error,omitempty" example:"Invalid SQL syntax"`
}

// HandleSqlQuery обрабатывает HTTP запросы для выполнения SQL запросов
// @Summary Выполнить SQL запрос
// @Description Выполняет SQL запрос и возвращает результат
// @Tags query
// @Accept json
// @Produce json
// @Param request body SqlQueryRequest true "SQL запрос"
// @Success 200 {object} SqlQueryResponse
// @Failure 400 {object} SqlQueryResponse
// @Router /query [post]
func (h *handlers) HandleSqlQuery(c *gin.Context) {
	var request SqlQueryRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(400, SqlQueryResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	query := request.Query

	ast, err := h.parser.Parse(query)
	if err != nil {
		c.JSON(400, SqlQueryResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	result, err := h.mb.ExecuteStatement(ast)
	if err != nil {
		c.JSON(400, SqlQueryResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	if result == nil {
		c.JSON(200, SqlQueryResponse{
			Success: true,
			Result:  "Query executed successfully",
		})
		return
	}

	// Конвертируем результат в JSON строку
	jsonResult, err := convertToJson(result)
	if err != nil {
		c.JSON(400, SqlQueryResponse{
			Success: false,
			Error:   "Error converting result to JSON: " + err.Error(),
		})
		return
	}

	c.JSON(200, SqlQueryResponse{
		Success: true,
		Result:  string(jsonResult),
	})
}

type jsonTable struct {
	Name    string           `json:"name"`
	Columns []backend.Column `json:"columns"`
	Rows    [][]interface{}  `json:"rows"`
}

func convertToJson(table *backend.Table) (string, error) {
	result := jsonTable{
		Name:    table.Name,
		Columns: table.Columns,
		Rows:    [][]interface{}{},
	}

	for _, row := range table.Rows {
		jsonRow := []interface{}{}

		for i, cell := range row {
			typ := table.Columns[i].Type
			var s interface{}
			switch typ {
			case backend.TextType, backend.IntType:
				if cell.IsNull() {
					s = nil
				} else {
					s = cell.AsText()
				}
			}

			jsonRow = append(jsonRow, s)
		}

		result.Rows = append(result.Rows, jsonRow)
	}

	jsonResult, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(jsonResult), nil
}
