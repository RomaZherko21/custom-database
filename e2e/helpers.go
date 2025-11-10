package e2e

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	serverURL = "http://localhost:8080"
)

type QueryRequest struct {
	Query string `json:"query"`
}

type QueryResponse struct {
	Result interface{} `json:"result"`
	Error  string      `json:"error,omitempty"`
}

func executeQuery(t *testing.T, query string) *QueryResponse {
	reqBody := QueryRequest{Query: query}
	jsonBody, err := json.Marshal(reqBody)
	require.NoError(t, err)

	resp, err := http.Post(serverURL+"/query", "application/json", bytes.NewBuffer(jsonBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	var response QueryResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	require.NoError(t, err)

	return &response
}
