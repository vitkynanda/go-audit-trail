package middleware

import (
	"bytes"

	"github.com/gin-gonic/gin"
)

// CustomResponseWriter wraps gin.ResponseWriter to capture the response body.
type CustomResponseWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

// Write captures the response body.
func (w *CustomResponseWriter) Write(b []byte) (int, error) {
	w.body.Write(b)                  // Write to the buffer
	return w.ResponseWriter.Write(b) // Write to the original ResponseWriter
}
