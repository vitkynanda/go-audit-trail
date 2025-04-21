package middleware

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"github.com/vitkynanda/go-audit-trail/model"
	"gorm.io/gorm"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func AuditTrail(conn *gorm.DB, logger *logrus.Logger, redisClient *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		// Capture request details
		requestURL := c.Request.URL.String()
		requestMethod := c.Request.Method
		requestQuery := c.Request.URL.RawQuery
		requestHeaders := c.Request.Header
		clientIP := c.ClientIP()
		userAgent := c.Request.UserAgent()

		// Read and restore the request body
		var requestBody []byte
		if c.Request.Body != nil {
			buf := bufferPool.Get().(*bytes.Buffer)
			buf.Reset()
			defer bufferPool.Put(buf)

			_, err := buf.ReadFrom(c.Request.Body)
			if err == nil {
				requestBody = buf.Bytes()
				c.Request.Body = io.NopCloser(bytes.NewBuffer(requestBody))
			} else {
				logger.WithError(err).Warn("Failed to read request body")
			}
		}

		// Wrap the response writer to capture the response body
		bodyBuffer := bufferPool.Get().(*bytes.Buffer)
		bodyBuffer.Reset()
		defer bufferPool.Put(bodyBuffer)

		customWriter := &CustomResponseWriter{
			ResponseWriter: c.Writer,
			body:           bodyBuffer,
		}
		c.Writer = customWriter

		// Process the request
		c.Next()

		// Capture response details
		responseCode := c.Writer.Status()
		responseStatus := http.StatusText(responseCode)

		// Calculate request duration
		durationMs := int64(time.Since(start) / time.Millisecond)

		// Convert request headers and params to JSON
		headerJSON, _ := json.Marshal(requestHeaders)
		paramsMap := make(map[string]string)
		for _, param := range c.Params {
			paramsMap[param.Key] = param.Value
		}
		paramsJSON, _ := json.Marshal(paramsMap)

		// Create log entry
		logEntry := model.AuditTrail{
			Timestamp:      time.Now(),
			RequestURL:     requestURL,
			RequestMethod:  requestMethod,
			RequestParams:  string(paramsJSON),
			RequestQuery:   requestQuery,
			RequestHeaders: string(headerJSON),
			RequestBody:    string(requestBody),
			ResponseCode:   responseCode,
			ResponseStatus: responseStatus,
			UserAgent:      userAgent,
			ClientIP:       clientIP,
			DurationMs:     durationMs,
		}

		// Push log entry to Redis queue asynchronously with retry mechanism
		go func() {
			jsonLog, err := json.Marshal(logEntry)
			if err != nil {
				logger.WithError(err).Error("Failed to marshal log entry")
				return
			}

			retryCount := 3
			for i := 0; i < retryCount; i++ {
				err = redisClient.LPush(c, "audit_logs", jsonLog).Err()
				if err == nil {
					break
				}
				logger.WithError(err).Warnf("Retry %d/%d: Failed to push log entry to Redis", i+1, retryCount)
				time.Sleep(100 * time.Millisecond)
			}

			if err != nil {
				logger.WithError(err).Error("Failed to push log entry to Redis after retries")
			}
		}()
	}
}
