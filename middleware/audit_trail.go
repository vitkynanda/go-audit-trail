package middleware

import (
	"bytes"
	"context"
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

const maxRequestBodySize = 10 * 1024 // 10 KB

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// AuditTrail is a middleware that captures HTTP request/response details and stores them in Redis.
func AuditTrail(conn *gorm.DB, logger *logrus.Logger, redisClient *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {

		logger.Println("AuditTrail middleware started")
		start := time.Now()

		// Capture request details
		requestURL := c.Request.URL.String()
		requestMethod := c.Request.Method
		requestQuery := c.Request.URL.RawQuery
		requestHeaders := c.Request.Header
		clientIP := c.ClientIP()
		userAgent := c.Request.UserAgent()

		// Read and restore the request body with size validation
		var requestBody []byte
		if c.Request.Body != nil {
			buf := bufferPool.Get().(*bytes.Buffer)
			buf.Reset()
			defer bufferPool.Put(buf)

			// Limit the size of the request body
			bodyReader := io.LimitReader(c.Request.Body, maxRequestBodySize)
			_, err := buf.ReadFrom(bodyReader)
			if err == nil {
				requestBody = buf.Bytes()
				c.Request.Body = io.NopCloser(bytes.NewBuffer(requestBody))
			} else {
				logger.WithFields(logrus.Fields{
					"request_url":    requestURL,
					"request_method": requestMethod,
					"client_ip":      clientIP,
				}).WithError(err).Warn("Failed to read request body or request body exceeds maximum size")
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
			ResponseCode:   int32(responseCode),
			ResponseStatus: responseStatus,
			UserAgent:      userAgent,
			ClientIP:       clientIP,
			DurationMs:     durationMs,
		}

		// Push log entry to Redis queue asynchronously with retry mechanism
		go func(ctx context.Context) {
			select {
			case <-ctx.Done():
				logger.Warn("Context canceled, stopping Redis push")
				return
			default:
				jsonLog, err := json.Marshal(logEntry)
				if err != nil {
					logger.WithFields(logrus.Fields{
						"request_url":    requestURL,
						"request_method": requestMethod,
						"client_ip":      clientIP,
					}).WithError(err).Error("Failed to marshal log entry")
					return
				}

				retryCount := 3
				for i := 0; i < retryCount; i++ {
					err = redisClient.LPush(ctx, "audit_logs", jsonLog).Err()
					if err == nil {
						break
					}
					logger.WithFields(logrus.Fields{
						"request_url":    requestURL,
						"request_method": requestMethod,
						"client_ip":      clientIP,
					}).WithError(err).Warnf("Retry %d/%d: Failed to push log entry to Redis", i+1, retryCount)
					time.Sleep(time.Duration(i+1) * 200 * time.Millisecond) // Exponential backoff
				}

				if err != nil {
					logger.WithFields(logrus.Fields{
						"request_url":    requestURL,
						"request_method": requestMethod,
						"client_ip":      clientIP,
					}).WithError(err).Error("Failed to push log entry to Redis after retries")
				}
			}
		}(c.Request.Context())
	}
}
