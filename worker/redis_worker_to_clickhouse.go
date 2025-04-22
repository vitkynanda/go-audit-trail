package worker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/vitkynanda/go-audit-trail/model"
	"gorm.io/gorm"
)

// StartRedisToClickHouseWorker processes audit logs from Redis and saves them to ClickHouse
func StartRedisToClickHouseWorker(ctx context.Context, redisClient *redis.Client, db *gorm.DB, logger *logrus.Logger) {
	// Verify Redis connection
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		logger.WithError(err).Fatal("Redis connection failed")
		return
	}

	// Verify ClickHouse connection
	sqlDB, err := db.DB()
	if err != nil {
		logger.WithError(err).Fatal("Failed to get SQL DB instance")
		return
	}

	if err := sqlDB.PingContext(ctx); err != nil {
		logger.WithError(err).Fatal("ClickHouse connection failed")
		return
	}

	logger.Info("Redis-to-ClickHouse worker started successfully")

	// Start heartbeat routine to verify worker is running
	go func() {
		heartbeatTicker := time.NewTicker(1 * time.Minute)
		defer heartbeatTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-heartbeatTicker.C:
				logger.Info("Redis-to-ClickHouse worker is running")
				// Check queue length periodically
				count, err := redisClient.LLen(ctx, "audit_logs").Result()
				if err != nil {
					logger.WithError(err).Warn("Failed to get audit_logs queue length")
				} else {
					logger.Infof("Current audit_logs queue length: %d", count)
				}
			}
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping Redis-to-ClickHouse worker...")
			return
		case <-ticker.C:
			processAuditTrail(ctx, redisClient, db, logger)
		}
	}
}

// processAuditTrail fetches audit logs from Redis and saves them to ClickHouse
func processAuditTrail(ctx context.Context, redisClient *redis.Client, db *gorm.DB, logger *logrus.Logger) {
	// Add transaction timeout
	processingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	logger.Debug("Starting audit trail processing...")

	// Verify ClickHouse connection is still alive
	sqlDB, err := db.DB()
	if err != nil {
		logger.WithError(err).Error("Failed to get SQL DB instance")
		return
	}

	if err := sqlDB.PingContext(processingCtx); err != nil {
		logger.WithError(err).Error("ClickHouse connection failed during processing")
		return
	}

	// Get current queue size
	queueSize, err := redisClient.LLen(processingCtx, "audit_logs").Result()
	if err != nil {
		logger.WithError(err).Error("Failed to get queue size")
		return
	}

	if queueSize == 0 {
		logger.Debug("No audit logs to process")
		return
	}

	// Determine batch size (max 100)
	batchSize := int64(100)
	if queueSize < batchSize {
		batchSize = queueSize
	}

	logger.Infof("Processing %d/%d audit logs", batchSize, queueSize)

	// Retrieve logs from Redis
	results, err := redisClient.LRange(processingCtx, "audit_logs", 0, batchSize-1).Result()
	if err != nil {
		logger.WithError(err).Error("Failed to fetch logs from Redis")
		return
	}

	if len(results) == 0 {
		logger.Debug("No audit logs retrieved")
		return
	}

	var logs []model.AuditTrail
	var failedEntries []string
	var processedCount int

	for _, result := range results {
		var logEntry model.AuditTrail
		if err := json.Unmarshal([]byte(result), &logEntry); err != nil {
			logger.WithError(err).Error("Failed to unmarshal log entry")
			logger.Debugf("Invalid JSON: %s", result)
			failedEntries = append(failedEntries, result)
			continue
		}

		// Validate required fields
		if logEntry.ID == uuid.Nil || logEntry.Timestamp.IsZero() {
			logger.Error("Missing required fields in log entry")
			failedEntries = append(failedEntries, result)
			continue
		}

		logs = append(logs, logEntry)
		processedCount++
	}

	if len(logs) > 0 {
		logger.Infof("Inserting %d logs into ClickHouse", len(logs))

		// Use raw query for batch insert
		values := []interface{}{}
		query := `
            INSERT INTO capacity_planning_audit.audit_trails (
                id, timestamp, request_url, request_method, request_params, request_query,
                request_headers, request_body, response_code, response_status, user_agent,
                client_ip, duration_ms, user_id, created_at
            ) VALUES `

		for i, log := range logs {
			if i > 0 {
				query += ","
			}
			query += "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			values = append(values, log.ID, log.Timestamp, log.RequestURL, log.RequestMethod,
				log.RequestParams, log.RequestQuery, log.RequestHeaders, log.RequestBody,
				log.ResponseCode, log.ResponseStatus, log.UserAgent, log.ClientIP,
				log.DurationMs, log.UserID, log.CreatedAt)
		}

		// Execute raw query
		if err := db.Exec(query, values...).Error; err != nil {
			logger.WithError(err).Error("Failed to insert logs into ClickHouse")

			// Move failed logs to separate queue
			for _, log := range logs {
				jsonLog, _ := json.Marshal(log)
				if err := redisClient.RPush(processingCtx, "failed_audit_logs", string(jsonLog)).Err(); err != nil {
					logger.WithError(err).Error("Failed to push to failed_audit_logs queue")
				}
			}
			return
		}

		logger.Infof("Successfully inserted %d logs into ClickHouse", len(logs))

		// Remove processed logs from Redis in batches to avoid timeout
		if processedCount > 0 {
			pipe := redisClient.Pipeline()
			pipe.LTrim(processingCtx, "audit_logs", int64(processedCount), -1)
			_, err = pipe.Exec(processingCtx)
			if err != nil {
				logger.WithError(err).Error("Failed to trim Redis list")
				return
			}

			logger.Infof("Successfully removed %d processed logs from Redis", processedCount)
		}
	}

	// Handle failed entries
	if len(failedEntries) > 0 {
		for _, entry := range failedEntries {
			if err := redisClient.RPush(processingCtx, "failed_audit_logs", entry).Err(); err != nil {
				logger.WithError(err).Error("Failed to push to failed_audit_logs queue")
			}
		}
		logger.Infof("Moved %d failed entries to 'failed_audit_logs'", len(failedEntries))
	}
}
