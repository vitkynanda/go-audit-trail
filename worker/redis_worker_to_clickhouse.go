package worker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"github.com/vitkynanda/go-audit-trail/model"
	"gorm.io/gorm"
)

func StartRedisToClickHouseWorker(ctx context.Context, redisClient *redis.Client, db *gorm.DB, logger *logrus.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Println("Stopping Redis-to-ClickHouse worker...")
			return
		case <-ticker.C:
			processAuditTrail(ctx, redisClient, db, logger)
		}
	}
}

func processAuditTrail(ctx context.Context, redisClient *redis.Client, db *gorm.DB, logger *logrus.Logger) {
	results, err := redisClient.LRange(ctx, "audit_logs", 0, 99).Result()
	if err != nil {
		logger.Printf("Failed to fetch logs from Redis: %v", err)
		return
	}

	if len(results) == 0 {
		return
	}

	var logs []model.AuditTrail
	var failedEntries []string

	for _, result := range results {
		var logEntry model.AuditTrail
		if err := json.Unmarshal([]byte(result), &logEntry); err != nil {
			logger.Printf("Failed to unmarshal log entry: %v", err)
			failedEntries = append(failedEntries, result)
			continue
		}
		logs = append(logs, logEntry)
	}

	if len(logs) > 0 {
		if err := db.WithContext(ctx).Create(&logs).Error; err != nil {
			logger.Printf("Failed to insert logs into ClickHouse: %v", err)
			// Save failed logs back to Redis or another queue
			for _, log := range logs {
				jsonLog, _ := json.Marshal(log)
				redisClient.RPush(ctx, "failed_audit_logs", string(jsonLog))
			}
			return
		}
	}

	// Remove processed logs from Redis
	if err := redisClient.LTrim(ctx, "audit_logs", int64(len(results)), -1).Err(); err != nil {
		logger.Printf("Failed to trim Redis list: %v", err)
	}

	// Handle failed entries
	if len(failedEntries) > 0 {
		for _, entry := range failedEntries {
			redisClient.RPush(ctx, "failed_audit_logs", entry)
		}
		logger.Printf("Moved %d failed entries to 'failed_audit_logs'", len(failedEntries))
	}
}
