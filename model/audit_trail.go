package model

import (
	"time"
)

type AuditTrail struct {
	ID             string    `gorm:"primaryKey;type:UUID;column:id;default:generateUUIDv4()" json:"id"` // Use generateUUIDv4() for UUID
	Timestamp      time.Time `gorm:"type:DateTime;column:timestamp;default:now()" json:"timestamp"`
	RequestURL     string    `gorm:"type:String;column:request_url" json:"request_url"`
	RequestMethod  string    `gorm:"type:LowCardinality(String);column:request_method" json:"request_method"` // Low cardinality for better compression
	RequestParams  string    `gorm:"type:String;column:request_params" json:"request_params"`                 // JSON string
	RequestQuery   string    `gorm:"type:String;column:request_query" json:"request_query"`                   // Raw query string
	RequestHeaders string    `gorm:"type:String;column:request_headers" json:"request_headers"`               // JSON string
	RequestBody    string    `gorm:"type:String;column:request_body" json:"request_body"`                     // Raw request body
	ResponseCode   int32     `gorm:"type:Int32;column:response_code" json:"response_code"`                    // Use Int32 for better compatibility
	ResponseStatus string    `gorm:"type:LowCardinality(String);column:response_status" json:"response_status"`
	UserAgent      string    `gorm:"type:String;column:user_agent" json:"user_agent"`
	ClientIP       string    `gorm:"type:String;column:client_ip" json:"client_ip"`
	DurationMs     int64     `gorm:"type:Int64;column:duration_ms" json:"duration_ms"`
	UserID         string    `gorm:"type:String;column:user_id" json:"user_id"` // Optional: if you have user authentication
	CreatedAt      time.Time `gorm:"type:DateTime;column:created_at;default:now()" json:"created_at"`
}
