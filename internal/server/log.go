package server

import (
	"fmt"
	"sync"
)

// ErrOffsetNotFound - not found error
var ErrOffsetNotFound = fmt.Errorf("offset not found")

// Log ...
type Log struct {
	mu      sync.Mutex
	records []Record
}

// NewLog creats new Log struct
func NewLog() *Log {
	return &Log{}
}

// Append - appends record to log
func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

// Read - reads record from Log
func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}

	return c.records[offset], nil
}

// Record ...
type Record struct {
	Value  []byte `json:"value,omitempty"`
	Offset uint64 `json:"offset,omitempty"`
}
