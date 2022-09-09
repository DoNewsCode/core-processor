package processor

import (
	"time"
)

// options the options of BatchHandler.
//
// Note:
//		If sequence is necessary, make sure that per worker count is one.
//		Multiple goroutines cannot guarantee the order in which data is processed.
type options struct {
	// used to get reader from otkafka.ReaderMaker.
	// default: "default"
	Name string
	// batch workers count.
	// default: 1
	BatchWorker int
	// data size for batch processing.
	// default: 1
	BatchSize int
	// handler workers count.
	HandleWorker int
	// run the batchFunc automatically at specified intervals, avoid not executing without reaching BatchSize
	// default: 30s
	AutoBatchInterval time.Duration
	// auto commit message after read message and ignore error
	AutoCommit bool
}
