package processor

import (
	"context"
	"time"

	"github.com/DoNewsCode/core/logging"
	"github.com/segmentio/kafka-go"
)

// Handler only include options and Handle func.
type Handler interface {
	// Name get the reader from otkafka.ReaderMaker, also get options.
	Name() string
	// Handle for *kafka.Message.
	Handle(ctx context.Context, msg *kafka.Message) (interface{}, error)
}

// BatchHandler one more Batch method than Handler.
type BatchHandler interface {
	Handler
	// Batch processing the results returned by Handler.Handle.
	Batch(ctx context.Context, data []interface{}) error
}

// HandleFunc type for Handler.Handle Func.
type HandleFunc func(ctx context.Context, msg *kafka.Message) (interface{}, error)

// BatchFunc type for BatchHandler.Batch Func.
type BatchFunc func(ctx context.Context, data []interface{}) error

type wrapHandler struct {
	name string
	hf   HandleFunc
	bf   BatchFunc
}

func (h *wrapHandler) Name() string {
	return h.name
}

func (h *wrapHandler) Handle(ctx context.Context, msg *kafka.Message) (interface{}, error) {
	return h.hf(ctx, msg)
}

func (h *wrapHandler) Batch(ctx context.Context, data []interface{}) error {
	return h.bf(ctx, data)
}

// batchInfo data is the result of message processed by Handler.Handle.
//
// When BatchHandler.Batch is successfully called, then commit the message.
type batchInfo struct {
	message *kafka.Message
	data    interface{}
}

// handler private processor
type handler struct {
	reader *kafka.Reader

	handleFunc HandleFunc
	batchFunc  BatchFunc
	batchCh    chan *batchInfo

	options *options

	logger logging.LevelLogger
}

// read fetch message from kafka
func (h *handler) handle(ctx context.Context) error {
	for {
		select {
		default:
			message, err := h.reader.FetchMessage(ctx)
			if err != nil {
				return err
			}
			v, err := h.handleFunc(ctx, &message)
			if err != nil {
				if IsFatalErr(err) {
					return err
				}
				h.logger.Warn(err)
			}
			if v == nil {
				continue
			}
			vv := &batchInfo{data: v}
			commitMsg := kafka.Message{
				Topic:     message.Topic,
				Partition: message.Partition,
				Offset:    message.Offset,
			}
			if !h.options.AutoCommit {
				vv.message = &commitMsg
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case h.batchCh <- vv:
				if h.options.AutoCommit {
					err := h.reader.CommitMessages(ctx, commitMsg)
					if err != nil {
						h.logger.Warn(err)
					}
				}

			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// batch Call BatchHandler.Batch and commit *kafka.Message.
func (h *handler) batch(ctx context.Context) error {
	var (
		ticker   = time.NewTicker(h.options.AutoBatchInterval)
		messages = make([]kafka.Message, 0, h.options.BatchSize)
		data     = make([]interface{}, 0, h.options.BatchSize)
	)
	defer ticker.Stop()

	doFunc := func() error {
		if len(data) == 0 {
			return nil
		}
		if err := h.batchFunc(ctx, data); err != nil {
			if IsFatalErr(err) {
				return err
			}
			h.logger.Warn(err)
		}
		if len(messages) > 0 {
			if err := h.reader.CommitMessages(context.Background(), messages...); err != nil {
				return err
			}
		}
		data = data[:0]
		messages = messages[:0]
		return nil
	}

	for {
		select {
		case v := <-h.batchCh:
			if v.message != nil {
				messages = append(messages, *v.message)
			}
			data = append(data, v.data)
			if len(data) < h.options.BatchSize {
				continue
			}
			if err := doFunc(); err != nil {
				return err
			}
		case <-ticker.C:
			if err := doFunc(); err != nil {
				return err
			}
		case <-ctx.Done():
			if h.options.AutoCommit {
				if err := doFunc(); err != nil {
					return err
				}
			}
			return ctx.Err()
		}
	}
}
