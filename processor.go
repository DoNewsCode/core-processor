package processor

import (
	"context"
	"sync"
	"time"

	"github.com/DoNewsCode/core/di"
	"github.com/DoNewsCode/core/logging"
	"github.com/DoNewsCode/core/otkafka"
	"github.com/go-kit/log"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

// Processor dispatch Handler.
type Processor struct {
	maker    otkafka.ReaderMaker
	handlers []*handler
	logger   logging.LevelLogger
}

// Handler only include Info and Handle func.
type Handler interface {
	// Info set the topic name and some config.
	Info() *Info
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

type in struct {
	di.In

	Handlers []Handler `group:"ProcessorHandler"`
	Maker    otkafka.ReaderMaker
	Logger   log.Logger
}

// New create *Processor Module.
func New(i in) (*Processor, error) {
	e := &Processor{
		maker:    i.Maker,
		logger:   logging.WithLevel(i.Logger),
		handlers: []*handler{},
	}
	if len(i.Handlers) == 0 {
		return nil, errors.New("empty handler list")
	}
	for _, hh := range i.Handlers {
		if err := e.addHandler(hh); err != nil {
			return nil, err
		}
	}
	return e, nil
}

// Out to provide Handler to in.Handlers.
type Out struct {
	di.Out

	Handlers []Handler `group:"ProcessorHandler,flatten"`
}

// NewOut create Out to provide Handler to in.Handlers.
// 	Usage:
// 		func newHandlerA(logger log.Logger) processor.Out {
//			return processor.NewOut(
//				&HandlerA{logger: logger},
//			)
//		}
// 	Or
// 		func newHandlers(logger log.Logger) processor.Out {
//			return processor.NewOut(
//				&HandlerA{logger: logger},
//				&HandlerB{logger: logger},
//			)
//		}
func NewOut(handlers ...Handler) Out {
	return Out{Handlers: handlers}
}

// addHandler create handler and add to Processor.handlers
func (e *Processor) addHandler(h Handler) error {
	name := h.Info().name()
	reader, err := e.maker.Make(name)
	if err != nil {
		return err
	}

	if reader.Config().GroupID == "" {
		return errors.New("kafka reader must set consume group")
	}

	closeMsgOnce := sync.Once{}
	var hd = &handler{
		msgCh:      make(chan *kafka.Message, h.Info().chanSize()),
		reader:     reader,
		handleFunc: h.Handle,
		info:       h.Info(),
		logger:     e.logger,
		closeBatchCh: func() {

		},
	}
	hd.closeMsgCh = func() {
		closeMsgOnce.Do(func() {
			close(hd.msgCh)
		})
	}

	batchHandler, isBatchHandler := h.(BatchHandler)
	if isBatchHandler {
		hd.hasBatch = true
		hd.batchFunc = batchHandler.Batch
		hd.batchCh = make(chan *batchInfo, h.Info().chanSize())
		hd.ticker = time.NewTicker(h.Info().autoBatchInterval())
		closeBatchOnce := sync.Once{}
		hd.closeBatchCh = func() {
			closeBatchOnce.Do(func() {
				close(hd.batchCh)
			})
		}
	}

	e.handlers = append(e.handlers, hd)

	return nil
}

// batchInfo data is the result of message processed by Handler.Handle.
//
// When BatchHandler.Batch is successfully called, then commit the message.
type batchInfo struct {
	message *kafka.Message
	data    interface{}
}

// ProvideRunGroup run workers:
// 	1. Fetch message from *kafka.Reader.
// 	2. Handle message by Handler.Handle.
// 	3. Batch data by BatchHandler.Batch. If batch success then commit message.
func (e *Processor) ProvideRunGroup(group *run.Group) {
	if len(e.handlers) == 0 {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	errorGroup, ctx := errgroup.WithContext(ctx)

	for _, one := range e.handlers {
		handler := one
		for i := 0; i < handler.info.readWorker(); i++ {
			errorGroup.Go(func() error {
				return handler.read(ctx)
			})
		}

		for i := 0; i < handler.info.handleWorker(); i++ {
			errorGroup.Go(func() error {
				return handler.handle(ctx)
			})
		}

		if handler.batchFunc != nil {
			for i := 0; i < handler.info.batchWorker(); i++ {
				errorGroup.Go(func() error {
					return handler.batch(ctx)
				})
			}
		}
	}

	group.Add(func() error {
		return errorGroup.Wait()
	}, func(err error) {
		cancel()
	})

}

// handler private processor
// todo It's a bit messy
type handler struct {
	reader *kafka.Reader

	msgCh      chan *kafka.Message
	handleFunc HandleFunc

	batchCh   chan *batchInfo
	batchFunc BatchFunc
	hasBatch  bool

	info   *Info
	ticker *time.Ticker

	logger logging.LevelLogger

	closeMsgCh   func()
	closeBatchCh func()
}

// read fetch message from kafka
func (h *handler) read(ctx context.Context) error {
	defer h.closeMsgCh()
	for {
		select {
		default:
			message, err := h.reader.FetchMessage(ctx)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case h.msgCh <- &message:
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// handle call Handler.Handle
func (h *handler) handle(ctx context.Context) error {
	defer h.closeBatchCh()
	for {
		select {
		case msg := <-h.msgCh:
			v, err := h.handleFunc(ctx, msg)
			if err != nil {
				if IsFatalErr(err) {
					return err
				}
				h.logger.Warn("action", "handle", "err", err)
			}

			if !h.hasBatch || v == nil {
				if err := h.commit(*msg); err != nil {
					return err
				}
				continue
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case h.batchCh <- &batchInfo{message: msg, data: v}:
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// batch Call BatchHandler.Batch and commit *kafka.Message.
func (h *handler) batch(ctx context.Context) error {
	defer h.ticker.Stop()

	var data = make([]interface{}, 0, h.info.batchSize())
	var messages = make([]kafka.Message, 0, h.info.batchSize())

	doFunc := func() error {
		if len(data) == 0 {
			return nil
		}
		if err := h.batchFunc(ctx, data); err != nil {
			if IsFatalErr(err) {
				return err
			}
			h.logger.Warn("action", "batch", "err", err)
		}

		if err := h.commit(messages...); err != nil {
			return err
		}
		data = data[0:0]
		messages = messages[0:0]
		h.ticker.Reset(h.info.autoBatchInterval())
		return nil
	}

	for {
		select {
		case v := <-h.batchCh:
			if v == nil {
				continue
			}
			if v.message != nil {
				messages = append(messages, *v.message)
			}
			if v.data != nil {
				data = append(data, v.data)
			}
			if len(data) < h.info.batchSize() {
				continue
			}
			if err := doFunc(); err != nil {
				return err
			}
		case <-h.ticker.C:
			if err := doFunc(); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *handler) commit(messages ...kafka.Message) error {
	if len(messages) > 0 {
		if err := h.reader.CommitMessages(context.Background(), messages...); err != nil {
			return err
		}
	}
	return nil
}
