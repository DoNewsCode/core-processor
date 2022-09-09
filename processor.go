package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/DoNewsCode/core/contract"
	"github.com/DoNewsCode/core/di"
	"github.com/DoNewsCode/core/logging"
	"github.com/DoNewsCode/core/otkafka"
	"github.com/go-kit/log"
	"github.com/oklog/run"
	"golang.org/x/sync/errgroup"
)

// Processor dispatch Handler.
type Processor struct {
	maker    otkafka.ReaderMaker
	handlers []*handler
	logger   logging.LevelLogger
	config   contract.ConfigUnmarshaler

	eg     *errgroup.Group
	ctx    context.Context
	cancel context.CancelFunc

	// cant add duplicate processor with same name
	set map[string]struct{}
}

type in struct {
	di.In

	Maker     otkafka.ReaderMaker
	Logger    log.Logger
	Container contract.Container
	Config    contract.ConfigUnmarshaler
}

// provider provides Processor.
type provider interface {
	ProvideProcessor(p *Processor)
}

// New create *Processor Module.
func New(i in) (*Processor, error) {
	e := &Processor{
		maker:    i.Maker,
		logger:   logging.WithLevel(log.With(i.Logger, "module", "processor")),
		handlers: []*handler{},
		config:   i.Config,
		set:      map[string]struct{}{},
	}

	applyHandler(i.Container, e)

	ctx, cancel := context.WithCancel(context.Background())
	errorGroup, ctx := errgroup.WithContext(ctx)

	e.ctx = ctx
	e.cancel = cancel
	e.eg = errorGroup

	return e, nil
}

// newHandler create handler and add to Processor.handlers
func (p *Processor) newHandler(h Handler, batch bool) (*handler, error) {
	name := "default"
	if h.Name() != "" {
		name = h.Name()
	}
	if _, ok := p.set[name]; ok {
		return nil, fmt.Errorf("duplicate processor name: %s", name)
	}
	p.set[name] = struct{}{}
	var opt = options{
		Name:              name,
		BatchWorker:       1,
		BatchSize:         1,
		HandleWorker:      1,
		AutoBatchInterval: time.Minute,
		AutoCommit:        false,
	}
	if err := p.config.Unmarshal("processor."+name, &opt); err != nil {
		p.logger.Warnf("load %s options error: %v and using default options", name, err)
	}
	reader, err := p.maker.Make(name)
	if err != nil {
		return nil, fmt.Errorf("get reader %s error: %w", name, err)
	}

	if reader.Config().GroupID == "" {
		return nil, fmt.Errorf("reader %s did not set consumer group id", name)
	}

	var hd = &handler{
		reader:     reader,
		options:    &opt,
		logger:     logging.WithLevel(log.With(p.logger, "handler", name)),
		handleFunc: h.Handle,
	}

	if bf, ok := h.(BatchHandler); ok || batch {
		hd.batchFunc = bf.Batch
		hd.batchCh = make(chan *batchInfo)
	}
	return hd, nil
}

func applyHandler(ctn contract.Container, pp *Processor) {
	modules := ctn.Modules()
	for i := range modules {
		if p, ok := modules[i].(provider); ok {
			p.ProvideProcessor(pp)
		}
	}
}

// ProvideRunGroup run workers:
// 	1. Fetch message from *kafka.Reader.
// 	2. Handle message by Handler.Handle.
// 	3. Batch data by BatchHandler.Batch. If batch success then commit message.
func (p *Processor) ProvideRunGroup(group *run.Group) {
	for _, h := range p.handlers {
		p.run(h)
	}
	p.eg.Go(func() error {
		<-p.ctx.Done()
		return p.ctx.Err()
	})

	group.Add(func() error {
		return p.eg.Wait()
	}, func(err error) {
		p.cancel()
	})
}

// AddHandler add Handler to Processor.handlers
func (p *Processor) AddHandler(hs ...Handler) error {
	for _, hh := range hs {
		hd, err := p.newHandler(hh, false)
		if err != nil {
			return err
		}
		p.handlers = append(p.handlers, hd)
	}
	return nil
}

// AddHandlerFunc add functions of Handler to Processor.handlers
func (p *Processor) AddHandlerFunc(name string, hf HandleFunc, bf BatchFunc) error {
	hd, err := p.newHandler(&wrapHandler{
		name: name,
		hf:   hf,
		bf:   bf,
	}, bf != nil)
	if err != nil {
		return err
	}
	p.handlers = append(p.handlers, hd)
	return nil
}

func (p *Processor) run(h *handler) {
	for i := 0; i < h.options.HandleWorker; i++ {
		p.eg.Go(func() error {
			return h.handle(p.ctx)
		})
	}

	if h.batchFunc != nil {
		for i := 0; i < h.options.BatchWorker; i++ {
			p.eg.Go(func() error {
				return h.batch(p.ctx)
			})
		}
	}
}
