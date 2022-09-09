package processor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/DoNewsCode/core"
	"github.com/DoNewsCode/core/otkafka"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

type testData struct {
	ID int `json:"id"`
}

func TestProcessor(t *testing.T) {
	t.Parallel()
	if os.Getenv("KAFKA_ADDR") == "" {
		t.Skip()
	}

	c := core.New(
		core.WithInline("kafka.reader.A.brokers", envDefaultKafkaAddrs),
		core.WithInline("kafka.reader.A.topic", testTopic),
		core.WithInline("kafka.reader.A.groupID", "testA"),
		core.WithInline("kafka.reader.A.startOffset", kafka.FirstOffset),

		core.WithInline("kafka.reader.B.brokers", envDefaultKafkaAddrs),
		core.WithInline("kafka.reader.B.topic", testTopic),
		core.WithInline("kafka.reader.B.groupID", "testB"),
		core.WithInline("kafka.reader.B.startOffset", kafka.FirstOffset),

		core.WithInline("http.disable", "true"),
		core.WithInline("grpc.disable", "true"),
		core.WithInline("cron.disable", "true"),
		core.WithInline("log.level", "none"),

		core.WithInline("processor.A.batchSize", "3"),
		core.WithInline("processor.B.batchSize", "3"),
		core.WithInline("processor.B.autoCommit", "true"),
	)
	defer c.Shutdown()

	c.ProvideEssentials()
	c.Provide(otkafka.Providers())
	c.AddModuleFunc(New)

	chanA := make(chan *testData, 10)
	chanB := make(chan *testData, 10)
	handlerA := &wrapHandler{"A", func(ctx context.Context, msg *kafka.Message) (interface{}, error) {
		e := &testData{}
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			return nil, err
		}
		return e, nil
	}, func(ctx context.Context, data []interface{}) error {
		for _, v := range data {
			chanA <- v.(*testData)
		}
		return nil
	}}
	handlerB := &wrapHandler{"B", func(ctx context.Context, msg *kafka.Message) (interface{}, error) {
		e := &testData{}
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			return nil, err
		}
		return e, nil
	}, func(ctx context.Context, data []interface{}) error {
		for _, v := range data {
			chanB <- v.(*testData)
		}
		return nil
	}}

	c.Invoke(func(p *Processor) error {
		return p.AddHandler(
			handlerA, handlerB,
		)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := c.Serve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 3, len(chanA))
	assert.Equal(t, 4, len(chanB))
}

func TestProcessorBatchInterval(t *testing.T) {
	t.Parallel()

	if os.Getenv("KAFKA_ADDR") == "" {
		t.Skip()
	}

	c := core.New(
		core.WithInline("kafka.reader.default.brokers", envDefaultKafkaAddrs),
		core.WithInline("kafka.reader.default.topic", testTopic),
		core.WithInline("kafka.reader.default.groupID", "testE"),
		core.WithInline("kafka.reader.default.startOffset", kafka.FirstOffset),

		core.WithInline("http.disable", "true"),
		core.WithInline("grpc.disable", "true"),
		core.WithInline("cron.disable", "true"),
		core.WithInline("log.level", "none"),

		core.WithInline("processor.default.autoBatchInterval", "1s"),
	)
	defer c.Shutdown()
	c.ProvideEssentials()
	c.Provide(otkafka.Providers())

	ch := make(chan *testData, 10)
	hd := &wrapHandler{"default", func(ctx context.Context, msg *kafka.Message) (interface{}, error) {
		e := &testData{}
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			return nil, err
		}
		return e, nil
	}, func(ctx context.Context, data []interface{}) error {
		for _, v := range data {
			ch <- v.(*testData)
		}
		return nil
	}}

	c.AddModuleFunc(New)
	c.Invoke(func(p *Processor) error {
		return p.AddHandler(hd)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := c.Serve(ctx)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 4, len(ch))
}

func TestProcessorError(t *testing.T) {
	t.Parallel()

	if os.Getenv("KAFKA_ADDR") == "" {
		t.Skip()
	}

	c := core.New(
		core.WithInline("kafka.reader.default.brokers", envDefaultKafkaAddrs),
		core.WithInline("kafka.reader.default.topic", testTopic),
		core.WithInline("kafka.reader.default.groupID", "testF"),
		core.WithInline("kafka.reader.default.startOffset", kafka.FirstOffset),

		core.WithInline("http.disable", "true"),
		core.WithInline("grpc.disable", "true"),
		core.WithInline("cron.disable", "true"),
		core.WithInline("log.level", "warn"),
	)
	defer c.Shutdown()
	c.ProvideEssentials()
	c.Provide(otkafka.Providers())

	hd := &wrapHandler{"default", func(ctx context.Context, msg *kafka.Message) (interface{}, error) {
		return 1, errors.New("ignored error")
	}, func(ctx context.Context, data []interface{}) error {
		return NewFatalErr(errors.New("fatal error"))
	}}
	c.AddModuleFunc(New)
	c.Invoke(func(p *Processor) error {
		return p.AddHandler(hd)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.Serve(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "fatal error", err.Error())
	}
}
