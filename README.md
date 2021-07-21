### core-processor

The project provides a framework for consuming Kafka.

It aims to simplify the logic of data consumption and transmission, and actively provide a configurable and efficient way.

With [core](https://github.com/DoNewsCode/core) and `core-processor`, we can do this:
```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    
    "github.com/DoNewsCode/core"
    processor "github.com/DoNewsCode/core-processor"
    "github.com/DoNewsCode/core/di"
    "github.com/DoNewsCode/core/otkafka"
    "github.com/segmentio/kafka-go"
)

type Handler struct {
}

func NewHandlerOut() processor.Out {
    return processor.NewOut(
        &Handler{},
    )
}

type Data struct {
    ID   int    `json:"id"`
    Name string `json:"name"`
}

func (h *Handler) Info() *processor.Info {
    return &processor.Info{
        Name:      "default", // the reader name is default
        BatchSize: 3,
    }
}

func (h *Handler) Handle(ctx context.Context, msg *kafka.Message) (interface{}, error) {
    e := &Data{}
    if err := json.Unmarshal(msg.Value, &e); err != nil {
        return nil, err
    }
    return e, nil
}

func (h *Handler) Batch(ctx context.Context, data []interface{}) error {
    for _, e := range data {
        fmt.Println(e.(*Data))
    }
    return nil
}

func main() {
    // prepare config and dependencies
    c := core.New(
        core.WithInline("kafka.reader.default.brokers", []string{"127.0.0.1:9092"}),
        core.WithInline("kafka.reader.default.topic", "test"),
        core.WithInline("kafka.reader.default.groupID", "test"),
        core.WithInline("kafka.reader.default.startOffset", kafka.FirstOffset),
    )
    defer c.Shutdown()
    c.ProvideEssentials()
    c.Provide(otkafka.Providers())
    c.AddModuleFunc(processor.New)

    // provide your handlers
    c.Provide(di.Deps{
        NewHandlerOut,
    })
    
    // start server
    err := c.Serve(context.Background())
    if err != nil {
        panic(err)
    }
}

```

After the above, we just need to add  handlers and provide new methods for core.
We can use `processor.Info` to flexibly adjust the operation of the processor.

Have fun!
