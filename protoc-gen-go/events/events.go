package events

import (
	"fmt"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/golang/protobuf/protoc-gen-go/generator"
)

func init() {
	generator.RegisterPlugin(new(events))
}

type events struct {
	gen       *generator.Generator
	consumers []string
	publisher []string
}

func (e *events) Name() string {
	return "events"
}

func (e *events) Init(g *generator.Generator) {
	e.gen = g
	e.consumers = make([]string, 0)
	e.publisher = make([]string, 0)
}

func (e *events) Generate(file *generator.FileDescriptor) {
	e.P("// HELLO WORLD!")
	for _, message := range file.FileDescriptorProto.MessageType {
		e.generateMessageInterface(message)
	}

	e.generateCombinedInterfaces()

	e.generatePublisher()
}

func (e *events) GenerateImports(file *generator.FileDescriptor) {
	e.P(
		`import (
	"context"
)`)
}

// P forwards to g.gen.P.
func (e *events) P(args ...interface{}) { e.gen.P(args...) }

func (e *events) generateMessageInterface(message *descriptor.DescriptorProto) {
	if message.Name == nil {
		return
	}
	messageName := *message.Name

	e.P("")
	e.P("// ", messageName)

	e.P("type Event", messageName, "Consumer interface {")
	e.generateConsumerMethod(messageName)
	e.P("}")
	e.consumers = append(e.consumers, messageName)

	e.P("")

	e.P("type Event", messageName, "Publisher interface {")
	e.generatePublishInterfaceMethod(messageName)
	e.P("}")
	e.publisher = append(e.publisher, messageName)
}

func (e *events) generateCombinedInterfaces() {
	e.P("// Combined Interfaces")
	if len(e.publisher) > 0 {
		e.P("// Used on the producer side to publish events")
		e.P("type Publisher interface {")
		for _, messageName := range e.publisher {
			e.generatePublishInterfaceMethod(messageName)
		}
		e.P("}")
	}

	if len(e.consumers) > 0 {
		e.P("// Utility interface to help determine if you consume all event types")
		e.P("type Consumer interface {")
		for _, messageName := range e.consumers {
			e.generateConsumerMethod(messageName)
		}
		e.P("}")
	}

	e.P(`
// the mechanism that interacts with the outbox/inbox
type transport interface {
	Publish(ctx context.Context, serviceName string, eventName string, eventData []byte) error
}
`)
}

func (e *events) generatePublishInterfaceMethod(messageName string) {
	e.P("    Publish", messageName, "(context.Context, *", messageName, ") error")
}

func (e *events) generatePublishMethod(messageName string) {
	serviceName := "accounting"
	e.P("func (p *publisher) Publish", messageName, "(ctx context.Context, event *", messageName, ") error {")
	e.P("	data, err := proto.Marshal(event)")
	e.P("if err != nil { return err }")
	e.P(fmt.Sprintf(`return p.transport.Publish(ctx, "%s", "%s", data)`, serviceName, messageName))
	e.P("}")
}

func (e *events) generateConsumerMethod(messageName string) {
	e.P("    Consume", messageName, "(context.Context, *", messageName, ")")
}

func (e *events) generatePublisher() {
	e.P("// The publisher of events")
	e.P("type publisher struct {")
	e.P("transport transport")
	e.P("}")
	e.P("")
	e.P("// Use this to create your publisher")
	e.P("func NewPublisher(transport transport) *publisher {")
	e.P("   return &publisher{")
	e.P(" transport: transport, ")
	e.P("   }")
	e.P("}")

	for _, messageName := range e.publisher {
		e.generatePublishMethod(messageName)
	}
}
