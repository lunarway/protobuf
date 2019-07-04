package events

import (
	"fmt"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/golang/protobuf/protoc-gen-go/generator"
	"strings"
)

func init() {
	generator.RegisterPlugin(new(events))
}

const contentType = "application/x-protobuf"

type events struct {
	gen             *generator.Generator
	consumers       []string
	publisher       []string
	allConsumerName string
	publisherName   string
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
	for _, message := range file.FileDescriptorProto.MessageType {
		e.generateMessage(message)
	}

	e.allConsumerName = fmt.Sprintf("%sConsumeAll", strings.Title(file.GetPackage()))
	e.publisherName = fmt.Sprintf("%sPublisher", strings.Title(file.GetPackage()))

	e.generateCombinedInterfaces()

	e.P("// Name that groups these events")
	e.P("const serviceName string = \"", file.GetPackage(), "\"")
	e.P("const contentType string = \"", contentType, "\"")

	e.generatePublisher()

	e.generateConsumer()
}

func (e *events) GenerateImports(file *generator.FileDescriptor) {
	e.P(
		`import (
	"context"
	"go.lunarway.com/lw-go-postoffice"
	"go.lunarway.com/lw-go-postoffice/protobufs"
	"go.lunarway.com/lw-go-postoffice/store"
)`)
}

// P forwards to g.gen.P.
func (e *events) P(args ...interface{}) { e.gen.P(args...) }

func (e *events) generateMessage(message *descriptor.DescriptorProto) {
	if message.Name == nil {
		return
	}
	messageName := message.GetName()

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

	e.P(fmt.Sprintf(`
func (event *%s) EventName() string {
	return "%s"
}
`, messageName, messageName))

	e.P(fmt.Sprintf(`
func (event *%s) EventMarshal() ([]byte, string, error) {
	b, err := proto.Marshal(event)
	return b, contentType, err
}
`, messageName))

	e.P(fmt.Sprintf(`
func (event *%s) EventUnmarshal(b []byte) error {
	return proto.Unmarshal(b, event)
}
`, messageName))

	//	e.P(fmt.Sprintf(`
	//type %sEvent struct {
	//	postoffice.Info
	//	Event *%s
	//}
	//`, messageName, messageName))

	e.P(fmt.Sprintf(`
func (event *%s) Consume(ctx context.Context, tx store.Transaction, info postoffice.Info, consumer postoffice.Consumer) (bool, error) {
	if c, ok := consumer.(Event%sConsumer); ok {
		return true, c.Consume%s(ctx, tx, info, event)
	}
	return false, nil
}
`, messageName, messageName, messageName))

	e.P(fmt.Sprintf(`
func init() {
	protobufs.RegisterEventType(serviceName, "%s", func() postoffice.Event {
		return &%s{}
	})
}
`, messageName, messageName))
}

func (e *events) generateCombinedInterfaces() {
	e.P("// Combined Interfaces")
	if len(e.publisher) > 0 {
		e.P("// Used on the producer side to publish events")
		e.P("type ", e.publisherName, " interface {")
		for _, messageName := range e.publisher {
			e.generatePublishInterfaceMethod(messageName)
		}
		e.P("}")
	}

	if len(e.consumers) > 0 {
		e.P("// Utility interface to help determine if you consume all event types")
		e.P("type ", e.allConsumerName, " interface {")
		for _, messageName := range e.consumers {
			e.generateConsumerMethod(messageName)
		}
		e.P("}")
	}

	e.P(`
// the mechanism that interacts with the outbox/inbox
type box interface {
	Publish(ctx context.Context, tx store.Transaction, serviceName string, entityID string, event postoffice.Event) error
	Register(publisherName string, consumer postoffice.Consumer)
}
`)
}

func (e *events) generatePublishInterfaceMethod(messageName string) {
	e.P("    Publish", messageName, "(ctx context.Context, entityID string, event *", messageName, ") error")
}

func (e *events) generatePublishMethod(messageName string) {
	e.P(fmt.Sprintf(`
	func (p *publisher) Publish%s(ctx context.Context, tx store.Transaction, entityID string, event *%s) error {
		return p.box.Publish(ctx, tx, serviceName, entityID, event)
	}
	`, messageName, messageName))
}

func (e *events) generateConsumerMethod(messageName string) {
	e.P("    Consume", messageName, "(context.Context, store.Transaction, postoffice.Info, *", messageName, ") error")
}

func (e *events) generatePublisher() {
	e.P("// The publisher of events")
	e.P("type publisher struct {")
	e.P("box box")
	e.P("}")
	e.P("")
	e.P("// Use this to create your publisher")
	e.P("func NewPublisher(outbox box) *publisher {")
	e.P("   return &publisher{")
	e.P(" box: outbox, ")
	e.P("   }")
	e.P("}")

	for _, messageName := range e.publisher {
		e.generatePublishMethod(messageName)
	}
}

func (e *events) generateConsumer() {
	e.P("// The registrar for event consumption")
	e.P(fmt.Sprintf(
		`
func RegisterAllConsumers(office box, consumer %s) {
	office.Register(serviceName, consumer)
}
`, e.allConsumerName))
}
