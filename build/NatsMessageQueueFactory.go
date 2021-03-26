package build

import (
	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	"github.com/pip-services3-go/pip-services3-components-go/build"
	cqueues "github.com/pip-services3-go/pip-services3-messaging-go/queues"
	"github.com/pip-services3-go/pip-services3-nats-go/queues"
)

// NatsMessageQueueFactory are creates NatsMessageQueue components by their descriptors.
// Name of created message queue is taken from its descriptor.
//
// See Factory
// See NatsMessageQueue
type NatsMessageQueueFactory struct {
	build.Factory
	config     *cconf.ConfigParams
	references cref.IReferences
}

// NewNatsMessageQueueFactory method are create a new instance of the factory.
func NewNatsMessageQueueFactory() *NatsMessageQueueFactory {
	c := NatsMessageQueueFactory{}

	bareNatsQueueDescriptor := cref.NewDescriptor("pip-services", "message-queue", "bare-nats", "*", "1.0")
	natsQueueDescriptor := cref.NewDescriptor("pip-services", "message-queue", "nats", "*", "1.0")

	c.Register(bareNatsQueueDescriptor, func(locator interface{}) interface{} {
		name := ""
		descriptor, ok := locator.(*cref.Descriptor)
		if ok {
			name = descriptor.Name()
		}
		return c.CreateBareQueue(name)
	})

	c.Register(natsQueueDescriptor, func(locator interface{}) interface{} {
		name := ""
		descriptor, ok := locator.(*cref.Descriptor)
		if ok {
			name = descriptor.Name()
		}
		return c.CreateQueue(name)
	})

	return &c
}

func (c *NatsMessageQueueFactory) Configure(config *cconf.ConfigParams) {
	c.config = config
}

func (c *NatsMessageQueueFactory) SetReferences(references cref.IReferences) {
	c.references = references
}

// Creates a message queue component and assigns its name.
//
// Parameters:
//   - name: a name of the created message queue.
func (c *NatsMessageQueueFactory) CreateQueue(name string) cqueues.IMessageQueue {
	queue := queues.NewNatsMessageQueue(name)

	if c.config != nil {
		queue.Configure(c.config)
	}
	if c.references != nil {
		queue.SetReferences(c.references)
	}

	return queue
}

// Creates a message queue component and assigns its name.
//
// Parameters:
//   - name: a name of the created message queue.
func (c *NatsMessageQueueFactory) CreateBareQueue(name string) cqueues.IMessageQueue {
	queue := queues.NewNatsBareMessageQueue(name)

	if c.config != nil {
		queue.Configure(c.config)
	}
	if c.references != nil {
		queue.SetReferences(c.references)
	}

	return queue
}
