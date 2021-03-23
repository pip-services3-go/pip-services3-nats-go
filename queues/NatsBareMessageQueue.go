package queues

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	cqueues "github.com/pip-services3-go/pip-services3-messaging-go/queues"
)

/*
NatsBareMessageQueue are message queue that sends and receives messages via NATS message broker.

 Configuration parameters:

- subject:                       name of NATS topic (subject) to subscribe
- queue_group:                   name of NATS queue group
- connection(s):
  - discovery_key:               (optional) a key to retrieve the connection from  IDiscovery
  - host:                        host name or IP address
  - port:                        port number
  - uri:                         resource URI or connection string with all parameters in it
- credential(s):
  - store_key:                   (optional) a key to retrieve the credentials from  ICredentialStore
  - username:                    user name
  - password:                    user password
- options:
  - serialize_message:    (optional) true to serialize entire message as JSON, false to send only message payload (default: true)
  - retry_connect:        (optional) turns on/off automated reconnect when connection is log (default: true)
  - max_reconnect:        (optional) maximum reconnection attempts (default: 3)
  - reconnect_timeout:    (optional) number of milliseconds to wait on each reconnection attempt (default: 3000)
  - flush_timeout:        (optional) number of milliseconds to wait on flushing messages (default: 3000)

 References:

- *:logger:*:*:1.0             (optional)  ILogger components to pass log messages
- *:counters:*:*:1.0           (optional)  ICounters components to pass collected measurements
- *:discovery:*:*:1.0          (optional)  IDiscovery services to resolve connections
- *:credential-store:*:*:1.0   (optional) Credential stores to resolve credentials

See MessageQueue
See MessagingCapabilities

Example:

    queue := NewNatsBareMessageQueue("myqueue")
    queue.Configure(cconf.NewConfigParamsFromTuples(
      "subject", "mytopic",
	  "queue_group", "mygroup",
      "connection.protocol", "nats"
      "connection.host", "localhost"
      "connection.port", 1883
    ))

    queue.open("123")

    queue.Send("123", NewMessageEnvelope("", "mymessage", "ABC"))

    message, err := queue.Receive("123")
	if (message != nil) {
		...
		queue.Complete("123", message);
	}
*/
type NatsBareMessageQueue struct {
	*NatsAbstractMessageQueue
	subscription *nats.Subscription
}

// NewNatsBareMessageQueue are creates a new instance of the message queue.
// Parameters:
//   - name  string (optional) a queue name.
func NewNatsBareMessageQueue(name string) *NatsBareMessageQueue {
	c := NatsBareMessageQueue{}
	c.NatsAbstractMessageQueue = InheritNatsAbstractMessageQueue(&c, name)
	c.Capabilities = cqueues.NewMessagingCapabilities(false, true, true, false, false, false, false, false, false)
	return &c
}

// Peek method are peeks a single incoming message from the queue without removing it.
// If there are no messages available in the queue it returns nil.
// Parameters:
//   - correlationId  string  (optional) transaction id to trace execution through call chain.
// Returns: result *cqueues.MessageEnvelope, err error
// message or error.
func (c *NatsBareMessageQueue) Peek(correlationId string) (*cqueues.MessageEnvelope, error) {
	// Not supported
	return nil, nil
}

// PeekBatch method are peeks multiple incoming messages from the queue without removing them.
// If there are no messages available in the queue it returns an empty list.
// Important: This method is not supported by NATS.
// Parameters:
//   - correlationId     (optional) transaction id to trace execution through call chain.
//   - messageCount      a maximum number of messages to peek.
// Returns:          callback function that receives a list with messages or error.
func (c *NatsBareMessageQueue) PeekBatch(correlationId string, messageCount int64) ([]*cqueues.MessageEnvelope, error) {
	// Not supported
	return []*cqueues.MessageEnvelope{}, nil
}

// Receive method are receives an incoming message and removes it from the queue.
// Parameters:
//  - correlationId   string   (optional) transaction id to trace execution through call chain.
//  - waitTimeout  time.Duration     a timeout in milliseconds to wait for a message to come.
// Returns:  result *cqueues.MessageEnvelope, err error
// receives a message or error.
func (c *NatsBareMessageQueue) Receive(correlationId string, waitTimeout time.Duration) (*cqueues.MessageEnvelope, error) {
	err := c.CheckOpen("")
	if err != nil {
		return nil, err
	}

	// Create a temporary subscription
	var subscription *nats.Subscription
	if c.QueueGroup != "" {
		subscription, err = c.Client.QueueSubscribeSync(c.SubscriptionSubject(), c.QueueGroup)
	} else {
		subscription, err = c.Client.SubscribeSync(c.SubscriptionSubject())
	}
	if err != nil {
		return nil, err
	}
	defer subscription.Unsubscribe()

	// Wait for a message
	msg, err := subscription.NextMsg(waitTimeout)
	if err != nil {
		return nil, err
	}

	// Convert the message and return
	return c.ToMessage(msg)
}

func (c *NatsBareMessageQueue) receiveMessage(receiver cqueues.IMessageReceiver) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		// Deserialize message
		message, err := c.ToMessage(msg)
		if err != nil {
			c.Logger.Error("", err, "Failed to read received message")
		}

		c.Counters.IncrementOne("queue." + c.GetName() + ".received_messages")
		c.Logger.Debug(message.CorrelationId, "Received message %s via %s", msg, c.GetName())

		// Pass the message to receiver and recover after panic
		func(message *cqueues.MessageEnvelope) {
			defer func() {
				if r := recover(); r != nil {
					err := fmt.Sprintf("%v", r)
					c.Logger.Error(message.CorrelationId, nil, "Failed to process the message - "+err)
				}
			}()

			err = receiver.ReceiveMessage(message, c)
			if err != nil {
				c.Logger.Error(message.CorrelationId, err, "Failed to process the message")
			}
		}(message)
	}
}

// Listens for incoming messages and blocks the current thread until queue is closed.
// Parameters:
//  - correlationId   string  (optional) transaction id to trace execution through call chain.
//  - receiver    cqueues.IMessageReceiver      a receiver to receive incoming messages.
//
// See IMessageReceiver
// See receive
func (c *NatsBareMessageQueue) Listen(correlationId string, receiver cqueues.IMessageReceiver) error {
	err := c.CheckOpen("")
	if err != nil {
		return err
	}

	if c.QueueGroup != "" {
		c.subscription, err = c.Client.QueueSubscribe(c.SubscriptionSubject(), c.QueueGroup, c.receiveMessage(receiver))
	} else {
		c.subscription, err = c.Client.Subscribe(c.SubscriptionSubject(), c.receiveMessage(receiver))
	}

	return err
}

// EndListen method are ends listening for incoming messages.
// When this method is call listen unblocks the thread and execution continues.
// Parameters:
//   - correlationId  string   (optional) transaction id to trace execution through call chain.
func (c *NatsBareMessageQueue) EndListen(correlationId string) {
	if c.subscription != nil {
		c.subscription.Unsubscribe()
		c.subscription = nil
	}
}
