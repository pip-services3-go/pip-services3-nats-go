package queues

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	cqueues "github.com/pip-services3-go/pip-services3-messaging-go/queues"
)

/*
NatsMessageQueue are message queue that sends and receives messages via NATS message broker.

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
- *:connection:nats:*:1.0      (optional) Shared connection to NATS service

See MessageQueue
See MessagingCapabilities

Example:

    queue := NewNatsMessageQueue("myqueue")
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
type NatsMessageQueue struct {
	*NatsAbstractMessageQueue

	messages []cqueues.MessageEnvelope
	receiver cqueues.IMessageReceiver
}

// NewNatsMessageQueue are creates a new instance of the message queue.
// Parameters:
//   - name  string (optional) a queue name.
func NewNatsMessageQueue(name string) *NatsMessageQueue {
	c := NatsMessageQueue{}

	c.NatsAbstractMessageQueue = InheritNatsAbstractMessageQueue(&c, name,
		cqueues.NewMessagingCapabilities(false, true, true, true, true, false, false, false, true))

	c.messages = make([]cqueues.MessageEnvelope, 0)

	return &c
}

// Opens the component with given connection and credential parameters.
// Parameters:
//   - correlationId     (optional) transaction id to trace execution through call chain.
// Returns error or nil no errors occured.
func (c *NatsMessageQueue) Open(correlationId string) error {
	if c.IsOpen() {
		return nil
	}

	err := c.NatsAbstractMessageQueue.Open(correlationId)
	if err != nil {
		return err
	}

	// Subscribe right away
	subject := c.SubscriptionSubject()
	err = c.Connection.Subscribe(subject, c.QueueGroup, c)
	if err != nil {
		c.Close(correlationId)
		return err
	}

	return nil
}

// Close method are Closes component and frees used resources.
// Parameters:
//   - correlationId string 	(optional) transaction id to trace execution through call chain.
// Returns error or nil no errors occured.
func (c *NatsMessageQueue) Close(correlationId string) error {
	if !c.IsOpen() {
		return nil
	}

	err := c.NatsAbstractMessageQueue.Close(correlationId)

	// Unsubscribe from topic
	subject := c.SubscriptionSubject()
	c.Connection.Unsubscribe(subject, c.QueueGroup, c)

	c.Lock.Lock()
	defer c.Lock.Unlock()
	c.receiver = nil
	c.messages = make([]cqueues.MessageEnvelope, 0)

	return err
}

// Clear method are clears component state.
// Parameters:
//   - correlationId 	string (optional) transaction id to trace execution through call chain.
// Returns error or nil no errors occured.
func (c *NatsMessageQueue) Clear(correlationId string) (err error) {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.messages = make([]cqueues.MessageEnvelope, 0)
	c.receiver = nil

	return nil
}

// ReadMessageCount method are reads the current number of messages in the queue to be delivered.
// Returns number of messages or error.
func (c *NatsMessageQueue) ReadMessageCount() (count int64, err error) {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	count = (int64)(len(c.messages))
	return count, nil
}

// Peek method are peeks a single incoming message from the queue without removing it.
// If there are no messages available in the queue it returns nil.
// Parameters:
//   - correlationId  string  (optional) transaction id to trace execution through call chain.
// Returns: result *cqueues.MessageEnvelope, err error
// message or error.
func (c *NatsMessageQueue) Peek(correlationId string) (*cqueues.MessageEnvelope, error) {
	err := c.CheckOpen(correlationId)
	if err != nil {
		return nil, err
	}

	var message *cqueues.MessageEnvelope

	// Pick a message
	c.Lock.Lock()
	if len(c.messages) > 0 {
		message = &c.messages[0]
	}
	c.Lock.Unlock()

	if message != nil {
		c.Logger.Trace(message.CorrelationId, "Peeked message %s on %s", message, c.String())
	}

	return message, nil
}

// PeekBatch method are peeks multiple incoming messages from the queue without removing them.
// If there are no messages available in the queue it returns an empty list.
// Important: This method is not supported by NATS.
// Parameters:
//   - correlationId     (optional) transaction id to trace execution through call chain.
//   - messageCount      a maximum number of messages to peek.
// Returns:          callback function that receives a list with messages or error.
func (c *NatsMessageQueue) PeekBatch(correlationId string, messageCount int64) ([]*cqueues.MessageEnvelope, error) {
	err := c.CheckOpen(correlationId)
	if err != nil {
		return nil, err
	}

	c.Lock.Lock()
	batchMessages := c.messages
	if messageCount <= (int64)(len(batchMessages)) {
		batchMessages = batchMessages[0:messageCount]
	}
	c.Lock.Unlock()

	messages := []*cqueues.MessageEnvelope{}
	for _, message := range batchMessages {
		messages = append(messages, &message)
	}

	c.Logger.Trace(correlationId, "Peeked %d messages on %s", len(messages), c.Name())

	return messages, nil
}

// Receive method are receives an incoming message and removes it from the queue.
// Parameters:
//  - correlationId   string   (optional) transaction id to trace execution through call chain.
//  - waitTimeout  time.Duration     a timeout in milliseconds to wait for a message to come.
// Returns:  result *cqueues.MessageEnvelope, err error
// receives a message or error.
func (c *NatsMessageQueue) Receive(correlationId string, waitTimeout time.Duration) (*cqueues.MessageEnvelope, error) {
	err := c.CheckOpen(correlationId)
	if err != nil {
		return nil, err
	}

	messageReceived := false
	var message *cqueues.MessageEnvelope
	elapsedTime := time.Duration(0)

	for elapsedTime < waitTimeout && !messageReceived {
		c.Lock.Lock()
		if len(c.messages) == 0 {
			c.Lock.Unlock()
			time.Sleep(time.Duration(100) * time.Millisecond)
			elapsedTime += time.Duration(100)
			continue
		}

		// Get message from the queue
		message = &c.messages[0]
		c.messages = c.messages[1:]

		// Add messages to locked messages list
		messageReceived = true
		c.Lock.Unlock()
	}

	return message, nil
}

func (c *NatsMessageQueue) OnMessage(msg *nats.Msg) {
	// Deserialize message
	message, err := c.ToMessage(msg)
	if err != nil {
		c.Logger.Error("", err, "Failed to read received message")
	}

	c.Counters.IncrementOne("queue." + c.Name() + ".received_messages")
	c.Logger.Debug(message.CorrelationId, "Received message %s via %s", msg, c.Name())

	// Send message to receiver if its set or put it into the queue
	c.Lock.Lock()
	if c.receiver != nil {
		receiver := c.receiver
		c.Lock.Unlock()
		c.sendMessageToReceiver(receiver, message)
	} else {
		c.messages = append(c.messages, *message)
		c.Lock.Unlock()
	}
}

func (c *NatsMessageQueue) sendMessageToReceiver(receiver cqueues.IMessageReceiver, message *cqueues.MessageEnvelope) {
	correlationId := message.CorrelationId

	defer func() {
		if r := recover(); r != nil {
			err := fmt.Sprintf("%v", r)
			c.Logger.Error(correlationId, nil, "Failed to process the message - "+err)
		}
	}()

	err := receiver.ReceiveMessage(message, c)
	if err != nil {
		c.Logger.Error(correlationId, err, "Failed to process the message")
	}
}

// Listens for incoming messages and blocks the current thread until queue is closed.
// Parameters:
//  - correlationId   string  (optional) transaction id to trace execution through call chain.
//  - receiver    cqueues.IMessageReceiver      a receiver to receive incoming messages.
//
// See IMessageReceiver
// See receive
func (c *NatsMessageQueue) Listen(correlationId string, receiver cqueues.IMessageReceiver) error {
	err := c.CheckOpen(correlationId)
	if err != nil {
		return err
	}

	c.Logger.Trace("", "Started listening messages at %s", c.Name())

	// Get all collected messages
	c.Lock.Lock()
	batchMessages := c.messages
	c.messages = []cqueues.MessageEnvelope{}
	c.Lock.Unlock()

	// Resend collected messages to receiver
	for _, message := range batchMessages {
		receiver.ReceiveMessage(&message, c)
	}

	// Set the receiver
	c.Lock.Lock()
	c.receiver = receiver
	c.Lock.Unlock()

	return nil
}

// EndListen method are ends listening for incoming messages.
// When this method is call listen unblocks the thread and execution continues.
// Parameters:
//   - correlationId  string   (optional) transaction id to trace execution through call chain.
func (c *NatsMessageQueue) EndListen(correlationId string) {
	c.Lock.Lock()
	c.receiver = nil
	c.Lock.Unlock()
}
