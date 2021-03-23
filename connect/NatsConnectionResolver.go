package connect

import (
	"strconv"
	"strings"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cauth "github.com/pip-services3-go/pip-services3-components-go/auth"
	ccon "github.com/pip-services3-go/pip-services3-components-go/connect"
)

/*
NatsConnectionResolver helper class that resolves Nats connection and credential parameters,
validates them and generates connection options.

  Configuration parameters:

- connection(s):
  - discovery_key:               (optional) a key to retrieve the connection from IDiscovery
  - host:                        host name or IP address
  - port:                        port number
  - uri:                         resource URI or connection string with all parameters in it
- credential(s):
  - store_key:                   (optional) a key to retrieve the credentials from ICredentialStore
  - username:                    user name
  - password:                    user password

 References:

- *:discovery:*:*:1.0          (optional) IDiscovery services to resolve connections
- *:credential-store:*:*:1.0   (optional) Credential stores to resolve credentials
*/
type NatsConnectionResolver struct {
	// The connections resolver.
	ConnectionResolver *ccon.ConnectionResolver
	//The credentials resolver.
	CredentialResolver *cauth.CredentialResolver
}

func NewNatsConnectionResolver() *NatsConnectionResolver {
	c := NatsConnectionResolver{}
	c.ConnectionResolver = ccon.NewEmptyConnectionResolver()
	c.CredentialResolver = cauth.NewEmptyCredentialResolver()
	return &c
}

// Configure are configures component by passing configuration parameters.
// Parameters:
//  - config   *cconf.ConfigParams
// configuration parameters to be set.
func (c *NatsConnectionResolver) Configure(config *cconf.ConfigParams) {
	c.ConnectionResolver.Configure(config)
	c.CredentialResolver.Configure(config)
}

// SetReferences are sets references to dependent components.
// Parameters:
//  - references  cref.IReferences
//	references to locate the component dependencies.
func (c *NatsConnectionResolver) SetReferences(references cref.IReferences) {
	c.ConnectionResolver.SetReferences(references)
	c.CredentialResolver.SetReferences(references)
}

func (c *NatsConnectionResolver) validateConnection(correlationId string, connection *ccon.ConnectionParams) error {
	if connection == nil {
		return cerr.NewConfigError(correlationId, "NO_CONNECTION", "Nats connection is not set")
	}

	uri := connection.Uri()
	if uri != "" {
		return nil
	}

	protocol := connection.ProtocolWithDefault("nats")
	if protocol == "" {
		return cerr.NewConfigError(correlationId, "NO_PROTOCOL", "Connection protocol is not set")
	}
	if protocol != "nats" {
		return cerr.NewConfigError(correlationId, "UNSUPPORTED_PROTOCOL", "The protocol "+protocol+" is not supported")
	}

	host := connection.Host()
	if host == "" {
		return cerr.NewConfigError(correlationId, "NO_HOST", "Connection host is not set")
	}

	port := connection.PortWithDefault(4222)
	if port == 0 {
		return cerr.NewConfigError(correlationId, "NO_PORT", "Connection port is not set")
	}

	return nil
}

func (c *NatsConnectionResolver) composeOptions(connections []*ccon.ConnectionParams,
	credential *cauth.CredentialParams) *cconf.ConfigParams {

	// Define additional parameters parameters
	if credential == nil {
		credential = cauth.NewEmptyCredentialParams()
	}

	// Contruct options and copy over credentials
	options := cconf.NewEmptyConfigParams().SetDefaults(&credential.ConfigParams)

	globalUri := ""
	uriBuilder := strings.Builder{}

	// Process connections, find or constract uri
	for _, connection := range connections {
		options = options.SetDefaults(&connection.ConfigParams)

		if globalUri != "" {
			continue
		}

		uri := connection.Uri()
		if uri != "" {
			globalUri = uri
			continue
		}

		if uriBuilder.Len() > 0 {
			uriBuilder.WriteString(", ")
		}

		protocol := connection.ProtocolWithDefault("nats")
		uriBuilder.WriteString(protocol)

		host := connection.Host()
		uriBuilder.WriteString("://")
		uriBuilder.WriteString(host)

		port := connection.PortWithDefault(4222)
		uriBuilder.WriteString(":")
		uriBuilder.WriteString(strconv.Itoa(port))
	}

	// Set connection uri
	if globalUri != "" {
		options.SetAsObject("uri", globalUri)
	} else {
		options.SetAsObject("uri", uriBuilder.String())
	}

	return options
}

// Resolves Nats connection options from connection and credential parameters.
// Parameters:
//   - correlationId   string
//   (optional) transaction id to trace execution through call chain.
// Returns options *cconf.ConfigParams, err error
// receives resolved options or error.
func (c *NatsConnectionResolver) Resolve(correlationId string) (*cconf.ConfigParams, error) {
	connections, err := c.ConnectionResolver.ResolveAll(correlationId)
	if err != nil {
		return nil, err
	}

	credential, err := c.CredentialResolver.Lookup(correlationId)
	if err != nil {
		return nil, err
	}

	// Validate connections
	for _, connection := range connections {
		err = c.validateConnection(correlationId, connection)
		if err != nil {
			return nil, err
		}
	}

	options := c.composeOptions(connections, credential)
	return options, nil
}

// Compose method are composes Nats connection options from connection and credential parameters.
// Parameters:
//   - correlationId  string  (optional) transaction id to trace execution through call chain.
//   - connection  *ccon.ConnectionParams    connection parameters
//   - credential  *cauth.CredentialParams   credential parameters
// Returns: options *cconf.ConfigParams, err error
// resolved options or error.
func (c *NatsConnectionResolver) Compose(correlationId string, connections []*ccon.ConnectionParams, credential *cauth.CredentialParams) (*cconf.ConfigParams, error) {
	// Validate connections
	for _, connection := range connections {
		err := c.validateConnection(correlationId, connection)
		if err != nil {
			return nil, err
		}
	}

	options := c.composeOptions(connections, credential)
	return options, nil
}
