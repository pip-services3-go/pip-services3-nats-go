# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> NATS messaging components for Pip.Services in Go

This library is a part of [Pip.Services](https://github.com/pip-services/pip-services) project.
The Nats module contains a set of components for working with the message queue via NATS server [https://nats.io/](https://nats.io/).

The module contains the following packages:
- [**Build**](https://godoc.org/github.com/pip-services3-go/pip-services3-nats-go/build) - Factory for constructing module components
- [**Connect**](https://godoc.org/github.com/pip-services3-go/pip-services3-nats-go/connect) - Components for creating and configuring a connection with Nats
- [**Queues**](https://godoc.org/github.com/pip-services3-go/pip-services3-nats-go/queues) - Message Queuing components that implement the standard [Messaging](https://github.com/pip-services3-go/pip-services3-messaging-go) module interface

<a name="links"></a> Quick links:

* [Configuration](https://www.pipservices.org/recipies/configuration)
* [API Reference](https://godoc.org/github.com/pip-services3-go/pip-services3-nats-go/)
* [Change Log](CHANGELOG.md)
* [Get Help](https://www.pipservices.org/community/help)
* [Contribute](https://www.pipservices.org/community/contribute)

## Use

Get the package from the Github repository:
```bash
go get -u github.com/pip-services3-go/pip-services3-nats-go@latest
```

## Develop

For development you shall install the following prerequisites:
* Golang v1.12+
* Visual Studio Code or another IDE of your choice
* Docker
* Git

Run automated tests:
```bash
go test -v ./test/...
```

Generate API documentation:
```bash
./docgen.ps1
```

Before committing changes run dockerized test as:
```bash
./test.ps1
./clear.ps1
```

## Contacts

The Golang version of Pip.Services is created and maintained by:
- **Sergey Seroukhov**

The documentation is written by:
- **Levichev Dmitry**
