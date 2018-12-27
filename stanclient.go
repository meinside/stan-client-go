package stanclient

// STAN client wrapper

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	nats "github.com/nats-io/go-nats"
	stan "github.com/nats-io/go-nats-streaming"
)

// Constants
const (
	connectTimeoutSeconds = 5
	reconnectDelaySeconds = 5

	channelSize = 32
)

// Logger interface for logging
type Logger interface {
	Log(format string, args ...interface{})
	Error(format string, args ...interface{})
}

// ToSubscribe struct for subscription request
type ToSubscribe struct {
	Subject string

	QueueGroupName string
	DurableName    string

	DeliverAll bool
}

// AuthOption for authentication
type AuthOption struct {
	Username               string
	Password               string
	hasUsernameAndPassword bool

	Token    string
	hasToken bool
}

// AuthOptionWithUsernameAndPassword returns a pointer to AuthOption with username and password
func AuthOptionWithUsernameAndPassword(username, password string) *AuthOption {
	return &AuthOption{
		Username:               username,
		Password:               password,
		hasUsernameAndPassword: true,
	}
}

// AuthOptionWithToken returns a pointer to AuthOption with token
func AuthOptionWithToken(token string) *AuthOption {
	return &AuthOption{
		Token:    token,
		hasToken: true,
	}
}

// SecOption for security (TLS)
type SecOption struct {
	ClientCertPath string
	ClientKeyPath  string
	RootCaPath     string
}

// SecOptionWithCerts returns a pointer to SecOption with certification files' locations
func SecOptionWithCerts(clientCertPath, clientKeyPath, rootCaPath string) *SecOption {
	return &SecOption{
		ClientCertPath: clientCertPath,
		ClientKeyPath:  clientKeyPath,
		RootCaPath:     rootCaPath,
	}
}

// Client for publish/subscribe with STAN servers
type Client struct {
	// values for NATS connection
	natsServers    []string
	natsAuthOption *AuthOption
	natsSecOption  *SecOption

	// values for STAN connection
	stanClusterID string
	stanClientID  string

	// connections
	natsConn *nats.Conn
	stanConn stan.Conn

	// sync locks
	connLock    sync.Mutex
	serversLock sync.Mutex

	// flags for connections
	natsConnected bool
	stanConnected bool
	shouldStopAll bool

	toSubscribe []ToSubscribe

	// subscribed subscriptions (for closing later)
	subscribed []stan.Subscription

	// channels for polling subscriptions
	chanSubscription chan *stan.Msg
	chanQuitLoop     chan struct{}

	// handlers
	messageHandler             func(msg *stan.Msg)
	asyncPublishFailureHandler func(subject, nuid string, obj interface{})

	// logger
	logger Logger
}

// Connect establishes connection to stan servers
//
// natsServers: array of NATS URLs
// natsAuthOption: nil if none
// natsSecOption: nil if none
// stanClusterID: cluster ID of STAN
// stanClientID: client ID of STAN
// subscriptions: array of subscriptions to subscribe
// messageHandler: callback function for message handling
// asyncPublishFailureHandler: callback function for handling failures of PublishAsync function
// logger: logger interface for logging
func Connect(
	natsServers []string,
	natsAuthOption *AuthOption,
	natsSecOption *SecOption,
	stanClusterID,
	stanClientID string,
	subscriptions []ToSubscribe,
	messageHandler func(msg *stan.Msg),
	asyncPublishFailureHandler func(subject, nuid string, obj interface{}),
	logger Logger,
) *Client {
	sc := &Client{
		natsServers:    natsServers,
		natsAuthOption: natsAuthOption,
		natsSecOption:  natsSecOption,

		stanClusterID: stanClusterID,
		stanClientID:  stanClientID,

		toSubscribe: subscriptions,
		subscribed:  []stan.Subscription{},

		messageHandler:             messageHandler,
		asyncPublishFailureHandler: asyncPublishFailureHandler,

		logger: logger,
	}

	var err error
	for { // XXX - try infinitely
		if sc.natsConn != nil {
			sc.natsConn.Close()
			sc.natsConn = nil
		}
		sc.natsConnected = false

		// initialize connection to NATS,
		if err = sc.connectToNats(); err != nil {
			sc.logger.Error("Failed to connect to NATS: %s", err)

			// wait for some time
			time.Sleep(reconnectDelaySeconds * time.Second)
		} else {
			// and initialize connection to STAN,
			if err = sc.connectToStan(); err != nil {
				sc.logger.Error("Failed to connect and subscribe to STAN: %s", err)

				// wait for some time
				time.Sleep(reconnectDelaySeconds * time.Second)
			} else {
				// then subscribe
				sc.subscribe()

				break
			}
		}
	}

	return sc
}

// Poll waits for incoming messages from subscriptions, with lock on connections
func (sc *Client) Poll() {
	sc.logger.Log("Start polling subscriptions")

	sc.connLock.Lock()

	sc.chanSubscription = make(chan *stan.Msg, channelSize)
	sc.chanQuitLoop = make(chan struct{}, 1)

	sc.connLock.Unlock()

loop:
	for {
		select {
		case message := <-sc.chanSubscription:
			if sc.messageHandler != nil {
				go sc.messageHandler(message)
			} else {
				sc.logger.Error("Message received but no handler set yet: %+v", message)
			}
		case <-sc.chanQuitLoop:
			break loop
		}
	}

	sc.logger.Log("Polling finished")
}

// Publish publishes to STAN synchronously, with lock on connections
func (sc *Client) Publish(subject string, obj interface{}) (err error) {
	if sc.shouldStopAll {
		return fmt.Errorf("cannot publish, should stop now")
	}

	sc.connLock.Lock()
	defer sc.connLock.Unlock()

	var data []byte
	if data, err = json.Marshal(obj); err == nil {
		if err = sc.stanConn.Publish(subject, data); err != nil {
			err = fmt.Errorf("Failed to publish: %s", err)
		}
	} else {
		err = fmt.Errorf("Failed to serialize data for publish: %s", err)
	}

	if err != nil {
		sc.logger.Error(err.Error())
	}

	return err
}

// PublishAsync publishes to STAN asynchronously, with lock on connections
func (sc *Client) PublishAsync(subject string, obj interface{}) (nuid string, err error) {
	if sc.shouldStopAll {
		return "", fmt.Errorf("cannot publish, should stop now")
	}

	sc.connLock.Lock()
	defer sc.connLock.Unlock()

	var data []byte
	if data, err = json.Marshal(obj); err == nil {
		if nuid, err = sc.stanConn.PublishAsync(subject, data, func(nuid string, err error) {
			if err != nil {
				if sc.asyncPublishFailureHandler != nil {
					// callback
					sc.asyncPublishFailureHandler(subject, nuid, obj)
				} else {
					sc.logger.Error("Failed to publish (%s) asynchronously: %s", nuid, err)
				}
			}
		}); err != nil {
			err = fmt.Errorf("Failed to publish (%s): %s", nuid, err)
		}
	} else {
		err = fmt.Errorf("Failed to serialize data for publish: %s", err)
	}

	if err != nil {
		sc.logger.Error(err.Error())
	}

	return nuid, err
}

// Close closes connections to NATS and STAN servers with lock on connections
func (sc *Client) Close() {
	sc.logger.Log("Closing client...")

	// for stopping infinite-loops of reconnection
	sc.shouldStopAll = true

	sc.connLock.Lock()
	defer sc.connLock.Unlock()

	// stop polling
	if sc.chanQuitLoop != nil {
		sc.chanQuitLoop <- struct{}{}
	}

	// close STAN's connection
	if sc.stanConn != nil {
		sc.logger.Log("Closing STAN connection...")

		sc.stanConn.Close()
		sc.stanConn = nil
	}
	sc.stanConnected = false

	// finish NATS' remaining jobs
	if sc.natsConn != nil {
		sc.logger.Log("Closing NATS connection...")

		sc.natsConn.Drain()
		sc.natsConn.Close()
		sc.natsConn = nil
	}
	sc.natsConnected = false

	// close channels
	if sc.chanSubscription != nil {
		close(sc.chanSubscription)
		sc.chanSubscription = nil
	}
	if sc.chanQuitLoop != nil {
		close(sc.chanQuitLoop)
		sc.chanQuitLoop = nil
	}
}

// called when NATS disconnects, with lock on connections
func (sc *Client) handleNatsDisconnection(nc *nats.Conn) {
	sc.logger.Error("Handling NATS disconnection: disconnected from NATS")

	// XXX - will recover connection automatically (nats.MaxReconnects(-1))
}

// called when NATS connection is closed (due to error, or intentionally)
func (sc *Client) handleNatsClosed(nc *nats.Conn) {
	err := nc.LastError()

	if err != nil {
		sc.logger.Error("Handling NATS close: connection to NATS closed with error: %s", err)
	} else {
		sc.logger.Error("Handling NATS close: connection to NATS closed")
	}
}

// called when NATS recovers connection, with lock on connections
func (sc *Client) handleNatsReconnection(nc *nats.Conn) {
	sc.logger.Error("Handling NATS reconnection: reconnected to NATS: %s", nc.ConnectedUrl())

	sc.connLock.Lock()
	defer sc.connLock.Unlock()

	// disconnect from STAN,
	if sc.stanConn != nil {
		sc.logger.Error("Handling NATS reconnection: closing STAN connection...")

		// XXX - unsubscribe (without doing this manually, goroutines leak...)
		for _, subscription := range sc.subscribed {
			subscription.Close()
		}

		sc.stanConn.Close()
		sc.stanConn = nil
	}
	sc.stanConnected = false

	sc.logger.Error("Handling NATS reconnection: reconnecting to STAN...")

	// then reconnect to STAN,
	for { // XXX - try infinitely
		if sc.shouldStopAll {
			sc.logger.Error("Handling NATS reconnection: exiting loop for reconnection")
			return
		}

		if err := sc.connectToStan(); err == nil {
			sc.logger.Error("Handling NATS reconnection: reconnected to STAN")

			break
		} else {
			sc.logger.Error("Handling NATS reconnection: failed to reconnect to STAN: %s", err)

			time.Sleep(reconnectDelaySeconds * time.Second)
		}
	}

	sc.logger.Error("Handling NATS reconnection: resubscribing to STAN")

	// and resubscribe
	sc.subscribe()
}

// called when a new server is discovered, with lock on server urls
func (sc *Client) handleNatsDiscoveredServer(nc *nats.Conn) {
	sc.logger.Log("Handling NATS server discovery: discovered a new NATS server: %s", nc.ConnectedUrl())

	sc.serversLock.Lock()

	// append the newly discovered server's url
	sc.natsServers = append(sc.natsServers, nc.ConnectedUrl())

	sc.serversLock.Unlock()
}

// called when STAN disconnects, with lock on connection
func (sc *Client) handleStanDisconnection(conn stan.Conn, err error) {
	if err != nil {
		sc.logger.Error("Handling STAN disconnection: connection to STAN closed with error: %s", err)
	} else {
		sc.logger.Error("Handling STAN disconnection: connection to STAN closed")
	}

	sc.connLock.Lock()
	defer sc.connLock.Unlock()

	// disconnect from STAN,
	if sc.stanConn != nil {
		// XXX - unsubscribe (without doing this manually, goroutines leak...)
		for _, subscription := range sc.subscribed {
			subscription.Close()
		}

		sc.stanConn.Close()
		sc.stanConn = nil
	}
	sc.stanConnected = false

	sc.logger.Error("Handling STAN disconnection: reestablishing connection to NATS...")

	for { // XXX - try infinitely
		if sc.shouldStopAll {
			sc.logger.Error("Handling STAN disconnection: exiting loop for reconnection")

			return
		}

		if sc.natsConn != nil {
			sc.logger.Error("Handling STAN disconnection: resetting NATS connection")

			sc.natsConn.Close()
			sc.natsConn = nil
		}
		sc.natsConnected = false

		if err = sc.connectToNats(); err != nil {
			// wait for some time
			time.Sleep(reconnectDelaySeconds * time.Second)
		} else {
			sc.logger.Error("Handling STAN disconnection: reconnecting to STAN...")

			// reconnect to STAN,
			if err = sc.connectToStan(); err == nil {
				sc.logger.Error("Handling STAN disconnection: reconnected to STAN")

				break
			} else {
				sc.logger.Error("Handling STAN disconnection: failed to reconnect to STAN: %s", err)

				time.Sleep(reconnectDelaySeconds * time.Second)
			}
		}
	}

	sc.logger.Error("Handling STAN disconnection: resubscribing to STAN")

	// and resubscribe
	sc.subscribe()
}

// establish connection to NATS server, with lock on server urls
func (sc *Client) connectToNats() (err error) {
	sc.logger.Log("Connecting to NATS")

	// options for connections
	options := []nats.Option{
		nats.ReconnectWait(reconnectDelaySeconds * time.Second),
		nats.MaxReconnects(-1), // try reconnecting infinitely
		nats.DisconnectHandler(func(nc *nats.Conn) {
			sc.handleNatsDisconnection(nc)
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			sc.handleNatsClosed(nc)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			sc.handleNatsReconnection(nc)
		}),
		nats.DiscoveredServersHandler(func(nc *nats.Conn) {
			sc.handleNatsDiscoveredServer(nc)
		}),
	}

	// options for authentication (optional)
	if sc.natsAuthOption != nil {
		if sc.natsAuthOption.hasUsernameAndPassword {
			options = append(options, nats.UserInfo(sc.natsAuthOption.Username, sc.natsAuthOption.Password))
		} else if sc.natsAuthOption.hasToken {
			options = append(options, nats.Token(sc.natsAuthOption.Token))
		}
	}

	// options for TLS security (optional)
	if sc.natsSecOption != nil {
		options = append(options, nats.ClientCert(sc.natsSecOption.ClientCertPath, sc.natsSecOption.ClientKeyPath))
		options = append(options, nats.RootCAs(sc.natsSecOption.RootCaPath))
	}

	// join server urls
	sc.serversLock.Lock()
	serversURL := strings.Join(sc.natsServers, ", ")
	sc.serversLock.Unlock()

	// connect with options,
	sc.natsConn, err = nats.Connect(
		serversURL,
		options...,
	)

	if err != nil {
		if sc.natsConn != nil {
			sc.logger.Error("Closing errorneous NATS connection")

			sc.natsConn.Close()
			sc.natsConn = nil
		}

		return err
	}

	sc.natsConnected = true

	return nil
}

// establish connection to STAN server
func (sc *Client) connectToStan() (err error) {
	sc.logger.Log("Connecting to STAN")

	sc.stanConn, err = stan.Connect(
		sc.stanClusterID,
		sc.stanClientID,
		stan.ConnectWait(connectTimeoutSeconds*time.Second),
		stan.NatsConn(sc.natsConn),
		stan.SetConnectionLostHandler(sc.handleStanDisconnection),
	)

	if err != nil {
		// if connection to NATS is incomplete,
		if err == stan.ErrBadConnection {
			if sc.natsConn != nil {
				sc.logger.Error("Closing errorneous NATS connection: %s", err)

				sc.natsConn.Close()
				sc.natsConn = nil
			}
			sc.natsConnected = false
		}

		if sc.stanConn != nil {
			sc.logger.Error("Closing errorneous STAN connection")

			sc.stanConn.Close()
			sc.stanConn = nil
		}
		sc.stanConnected = false

		return err
	}

	sc.stanConnected = true

	return nil
}

// subscribe to subjects
func (sc *Client) subscribe() {
	sc.logger.Log("Subscribing to STAN")

	sc.subscribed = []stan.Subscription{}

	for _, subscribe := range sc.toSubscribe {
		options := []stan.SubscriptionOption{}

		// subscribe options
		if subscribe.DeliverAll {
			options = append(options, stan.DeliverAllAvailable())
		}
		if subscribe.DurableName != "" {
			options = append(options, stan.DurableName(subscribe.DurableName))
		}
		// TODO - handle more subscription options here

		if subscribe.QueueGroupName == "" {
			if subscription, err := sc.stanConn.Subscribe(subscribe.Subject, func(msg *stan.Msg) {
				sc.chanSubscription <- msg
			}, options...); err != nil {
				sc.logger.Error("Failed to subscribe to %s: %s", subscribe.Subject, err)
			} else {
				sc.subscribed = append(sc.subscribed, subscription)
			}
		} else {
			if subscription, err := sc.stanConn.QueueSubscribe(subscribe.Subject, subscribe.QueueGroupName, func(msg *stan.Msg) {
				sc.chanSubscription <- msg
			}, options...); err != nil {
				sc.logger.Error("Failed to subscribe to %s: %s", subscribe.Subject, err)
			} else {
				sc.subscribed = append(sc.subscribed, subscription)
			}
		}
	}

	sc.logger.Log("Subscribed to %d subscription(s)", len(sc.subscribed))
}
