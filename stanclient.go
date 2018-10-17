package stanclient

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
)

const (
	connectTimeoutSeconds = 5
	reconnectDelaySeconds = 5

	channelSize = 32
)

// Logger interface for logging
type Logger interface {
	Log(log string)
	Error(err string)
}

// ToSubscribe struct for subscription request
type ToSubscribe struct {
	Subject string

	QueueGroupName string
	DurableName    string

	DeliverAll bool
}

// Client for publish/subscribe with STAN servers
type Client struct {
	// values for NATS connection
	natsServers        []string
	natsClientUsername string
	natsClientPasswd   string
	natsClientCertPath string
	natsClientKeyPath  string
	natsRootCaPath     string

	// values for STAN connection
	stanClusterID string
	stanClientID  string

	// connections
	natsConn *nats.Conn
	stanConn stan.Conn

	// sync lock for connections
	connLock sync.Mutex

	// flags for connections
	natsConnected bool
	stanConnected bool

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
func Connect(
	natsServers []string,
	natsClientUsername,
	natsClientPasswd,
	natsClientCertPath,
	natsClientKeyPath,
	natsRootCaPath,
	stanClusterID,
	stanClientID string,
	subscriptions []ToSubscribe,
	messageHandler func(msg *stan.Msg),
	asyncPublishFailureHandler func(subject, nuid string, obj interface{}),
	logger Logger,
) *Client {
	sc := &Client{
		natsServers:        natsServers,
		natsClientUsername: natsClientUsername,
		natsClientPasswd:   natsClientPasswd,
		natsClientCertPath: natsClientCertPath,
		natsClientKeyPath:  natsClientKeyPath,
		natsRootCaPath:     natsRootCaPath,

		stanClusterID: stanClusterID,
		stanClientID:  stanClientID,

		toSubscribe: subscriptions,
		subscribed:  []stan.Subscription{},

		messageHandler:             messageHandler,
		asyncPublishFailureHandler: asyncPublishFailureHandler,

		logger: logger,
	}

	var err error

	// initialize connection to NATS,
	for { // XXX - try infinitely
		if sc.natsConn, err = sc.connectToNats(true); err != nil {
			sc.logger.Error(fmt.Sprintf("Failed to connect to NATS: %s", err))

			// wait for some time
			time.Sleep(reconnectDelaySeconds * time.Second)
		} else {
			sc.natsConnected = true

			break
		}
	}

	// and initialize connection to STAN,
	for { // XXX - try infinitely
		if sc.stanConn, err = sc.connectToStan(true); err != nil {
			sc.logger.Error(fmt.Sprintf("Failed to connect and subscribe to STAN: %s", err))

			// wait for some time
			time.Sleep(reconnectDelaySeconds * time.Second)
		} else {
			sc.stanConnected = true

			sc.subscribe(true)

			break
		}
	}

	return sc
}

// Poll waits for incoming messages from subscriptions
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
				sc.logger.Error(fmt.Sprintf("Message received but no handler set yet: %+v", message))
			}
		case <-sc.chanQuitLoop:
			break loop
		}
	}

	sc.logger.Log("Polling finished")
}

// Publish publishes to STAN synchronously
func (sc *Client) Publish(subject string, obj interface{}) (err error) {
	sc.connLock.Lock()
	defer sc.connLock.Unlock()

	if sc.stanConn == nil || !sc.stanConnected {
		err = fmt.Errorf("Connection to STAN is incomplete")
		sc.logger.Error(err.Error())
		return err
	}

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

// PublishAsync publishes to STAN asynchronously
func (sc *Client) PublishAsync(subject string, obj interface{}) (nuid string, err error) {
	sc.connLock.Lock()
	defer sc.connLock.Unlock()

	if sc.stanConn == nil || !sc.stanConnected {
		err = fmt.Errorf("Connection to STAN is incomplete")
		sc.logger.Error(err.Error())
		return "", err
	}

	var data []byte
	if data, err = json.Marshal(obj); err == nil {
		if nuid, err = sc.stanConn.PublishAsync(subject, data, func(nuid string, err error) {
			if err != nil {
				if sc.asyncPublishFailureHandler != nil {
					// callback
					sc.asyncPublishFailureHandler(nuid, subject, obj)
				} else {
					sc.logger.Error(fmt.Sprintf("Failed to publish (%s) asynchronously: %s", nuid, err))
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

// Close closes connections to NATS and STAN servers
func (sc *Client) Close() {
	sc.connLock.Lock()

	// stop polling
	if sc.chanQuitLoop != nil {
		sc.chanQuitLoop <- struct{}{}
	}

	// close STAN's connection
	if sc.stanConn != nil {
		sc.stanConn.Close()
	}
	sc.stanConnected = false

	// finish NATS' remaining jobs
	if sc.natsConn != nil {
		sc.natsConn.Drain()
		sc.natsConn.Close()
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

	sc.connLock.Unlock()
}

// called when NATS disconnects
func (sc *Client) handleNatsDisconnection(nc *nats.Conn) {
	sc.logger.Error("Disconnected from NATS")

	sc.connLock.Lock()
	defer sc.connLock.Unlock()

	sc.natsConnected = false

	// XXX - will recover connection automatically (nats.MaxReconnects(-1))
}

// called when NATS connection is closed (due to error, or intentionally)
func (sc *Client) handleNatsClosed(nc *nats.Conn) {
	err := nc.LastError()

	if err != nil {
		sc.logger.Error(fmt.Sprintf("Connection to NATS closed with error: %s", err))
	} else {
		sc.logger.Error("Connection to NATS closed")
	}
}

// called when NATS recovers connection
func (sc *Client) handleNatsReconnection(nc *nats.Conn) {
	sc.logger.Error(fmt.Sprintf("Reconnected to NATS: %s", nc.ConnectedUrl()))

	sc.connLock.Lock()

	// disconnect from STAN,
	if sc.stanConn != nil {
		// XXX - unsubscribe (without doing this manually, goroutines leak...)
		for _, subscription := range sc.subscribed {
			subscription.Close()
		}

		sc.stanConn.Close()
	}

	sc.logger.Error("Reconnecting to STAN...")

	// reconnect to STAN,
	var err error
	for { // XXX - try infinitely
		sc.stanConn, err = sc.connectToStan(false)

		if err == nil {
			sc.logger.Error("Reconnected to STAN")

			sc.stanConnected = true

			break
		} else {
			sc.logger.Error(fmt.Sprintf("Failed to reconnect to STAN: %s", err))

			time.Sleep(reconnectDelaySeconds * time.Second)
		}
	}

	// and resubscribe
	sc.logger.Error("Resubscribing to STAN")

	sc.subscribe(false)

	sc.connLock.Unlock()
}

// called when STAN disconnects
func (sc *Client) handleStanDisconnection(conn stan.Conn, err error) {
	if err != nil {
		sc.logger.Error(fmt.Sprintf("Connection to STAN closed with error: %s", err))
	} else {
		sc.logger.Error("Connection to STAN closed")
	}

	sc.connLock.Lock()

	// disconnect from STAN,
	if sc.stanConn != nil {
		// XXX - unsubscribe (without doing this manually, goroutines leak...)
		for _, subscription := range sc.subscribed {
			subscription.Close()
		}

		sc.stanConn.Close()
	}

	sc.stanConnected = false

	if !sc.natsConnected {
		// ????? needed?
		if sc.natsConn != nil {
			sc.natsConn.Close()
		}

		sc.logger.Error("Reconnecting to NATS...")

		for { // XXX - try infinitely
			if sc.natsConn, err = sc.connectToNats(false); err != nil {
				// wait for some time
				time.Sleep(reconnectDelaySeconds * time.Second)
			} else {
				sc.natsConnected = true

				break
			}
		}
	}

	sc.logger.Error("Reconnecting to STAN...")

	// reconnect to STAN,
	for { // XXX - try infinitely
		sc.stanConn, err = sc.connectToStan(false)

		if err == nil {
			sc.logger.Error("Reconnected to STAN")

			sc.stanConnected = true

			break
		} else {
			sc.logger.Error(fmt.Sprintf("Failed to reconnect to STAN: %s", err))

			time.Sleep(reconnectDelaySeconds * time.Second)
		}
	}

	// and resubscribe
	sc.logger.Error("Resubscribing to STAN")

	sc.subscribe(false)

	sc.connLock.Unlock()
}

// establish connection to NATS server
func (sc *Client) connectToNats(withLock bool) (*nats.Conn, error) {
	sc.logger.Log("Connecting to NATS")

	if withLock {
		sc.connLock.Lock()
		defer sc.connLock.Unlock()
	}

	conn, err := nats.Connect(
		strings.Join(sc.natsServers, ", "),
		nats.UserInfo(sc.natsClientUsername, sc.natsClientPasswd),
		nats.ClientCert(sc.natsClientCertPath, sc.natsClientKeyPath),
		nats.RootCAs(sc.natsRootCaPath),
		nats.ReconnectWait(reconnectDelaySeconds*time.Second),
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
	)

	if err != nil {
		if conn != nil {
			conn.Close()
		}

		return nil, err
	}

	sc.natsConnected = true

	return conn, nil
}

// establish connection to STAN server
func (sc *Client) connectToStan(withLock bool) (stan.Conn, error) {
	sc.logger.Log("Connecting to STAN")

	if withLock {
		sc.connLock.Lock()
		defer sc.connLock.Unlock()
	}

	conn, err := stan.Connect(
		sc.stanClusterID,
		sc.stanClientID,
		stan.ConnectWait(connectTimeoutSeconds*time.Second),
		stan.NatsConn(sc.natsConn),
		stan.SetConnectionLostHandler(sc.handleStanDisconnection),
	)

	if err != nil {
		if conn != nil {
			conn.Close()
		}

		return nil, err
	}

	sc.stanConnected = true

	return conn, nil
}

func (sc *Client) subscribe(withLock bool) {
	sc.logger.Log("Subscribing to STAN")

	if withLock {
		sc.connLock.Lock()
		defer sc.connLock.Unlock()
	}

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
				sc.logger.Error(fmt.Sprintf("Failed to subscribe to %s: %s", subscribe.Subject, err))
			} else {
				sc.subscribed = append(sc.subscribed, subscription)
			}
		} else {
			if subscription, err := sc.stanConn.QueueSubscribe(subscribe.Subject, subscribe.QueueGroupName, func(msg *stan.Msg) {
				sc.chanSubscription <- msg
			}, options...); err != nil {
				sc.logger.Error(fmt.Sprintf("Failed to subscribe to %s: %s", subscribe.Subject, err))
			} else {
				sc.subscribed = append(sc.subscribed, subscription)
			}
		}
	}

	sc.logger.Log(fmt.Sprintf("Subscribed to %d subscription(s)", len(sc.subscribed)))
}
