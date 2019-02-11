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
	connectTimeoutSeconds = 10
	reconnectDelaySeconds = 2

	stanQueueSize = 64
	subsQueueSize = 32
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

type actionType int

const (
	actionHandleStanDisconnect  actionType = iota
	actionHandleNatsReconnect   actionType = iota
	actionHandleServerDiscovery actionType = iota
	actionHandleSubscribedMsg   actionType = iota
	actionHandlePublishAsync    actionType = iota
)

type action struct {
	// type of action
	typ actionType

	// when actionType == actionHandleMsg
	msg *stan.Msg

	// when actionType == actionHandleServerDiscovery
	url string

	// when actionType == actionHandlePublishAsync
	subject string
	obj     interface{}
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

	// flags for connections
	natsConnected bool
	stanConnected bool
	shouldClose   bool

	toSubscribe []ToSubscribe

	// subscribed subscriptions (for closing later)
	subscribed []stan.Subscription

	// channels for stan client's action
	actionQueue     chan action
	quitActionQueue chan struct{}

	// channels for polling subscriptions
	subscriptionQueue     chan *stan.Msg
	quitSubscriptionQueue chan struct{}

	// handlers
	messageHandler             func(msg *stan.Msg)
	asyncPublishFailureHandler func(subject, nuid string, obj interface{})

	// logger
	logger Logger

	// mutex for locking
	sync.RWMutex
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

		actionQueue:     make(chan action, stanQueueSize),
		quitActionQueue: make(chan struct{}, 1),

		subscriptionQueue:     make(chan *stan.Msg, subsQueueSize),
		quitSubscriptionQueue: make(chan struct{}, 1),

		messageHandler:             messageHandler,
		asyncPublishFailureHandler: asyncPublishFailureHandler,

		logger: logger,
	}

	// poll from actions channel
	go sc.processActions()

	var err error
	for { // XXX - try infinitely
		if sc.natsConn != nil {
			sc.natsConn.Close()
			sc.natsConn = nil
		}
		sc.natsConnected = false

		// initialize connection to NATS,
		if err = sc.connectToNats(); err != nil {
			sc.logger.Error("failed to connect to nats: %s", err)

			// wait for some time
			time.Sleep(reconnectDelaySeconds * time.Second)
		} else {
			// and initialize connection to STAN,
			if err = sc.connectToStan(); err != nil {
				sc.logger.Error("failed to connect and subscribe to stan: %s", err)

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

func (sc *Client) processActions() {
	sc.logger.Log("starting action queue...")

	for {
		select {
		case act := <-sc.actionQueue:
			switch act.typ {
			case actionHandleSubscribedMsg:
				if sc.messageHandler != nil {
					sc.messageHandler(act.msg)
				} else {
					sc.logger.Error("message received but no handler set yet: %+v", act.msg)
				}
			case actionHandlePublishAsync:
				sc.onPublishAsync(act.subject, act.obj)
			case actionHandleServerDiscovery:
				sc.onServerDiscovery(act.url)
			case actionHandleStanDisconnect:
				sc.onStanDisconnect()
			case actionHandleNatsReconnect:
				sc.onNatsReconnect()
			default:
				sc.logger.Error("no matching type for action: %d", act.typ)
			}
		case <-sc.quitActionQueue:
			sc.logger.Log("stopping action queue...")
			break
		}
	}
}

// Poll waits for incoming messages from subscriptions, with lock on connections
func (sc *Client) Poll() {
	sc.logger.Log("start polling subscriptions")

loop:
	for {
		select {
		case message := <-sc.subscriptionQueue:
			sc.actionQueue <- action{
				typ: actionHandleSubscribedMsg,
				msg: message,
			}
		case <-sc.quitSubscriptionQueue:
			sc.logger.Log("stopping polling subscriptions...")
			break loop
		}
	}

	sc.logger.Log("polling finished")
}

// Publish publishes to STAN synchronously, with lock on connections
func (sc *Client) Publish(subject string, obj interface{}) (err error) {
	if sc.shouldClose {
		return fmt.Errorf("should close now")
	}

	if !sc.allConnected() {
		return fmt.Errorf("not connected to nats or stan")
	}

	var data []byte
	if data, err = json.Marshal(obj); err == nil {
		if sc.stanConn != nil {
			err = sc.stanConn.Publish(subject, data)
		} else {
			err = fmt.Errorf("stan connection is not setup")
		}
	} else {
		err = fmt.Errorf("data serialization failed for publish: %s", err)
	}

	if err != nil {
		sc.logger.Error("publish failed: %s", err)
	}

	return err
}

// PublishAsync publishes to STAN asynchronously, with lock on connections
func (sc *Client) PublishAsync(subject string, obj interface{}) (err error) {
	if sc.shouldClose {
		return fmt.Errorf("should close now")
	}

	if !sc.allConnected() {
		return fmt.Errorf("not connected to nats or stan")
	}

	sc.actionQueue <- action{
		typ:     actionHandlePublishAsync,
		subject: subject,
		obj:     obj,
	}

	return nil
}

func (sc *Client) onPublishAsync(subject string, obj interface{}) (nuid string, err error) {
	var data []byte
	if data, err = json.Marshal(obj); err == nil {
		if sc.stanConn != nil {
			if nuid, err = sc.stanConn.PublishAsync(subject, data, func(nuid string, err error) {
				if err != nil {
					if sc.asyncPublishFailureHandler != nil {
						// callback
						sc.asyncPublishFailureHandler(subject, nuid, obj)
					} else {
						sc.logger.Error("publish async failed (nuid: %s): %s", nuid, err)
					}
				}
			}); err != nil {
				err = fmt.Errorf("%s (nuid: %s)", err, nuid)
			}
		} else {
			err = fmt.Errorf("publish async failed: stan connection is not setup")
		}
	} else {
		err = fmt.Errorf("data serialization failed for publish: %s", err)
	}

	if err != nil {
		sc.logger.Error("publish async failed: %s", err)
	}

	return nuid, err
}

func (sc *Client) allConnected() bool {
	sc.RLock()
	defer sc.RUnlock()

	return sc.natsConnected && sc.stanConnected
}

func (sc *Client) markNatsConnected(connected bool) {
	sc.Lock()
	defer sc.Unlock()

	sc.natsConnected = connected
}

func (sc *Client) markStanConnected(connected bool) {
	sc.Lock()
	defer sc.Unlock()

	sc.stanConnected = connected
}

// Close closes connections to NATS and STAN servers with lock on connections
// (Client should not be re-used after calling Close())
func (sc *Client) Close() {
	sc.logger.Log("closing client...")

	// for stopping infinite-loops of reconnection
	sc.shouldClose = true

	// stop polling
	if sc.quitActionQueue != nil {
		sc.quitActionQueue <- struct{}{}
	}
	if sc.quitSubscriptionQueue != nil {
		sc.quitSubscriptionQueue <- struct{}{}
	}

	// close STAN's connection
	if sc.stanConn != nil {
		sc.logger.Log("closing stan connection...")

		sc.stanConn.Close()
		sc.stanConn = nil
	}

	sc.markStanConnected(false)

	// finish NATS' remaining jobs
	if sc.natsConn != nil {
		sc.logger.Log("closing nats connection...")

		sc.natsConn.Drain()
		sc.natsConn.Close()
		sc.natsConn = nil
	}

	sc.markNatsConnected(false)

	// close channels
	if sc.subscriptionQueue != nil {
		close(sc.subscriptionQueue)
		sc.subscriptionQueue = nil
	}
	if sc.quitSubscriptionQueue != nil {
		close(sc.quitSubscriptionQueue)
		sc.quitSubscriptionQueue = nil
	}
}

// called when NATS disconnects, with lock on connections
func (sc *Client) handleNatsDisconnection(nc *nats.Conn) {
	sc.logger.Error("disconnected from nats")

	// XXX - will recover connection automatically (nats.MaxReconnects(-1))
}

// called when NATS connection is closed (due to error, or intentionally)
func (sc *Client) handleNatsClosed(nc *nats.Conn) {
	err := nc.LastError()

	if err != nil {
		sc.logger.Error("connection to nats closed with error: %s", err)
	} else {
		sc.logger.Error("connection to nats closed")
	}
}

// called when NATS recovers connection, with lock on connections
func (sc *Client) handleNatsReconnection(nc *nats.Conn) {
	sc.logger.Error("reconnected to nats: %s", nc.ConnectedUrl())

	sc.actionQueue <- action{
		typ: actionHandleNatsReconnect,
	}
}

func (sc *Client) onNatsReconnect() {
	// disconnect from STAN,
	if sc.stanConn != nil {
		sc.logger.Error("closing stan connection before reconnectiong to nats")

		// XXX - unsubscribe (without doing this manually, goroutines may leak...)
		for _, subscription := range sc.subscribed {
			subscription.Close()
		}

		sc.stanConn.Close()
		sc.stanConn = nil
	}

	sc.markStanConnected(false)

	sc.logger.Error("reconnecting to stan...")

	// then reconnect to STAN,
	for { // XXX - try infinitely
		if sc.shouldClose {
			sc.logger.Error("exiting loop for reconnection")
			return
		}

		if err := sc.connectToStan(); err == nil {
			sc.logger.Error("reconnected to stan")

			break
		} else {
			sc.logger.Error("failed to reconnect to stan: %s", err)

			time.Sleep(reconnectDelaySeconds * time.Second)
		}
	}

	// and resubscribe
	sc.subscribe()
}

// called when a new server is discovered, with lock on server urls
func (sc *Client) handleNatsDiscoveredServer(nc *nats.Conn) {
	sc.logger.Log("discovered a new nats server: %s", nc.ConnectedUrl())

	sc.actionQueue <- action{
		typ: actionHandleServerDiscovery,
		url: nc.ConnectedUrl(),
	}
}

func (sc *Client) onServerDiscovery(newURL string) {
	// check if duplicated,
	exists := false
	for _, url := range sc.natsServers {
		if url == newURL {
			exists = true
			break
		}
	}

	// and append the newly discovered server's url
	if !exists {
		sc.natsServers = append(sc.natsServers, newURL)
	}
}

// called when STAN disconnects, with lock on connection
func (sc *Client) handleStanDisconnection(conn stan.Conn, err error) {
	if err != nil {
		sc.logger.Error("connection to stan closed with error: %s", err)
	} else {
		sc.logger.Error("connection to stan closed")
	}

	sc.actionQueue <- action{
		typ: actionHandleStanDisconnect,
	}
}

func (sc *Client) onStanDisconnect() {
	var err error

	// disconnect from STAN,
	if sc.stanConn != nil {
		// XXX - unsubscribe (without doing this manually, goroutines leak...)
		for _, subscription := range sc.subscribed {
			subscription.Close()
		}

		sc.stanConn.Close()
		sc.stanConn = nil
	}

	sc.markStanConnected(false)

	sc.logger.Error("reestablishing connection to nats...")

	for { // XXX - try infinitely
		if sc.shouldClose {
			sc.logger.Error("exiting loop for reconnection")

			return
		}

		if sc.natsConn != nil {
			sc.logger.Error("resetting nats connection")

			sc.natsConn.Close()
			sc.natsConn = nil
		}

		sc.markNatsConnected(false)

		if err = sc.connectToNats(); err != nil {
			// wait for some time
			time.Sleep(reconnectDelaySeconds * time.Second)
		} else {
			sc.logger.Error("reconnecting to stan...")

			// reconnect to STAN,
			if err = sc.connectToStan(); err == nil {
				sc.logger.Error("reconnected to stan")

				break
			} else {
				sc.logger.Error("failed to reconnect to stan: %s", err)

				time.Sleep(reconnectDelaySeconds * time.Second)
			}
		}
	}

	sc.logger.Error("resubscribing to stan")

	// and resubscribe
	sc.subscribe()
}

// establish connection to NATS server, with lock on server urls
func (sc *Client) connectToNats() (err error) {
	sc.logger.Log("connecting to nats...")

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

	// connect with options,
	sc.natsConn, err = nats.Connect(
		strings.Join(sc.natsServers, ", "), // join server urls
		options...,
	)

	if err != nil {
		if sc.natsConn != nil {
			sc.logger.Error("closing errorneous nats connection")

			sc.natsConn.Close()
			sc.natsConn = nil
		}

		return err
	}

	sc.markNatsConnected(true)

	return nil
}

// establish connection to STAN server
func (sc *Client) connectToStan() (err error) {
	sc.logger.Log("connecting to stan")

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
				sc.logger.Error("closing errorneous nats connection: %s", err)

				sc.natsConn.Close()
				sc.natsConn = nil
			}

			sc.markNatsConnected(false)
		}

		if sc.stanConn != nil {
			sc.logger.Error("closing errorneous stan connection")

			sc.stanConn.Close()
			sc.stanConn = nil
		}

		sc.markStanConnected(false)

		return err
	}

	sc.markStanConnected(true)

	return nil
}

// subscribe to subjects
func (sc *Client) subscribe() {
	sc.logger.Log("subscribing to stan")

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
				sc.subscriptionQueue <- msg
			}, options...); err != nil {
				sc.logger.Error("failed to subscribe to %s: %s", subscribe.Subject, err)
			} else {
				sc.subscribed = append(sc.subscribed, subscription)
			}
		} else {
			if subscription, err := sc.stanConn.QueueSubscribe(subscribe.Subject, subscribe.QueueGroupName, func(msg *stan.Msg) {
				sc.subscriptionQueue <- msg
			}, options...); err != nil {
				sc.logger.Error("failed to subscribe to %s: %s", subscribe.Subject, err)
			} else {
				sc.subscribed = append(sc.subscribed, subscription)
			}
		}
	}

	sc.logger.Log("subscribed to %d subscription(s)", len(sc.subscribed))
}
