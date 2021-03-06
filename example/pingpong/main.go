package main

// Run stan server with:
//
// $ /path/to/nats-streaming-server -sc stan.conf -c stan.conf -D -user USER -pass PASSWORD

import (
	"encoding/json"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"time"

	stanclient "github.com/meinside/stan-client-go"
	stan "github.com/nats-io/stan.go"
)

const (
	delaySeconds = 5

	queueGroupUnique = "unique"
	durableDefault   = "durable"

	natsServerURL = "nats://localhost:4222"

	clientCertPath = "./certs/cert.pem"
	clientKeyPath  = "./certs/key.pem"
	rootCaPath     = "./certs/ca.pem"

	natsUsername = "USER"
	natsPassword = "PASSWORD"

	clusterID = "stan"
	clientID  = "pingpong-client"

	subjectPing = "ping"
	subjectPong = "pong"
)

type pingPong struct {
	Message string `json:"message"`
}

type logger struct {
}

func (l *logger) Log(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (l *logger) Error(format string, args ...interface{}) {
	log.Printf("ERROR: "+format, args...)
}

var sc *stanclient.Client

func main() {
	// for monitoring
	go func() {
		log.Printf("Visit http://localhost:8888/debug/pprof for profiling...")

		panic(http.ListenAndServe("localhost:8888", nil))
	}()

	// for catching signals
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	sc = stanclient.Connect(
		[]string{natsServerURL},
		stanclient.AuthOptionWithUsernameAndPassword(natsUsername, natsPassword),
		stanclient.SecOptionWithCerts(clientCertPath, clientKeyPath, rootCaPath),
		clusterID,
		clientID,
		[]stanclient.ToSubscribe{
			stanclient.ToSubscribe{
				Subject:        subjectPing,
				QueueGroupName: queueGroupUnique,
				DurableName:    durableDefault,
				DeliverAll:     true,
			},
			stanclient.ToSubscribe{
				Subject:        subjectPong,
				QueueGroupName: queueGroupUnique,
				DurableName:    durableDefault,
				DeliverAll:     true,
			},
		},
		handlePingPong,
		publishFailed,
		&logger{},
	)

	go func() {
		time.Sleep(delaySeconds * time.Second)

		sc.Publish(subjectPing, pingPong{Message: "initial ping"})
	}()

	go sc.Poll()

	// wait...
loop:
	for {
		select {
		case <-interrupt:
			break loop
		}
	}

	sc.Close()

	log.Println("Application terminating...")
}

func handlePingPong(message *stan.Msg) {
	var data pingPong
	err := json.Unmarshal(message.Data, &data)

	if err != nil {
		log.Printf("Failed to unmarshal data: %s", err)
		return
	}

	switch message.Subject {
	case subjectPing:
		log.Printf("Received PING: %s", data.Message)

		time.Sleep(delaySeconds * time.Second)
		sc.Publish(subjectPong, pingPong{Message: "pong for ping"})
	case subjectPong:
		log.Printf("Received PONG: %s", data.Message)

		time.Sleep(delaySeconds * time.Second)
		sc.Publish(subjectPing, pingPong{Message: "ping for pong"})
	}
}

func publishFailed(subject, nuid string, obj interface{}) {
	log.Printf("Failed to publish to subject: %s, nuid: %s, value: %+v", subject, nuid, obj)

	// resend it later
	go func(subject string, obj interface{}) {
		time.Sleep(delaySeconds * time.Second)

		log.Println("Retrying publishing...")

		sc.Publish(subject, obj)
	}(subject, obj)
}
