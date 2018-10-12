# example/pingpong

A sample application that publishes and consumes ping & pong messages.

## Generate self-signed certificates

Run [this script](https://raw.githubusercontent.com/meinside/rpi-configs/master/bin/gen_selfsigned_certs.sh) to generate self-signed certificates easily.

```bash
$ /path/to/gen_selfsigned_certs.sh localhost
```

Put the generated files in `./certs/` directory.

## Start local STAN server

Launch STAN server locally:

```bash
$ /path/to/nats-streaming-server -sc stan.conf -c stan.conf -D -user USER -pass PASSWORD
```

## Run

```bash
$ go run main.go
```
