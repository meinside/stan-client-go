# stan-client-go

A wrapper for NATS Streaming Server (STAN).

It will (not perfectly, but try hard to) handle connection recoveries to STAN servers

and make publshing/consuming messages a little easier.

## How to get

```bash
$ go get -u github.com/meinside/stan-client-go
```

## Example

See the [examples here](https://github.com/meinside/stan-client-go/tree/master/example).

## TODO

- [ ] Fix possible goroutine leaks on disconnects
- [ ] Add more utility functions
- [ ] Support more connection/subscription options
- [ ] Add test codes

## License

MIT

