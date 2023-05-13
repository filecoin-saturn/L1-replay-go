# L1 Replay

A tool that replays requests to an L1 and outputs performance metrics.

## Usage

```sh
$ go run cmd/*.go replay -f sample-logs.ndjson
```

Use http1 (default):

```sh
$ go run cmd/*.go replay --http=1
```

Use http2

```sh
$ go run cmd/*.go replay --http=2
```

Use L1 ip address

```sh
$ go run cmd/*.go replay --ip=1.2.3.4
```

Limit number of logs.

```sh
$ go run cmd/*.go replay -n 100
```

Limit number of logs by duration. `-d 5` means "Replay 5 minutes of logs from the start of the log file".

```sh
$ go run cmd/*.go replay -d 5
```

Set number of http clients

```sh
$ go run cmd/*.go replay -c=3
```
