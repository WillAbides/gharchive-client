# gharchive-client

[![godoc](https://godoc.org/github.com/WillAbides/gharchive-client?status.svg)](https://godoc.org/github.com/WillAbides/gharchive-client)
[![ci](https://github.com/WillAbides/gharchive-client/workflows/ci/badge.svg?branch=main&event=push)](https://github.com/WillAbides/gharchive-client/actions?query=workflow%3Aci+branch%3Amaster+event%3Apush)

A command line client and go package for iterating over events from
[gharchive](https://www.gharchive.org/).

## Installation

Download binaries from [the latest release](https://github.com/WillAbides/gharchive-client/releases/latest)

## Command line usage

```
$ gharchive --help
Usage: gharchive <start> [<end>]

Arguments:
  <start>    start time formatted as YYYY-MM-DD, or as an RFC3339 date
  [<end>]    end time formatted as YYYY-MM-DD, or as an RFC3339 date. default is a day past start

Flags:
  -h, --help                     Show context-sensitive help.
      --type=TYPE,...            include only these event types
      --not-type=NOT-TYPE,...    exclude these event types
      --strict-created-at        only output events with a created_at between start and end
      --no-empty-lines           skip empty lines
      --only-valid-json          skip lines that aren not valid json objects
```

## Performance

I can iterate about 45k events per second from a MacBook Pro with a cable modem.
The bottleneck is decompressing files.
