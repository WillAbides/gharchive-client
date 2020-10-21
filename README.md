# gharchive-client

[![godoc](https://godoc.org/github.com/WillAbides/gharchive-client?status.svg)](https://godoc.org/github.com/WillAbides/gharchive-client)
[![ci](https://github.com/WillAbides/gharchive-client/workflows/ci/badge.svg?branch=main&event=push)](https://github.com/WillAbides/gharchive-client/actions?query=workflow%3Aci+branch%3Amaster+event%3Apush)

A command line client and go package for iterating over events from
[gharchive](https://www.gharchive.org/).

## Installation

Download binaries from [the latest release](https://github.com/WillAbides/gharchive-client/releases/latest)

## Command line usage

```
Usage: gharchive <start> [<end>]

Arguments:
  <start>    start time formatted as YYYY-MM-DD, or as an RFC3339 date
  [<end>]    end time formatted as YYYY-MM-DD, or as an RFC3339 date. default is an hour past start

Flags:
  -h, --help                     Show context-sensitive help.
      --type=TYPE,...            include only these event types
      --not-type=NOT-TYPE,...    exclude these event types
      --strict-created-at        only output events with a created_at between start and end
      --no-empty-lines           skip empty lines
      --only-valid-json          skip lines that aren not valid json objects
      --preserve-order           ensure that events are output in the same order they exist on data.gharchive.org
      --concurrency=INT          max number of concurrent downloads to run. Ignored if --preserve-order is set. Default is the number of cpus available.
      --debug                    output debug logs
```

## Performance

I can iterate about 200k events per second from an 8 core MacBook Pro with a 
cable modem. On an 80 core server in a data center that increases to about 450k.
