# **github.com/vcabbage/dogstatsd**

[![Go Report Card](https://goreportcard.com/badge/github.com/vcabbage/dogstatsd)](https://goreportcard.com/report/github.com/vcabbage/dogstatsd)
[![Coverage Status](https://coveralls.io/repos/github/vcabbage/dogstatsd/badge.svg?branch=master)](https://coveralls.io/github/vcabbage/dogstatsd?branch=master)
[![Build Status](https://travis-ci.org/vcabbage/dogstatsd.svg?branch=master)](https://travis-ci.org/vcabbage/dogstatsd)
[![Build status](https://ci.appveyor.com/api/projects/status/cnhun09mqaofhffp?svg=true)](https://ci.appveyor.com/project/vCabbage/dogstatsd)
[![GoDoc](https://godoc.org/github.com/vcabbage/dogstatsd?status.svg)](http://godoc.org/github.com/vcabbage/dogstatsd)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/vcabbage/dogstatsd/master/LICENSE.txt)

## Fork Info

This code was forked from [datadog-go](https://github.com/DataDog/datadog-go) with the purpose of improving performance and adjusting the API.

## Overview

Package `dogstatsd` provides a Go [dogstatsd](https://docs.datadoghq.com/guides/dogstatsd/) client. Dogstatsd extends Statsd, adding tags
and histograms.

## Get the code

    $ go get -u github.com/vcabbage/dogstatsd

## Usage

```go
// Create the client
c, err := dogstatsd.New()
if err != nil {
    log.Fatal(err)
}
// Prefix every metric with the app name
c.Namespace = "flubber."
// Send the EC2 availability zone as a tag with every metric
c.Tags = append(c.Tags, "us-east-1a")

// Send some metrics
err = c.Gauge("request.queue_depth", 12, 1)
err = c.Timing("request.duration", duration, 1)
err = c.TimeInMilliseconds("request", 12, 1)
err = c.Incr("request.count_total", 1)
err = c.Decr("request.count_total", 1)
err = c.Count("request.count_total", 2, 1)
```

## Buffering Client

DogStatsD accepts packets with multiple statsd payloads in them. Using the `ConnBuffer` option will buffer up commands
and send them when the buffer is reached or after 100msec.

## Unix Domain Sockets Client

DogStatsD version 6 accepts packets through a Unix Socket datagram connection. You can use this protocol by giving a
`unix:///path/to/dsd.socket` argument to the `ConnAddr` option.

With this protocol, writes can become blocking if the server's receiving buffer is full. Our default behaviour is to
timeout and drop the packet after 1 ms. You can set a custom timeout duration via the `SetWriteTimeout` method.

The default mode is to pass write errors from the socket to the caller. This includes write errors the library will
automatically recover from (DogStatsD server not ready yet or is restarting). You can drop these errors and emulate
the UDP behavior by setting the `SkipErrors` property to `true`. Please note that packets will be dropped in both modes.

## Development

Run the tests with:

    $ go test

## Documentation

Please see: http://godoc.org/github.com/vcabbage/dogstatsd

## License

go-dogstatsd is released under the [MIT license](http://www.opensource.org/licenses/mit-license.php).

## Credits

Original code by [ooyala](https://github.com/ooyala/go-dogstatsd).
