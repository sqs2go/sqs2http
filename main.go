package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"github.com/chaseisabelle/flagz"
	"github.com/chaseisabelle/sqs2go"
	"github.com/chaseisabelle/sqs2go/config"
	"net/http"
	"strings"
)

var client *http.Client //<< http client
var to *string          //<< http endpoint
var method *string      //<< http request method
var headers http.Header //<< http headers
var requeue []int       //<< only requeue if http response code meets one of these
var onFail bool         //<< if no requeue params given, default to requeue on any !2xx status code

func main() {
	to = flag.String("to", "", "the url to forward the messages to")
	method = flag.String("method", "GET", "the request method to send the message with")

	var requeueFlags flagz.Flagz
	var headerFlags flagz.Flagz

	flag.Var(&requeueFlags, "requeue", "the http status code to requeue a message for")
	flag.Var(&requeueFlags, "header", "the http headers")

	sqs, err := sqs2go.New(config.Load(), handler, func(err error) {
		println(err.Error())
	})

	if err != nil {
		panic(err)
	}

	requeue, err = requeueFlags.Intz()

	if err != nil {
		panic(err)
	}

	err = buildHeaders(headerFlags.Stringz())

	if err != nil {
		panic(err)
	}

	onFail = len(requeue) == 0
	client = &http.Client{}

	err = sqs.Start()

	if err != nil {
		panic(err)
	}
}

func handler(bod string) error {
	req, err := http.NewRequest(*method, *to, bytes.NewBufferString(bod))

	if err != nil {
		return err
	}

	req.Header = headers

	res, err := client.Do(req)

	if res == nil {
		if err == nil {
			err = errors.New("received nil response with no error")
		}

		return err
	}

	rsc := res.StatusCode

	if onFail && (rsc < 200 || rsc > 299) {
		return statusCodeError(rsc, err)
	}

	for _, sc := range requeue {
		if sc == rsc {
			return statusCodeError(rsc, err)
		}
	}

	return nil
}

func statusCodeError(sc int, err error) error {
	if err == nil {
		err = fmt.Errorf("received %d response with no error", sc)
	}

	return err
}

func buildHeaders(hdrs []string) error {
	headers = http.Header{}

	if hdrs == nil {
		return nil
	}

	for _, h := range hdrs {
		spl := strings.SplitAfterN(h, ":", 2)

		if len(spl) != 2 {
			return fmt.Errorf("invalid header: %s", h)
		}

		key := strings.TrimSpace(spl[0])

		if key == "" {
			return fmt.Errorf("invalid header: %s", h)
		}

		headers.Add(key, spl[1])
	}

	return nil
}
