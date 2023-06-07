package main

import (
	"crypto/tls"
	"io/ioutil"

	"net/http"
	"net/http/httptrace"
	"net/url"
	"time"

	"golang.org/x/net/http2"
)

const (
    L1S_HOST = "l1s.strn.pl"
    MEGABYTE = 1024 * 1024
    MAX_DOWNLOAD_BYTES = MEGABYTE * 50
    TIMEOUT_SEC = 60
)

var http1Client = &http.Client{
    Timeout: time.Duration(TIMEOUT_SEC) * time.Second,
    Transport: &http.Transport{
        MaxIdleConns:        1000,
        MaxConnsPerHost:     1000,
        MaxIdleConnsPerHost: 1000,
        IdleConnTimeout:     90 * time.Second,
        TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
    },
}

var http2Client = &http.Client{
    Timeout: time.Duration(TIMEOUT_SEC) * time.Second,
    Transport: &http2.Transport{
        TLSClientConfig:    &tls.Config{InsecureSkipVerify: true},
        DisableCompression: true,
        AllowHTTP:           true,
       // MaxReadFrameSize:     262144 * 4, // defaults to 16k
        CountError: func(errType string) {
            println(errType)
        },

    },
}

func acceptHeader(format string) string {
    if format == "car" {
        return "application/vnd.ipld.car"
    } else if format == "raw" {
        return "application/vnd.ipld.raw"
    } else {
        return ""
    }
}

// sudo tcptrack -i eth0 "dst 51.161.35.66"
// iftop -f "src 70.95.27.225 && dst port 443" -P -n
func sendRequest(log Log, opts *Options) RequestResult {
    start := time.Now()
    var ttfb int64
    var cacheHit bool
    var requestErr error
    var status int
    var responseSize int
    var url *url.URL
    var client *http.Client

    if opts.HttpVersion == 1 {
        client = http1Client
    } else {
        client = http2Client
    }

    headers := map[string][]string{
        //"host": {L1S_HOST},
        "accept": {acceptHeader(log.Format)},
        "user-agent": {"L1-replay-go"},
    }

    url = log.URL
    req, err := http.NewRequest("GET", url.String(), nil)
    if err != nil {
        panic(err)
    }


    trace := &httptrace.ClientTrace{
        GotFirstResponseByte: func() {
            ttfb = time.Since(start).Milliseconds()
        },
    }

    req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
    req.Header = headers
    resp, err := client.Do(req)
    if err != nil {
        requestErr = err
    }

    if resp != nil {
        body, err := ioutil.ReadAll(resp.Body)
        if err != nil {
            requestErr = err
        }
        responseSize = len(body)
        status = resp.StatusCode
        cacheHit = resp.Header.Get("saturn-cache-status") == "HIT"

        resp.Body.Close()
    }

    duration := int64(time.Since(start).Milliseconds())

    result := RequestResult{
        int(ttfb),
        cacheHit,
        status,
        log.Format,
        duration,
        responseSize,
        requestErr,
    }

    return result
}
