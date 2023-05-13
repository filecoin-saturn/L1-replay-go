package main

import (
	"crypto/tls"
	_ "fmt"
	"io"
	"io/ioutil"
	"math/rand"

	"net/http"
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

var http1Clients []*http.Client
var http2Clients []*http.Client
var http2ClientsRaw []*http.Client

func makeHttp1ClientSet(count int) []*http.Client {
    ret := make([]*http.Client, count)
    for i := range ret {
        ret[i] = &http.Client{
            Timeout: time.Duration(TIMEOUT_SEC) * time.Second,
            Transport: &http.Transport{
                MaxIdleConns:        1000,
                MaxConnsPerHost:     200,
                MaxIdleConnsPerHost: 100,
                IdleConnTimeout:     90 * time.Second,
                TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
            },
        }
    }
    return ret
}

func makeHttp2ClientSet(count int) []*http.Client {
    ret := make([]*http.Client, count)
    for i := range ret {
        ret[i] = &http.Client{
            Timeout: time.Duration(TIMEOUT_SEC) * time.Second,
            Transport: &http2.Transport{
                TLSClientConfig:    &tls.Config{InsecureSkipVerify: true},
                DisableCompression: true,
                AllowHTTP:           true,
                //MaxReadFrameSize:     262144, // defaults to 16k
            },
        }
    }
    return ret
}

func initHttpClients (count int) {
    http1Clients = makeHttp1ClientSet(count)
    http2Clients = makeHttp2ClientSet(count)
    http2ClientsRaw = makeHttp2ClientSet(count)
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
func sendRequest(log Log, opts *Options) RequestResult {
    start := time.Now()
    var ttfb int
    var cacheHit bool
    var requestErr error
    var status int
    var responseSize int
    var url *url.URL
    var client *http.Client

    if opts.HttpVersion == 1 {
        index := rand.Intn(len(http1Clients))
        client = http1Clients[index]
    } else {
        if opts.ClientPerFormat {
            if log.Format == "raw" {
                index := rand.Intn(len(http2ClientsRaw))
                client = http2ClientsRaw[index]
            } else {
                index := rand.Intn(len(http2Clients))
                client = http2Clients[index]
            }
        } else {
            index := rand.Intn(len(http2Clients))
            client = http2Clients[index]
        }
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
    req.Header = headers
    resp, err := client.Do(req)
    if err != nil {
        requestErr = err
    }

    buffer := make([]byte, 1024)

    if resp != nil {
        for {
            n, err := resp.Body.Read(buffer)

            if n > 0 {
                if ttfb == 0 {
                    ttfb = int(time.Since(start).Milliseconds())
                }
                responseSize += n
            }

            if n == 0 || err != nil {
                if err != io.EOF {
                    requestErr = err
                }
                break
            }

            if responseSize >= MAX_DOWNLOAD_BYTES {
                break
            }
        }

        status = resp.StatusCode
        cacheHit = resp.Header.Get("saturn-cache-status") == "HIT"

        io.Copy(ioutil.Discard, resp.Body) // ensure body is fully read so conn can be reused
        resp.Body.Close()
    }

    duration := int64(time.Since(start).Milliseconds())

    result := RequestResult{
        ttfb,
        cacheHit,
        status,
        log.Format,
        duration,
        responseSize,
        requestErr,
    }

    return result
}