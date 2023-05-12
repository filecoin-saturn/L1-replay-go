package main

import (
	"crypto/tls"
	_ "fmt"
	"io"
	"io/ioutil"

	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/http2"
)

const (
    L1S_HOST = "l1s.strn.pl"
    MEGABYTE = 1024 * 1024
    MAX_DOWNLOAD_BYTES = MEGABYTE * 50
)

var sharedClient *http.Client

func acceptHeader(format string) string {
    if format == "car" {
        return "application/vnd.ipld.car"
    } else if format == "raw" {
        return "application/vnd.ipld.raw"
    } else {
        return ""
    }
}

func sendRequest(log Log, httpVersion int) RequestResult {
    start := time.Now()
    var ttfb int
    var cacheHit bool
    var requestErr error
    var status int
    var responseSize int
    timeoutSec := 60
    var url *url.URL

    if sharedClient == nil {
        println("SUH")
        if httpVersion == 1 {
            tr := &http.Transport{
                MaxIdleConns:        1000,
				MaxConnsPerHost:     100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
                TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
            }
            sharedClient = &http.Client{Timeout: time.Duration(timeoutSec) * time.Second, Transport: tr}
        } else {
            tr := &http2.Transport{
                TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
                DisableCompression:  true,
                AllowHTTP:           true,
            }
            sharedClient = &http.Client{Timeout: time.Duration(timeoutSec) * time.Second, Transport: tr}
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
    resp, err := sharedClient.Do(req)
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
