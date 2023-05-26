package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	_ "io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	_ "strings"
	"sync"
	"time"

	"github.com/montanaflynn/stats"
)

const LOG_FILE = "logs/logs.ndjson"

type Options struct {
	LogFilePath     string `json:"logFilePath"`
	DurationMinutes int    `json:"durationMinutes"`
	NumLogs         int    `json:"numLogs"`
	NumClients      int    `json:"numClients"`
	ClientPerFormat bool   `json:"clientPerFormat"`
	HttpVersion     int    `json:"httpVersion"`
	IpAddress       string `json:"ipAddress"`
}


type Log struct {
	Status   int    `json:"status"`
	URL          *url.URL  `json:"url"`
	StartTime    time.Time `json:"startTime"`
	Format       string    `json:"format"`
	CacheHit     bool      `json:"cacheHit"`
}

type Metric struct {
	Status   int    `json:"status"`
	Format   string `json:"format"`
	CacheHit bool `json:"cacheHit"`
	TTFBMs   Percentiles `json:"ttfb_ms"`
	DurationMs   Percentiles `json:"duration_ms"`
	NumLogs  int `json:"numLogs"`
	Errors   map[string]int `json:"errors,omitempty"`
}

type Percentiles struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

type Info struct {
	Options    *Options `json:"options"`
	Language   string  `json:"language"`
	Date       time.Time `json:"date"`
	NumLogs    int        `json:"numLogs"`
	Metrics    []Metric `json:"metrics"`
}

type RequestResult struct {
    TTFB         int
    CacheHit     bool
    Status       int
    Format       string
    DurationMs   int64
    ResponseSize int
    RequestErr   error
}

func (l *Log) UnmarshalJSON(data []byte) error {
    var jsonData map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return err
	}
	parsedURL, err := url.Parse(jsonData["url"].(string))
	if err != nil {
		return err
	}
	l.URL = parsedURL
	startTime, err := time.Parse(time.RFC3339Nano, jsonData["startTime"].(string))
	if err != nil {
		return err
	}
	l.StartTime = startTime
	if jsonData["format"] != nil {
		l.Format = jsonData["format"].(string)
	}
	l.CacheHit = jsonData["cacheHit"].(bool)
	l.Status = int(jsonData["httpStatusCode"].(float64))
	return nil
}

func getModifiedLogs(opts *Options) ([]Log, error) {
    f, err := os.Open(opts.LogFilePath)
    if err != nil {
        return nil, err
    }
    defer f.Close()

    var logs []Log
    var oldestTimestamp time.Time
    var offset time.Duration
    var count int

    scanner := bufio.NewScanner(f)
    for scanner.Scan() {
        line := scanner.Text()
        var log Log
        if err := json.Unmarshal([]byte(line), &log); err != nil {
			if !strings.Contains(err.Error(), "invalid control character in URL") {
				fmt.Println(err)
			}
            continue
        }
		if log.Format == "" {
            continue
        }

		// TODO: TESTING
		// if !log.CacheHit || log.Status != 200 || log.Format != "raw" {
		// 	continue
		// }

        originalTimestamp := log.StartTime

        if oldestTimestamp.IsZero() {
            oldestTimestamp = originalTimestamp
            buffer := 3000 * time.Millisecond
            offset = time.Since(oldestTimestamp) + buffer
        }
        log.StartTime = originalTimestamp.Add(offset)

        if opts.IpAddress != "" {
            log.URL.Host = opts.IpAddress
        }
        logs = append(logs, log)

        if !oldestTimestamp.IsZero() && opts.DurationMinutes != 0 {
            durationSoFar := originalTimestamp.Sub(oldestTimestamp)
            if durationSoFar.Minutes() > float64(opts.DurationMinutes) {
                break
            }
        }
        count++

        if opts.NumLogs != 0 && count >= opts.NumLogs {
            break
        }
    }

    return logs, nil
}

func replayLogs(logs []Log, opts *Options) ([]RequestResult, error) {
	var wg sync.WaitGroup
	var mutex sync.Mutex
	var results []RequestResult
	start := time.Now()

	numReqs := float32(0)
	numBytes := 0

	for i := 0; i < len(logs); {
		log := logs[i]

		if log.StartTime.Before(time.Now()) {
			i++
			numReqs++

			wg.Add(1)
			go func(log Log, index int) {
				defer wg.Done()
				result := sendRequest(log, opts)

				mutex.Lock()
				results = append(results, result)
				mutex.Unlock()

				progress := float64(i) / float64(len(logs)) * 100.0
				numBytes += result.ResponseSize
				numMB := float32(numBytes) / float32(1048576)

				durationSec := (int(time.Since(start).Seconds()))
				reqPerSec := float32(0)
				bandwidth := float32(0)
				if durationSec > 0 {
					bandwidth = numMB / float32(time.Since(start).Seconds())
					reqPerSec = numReqs / float32(durationSec)
				}
				fmt.Printf("%d, %.2f%%, %.2f r/s, %.2f mb/s, %+v\n", index, progress, reqPerSec, bandwidth, result)
			}(log, i)
		} else {
			time.Sleep(time.Millisecond * 1)
		}
	}

	wg.Wait()

	return results, nil
}

func calcMetrics(results []RequestResult) []Metric {
	metrics := make([]Metric, 0)
	groups := make(map[string][]RequestResult)

	for _, r := range results {
		if r.Status == 200 || r.Status == 0 {
			key := strconv.Itoa(r.Status) + "_" + r.Format + "_" + strconv.FormatBool(r.CacheHit)
			groups[key] = append(groups[key], r)
		}
	}

	for key, values := range groups {
		parts := strings.Split(key, "_")
		status, _ := strconv.Atoi(parts[0])
		format := parts[1]
		cacheHit, _ := strconv.ParseBool(parts[2])

		ttfbList := make([]float64, len(values))
		for i, v := range values {
			ttfbList[i] = float64(v.TTFB)
		}
		sort.Float64s(ttfbList)

		durationList := make([]float64, len(values))
		for i, v := range values {
			durationList[i] = float64(v.DurationMs)
		}
		sort.Float64s(durationList)

		errors := map[string]int{
			"timeoutAwaitingHeaders": 0,
			"timeoutReadingBody": 0,
		}
		for _, v := range values {
			if v.RequestErr != nil {
				if strings.Contains(v.RequestErr.Error(), "Client.Timeout exceeded while awaiting headers") {
					errors["timeoutAwaitingHeaders"]++
				} else if strings.Contains(v.RequestErr.Error(), "Client.Timeout or context cancellation while reading body") {
					errors["timeoutReadingBody"]++
				}
			}
		}

		numLogs := len(values)
		metrics = append(metrics, Metric{
			Status:   status,
			Format:   format,
			CacheHit: cacheHit,
			TTFBMs: Percentiles{
				P50: calcPercentile(ttfbList, 50),
				P90: calcPercentile(ttfbList, 90),
				P95: calcPercentile(ttfbList, 95),
				P99: calcPercentile(ttfbList, 99),
			},
			DurationMs: Percentiles{
				P50: calcPercentile(durationList, 50),
				P90: calcPercentile(durationList, 90),
				P95: calcPercentile(durationList, 95),
				P99: calcPercentile(durationList, 99),
			},
			Errors: errors,
			NumLogs: numLogs,
		})
	}
	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].NumLogs > metrics[j].NumLogs
	})
	return metrics
}

func calcPercentile (values []float64, percent float64) float64 {
	result, _ := stats.Percentile(values, percent)
	return result
}

func replay(opts *Options) error {
    logs, err := getModifiedLogs(opts)
    if err != nil {
        return err
    }

    results, err := replayLogs(logs, opts)
    if err != nil {
        return err
    }
    metrics := calcMetrics(results)

	ipAddress := opts.IpAddress
    if ipAddress == "" {
        ipAddress = logs[0].URL.Hostname()
    }

	info := Info{
		opts,
		"Go",
		time.Now(),
		len(logs),
		metrics,
	}

    b, err := json.MarshalIndent(info, "", "  ")
    if err != nil {
        return err
    }

    err = ioutil.WriteFile(fmt.Sprintf("results/results_%d.json", time.Now().Unix()), b, 0644)
    if err != nil {
        return err
    }

    return nil
}

func parseOptions() *Options {
    var logFilePath, ipAddress string
    var durationMinutes, numLogs, numClients, httpVersion int
    var clientPerFormat bool

    flag.StringVar(&logFilePath, "f", LOG_FILE, "path to log file")
    flag.IntVar(&durationMinutes, "d", 0, "duration of test based on log timestampts (not wall clock time)")
    flag.IntVar(&numLogs, "n", 0, "number of logs to replay")
    flag.IntVar(&numClients, "c", 1, "number of concurrent clients to simulate")
    flag.BoolVar(&clientPerFormat, "clientPerFormat", false, "use separate clients for each log format")
    flag.IntVar(&httpVersion, "http", 1, "HTTP version to use")
    flag.StringVar(&ipAddress, "ip", "", "IP address of L1 node")
    flag.Parse()

    return &Options{
        LogFilePath:     logFilePath,
        DurationMinutes: durationMinutes,
        NumLogs:         numLogs,
        NumClients:      numClients,
        ClientPerFormat: clientPerFormat,
        HttpVersion:     httpVersion,
        IpAddress:       ipAddress,
    }
}

// go run cmd/replay/*.go -n 3 -http 2 -ip 51.161.35.66 -c 1
//
// Core L1 montreal: 51.161.35.66
// Core Ly NYC: 138.199.41.51
func main() {
    rand.Seed(time.Now().UnixNano())

	opts := parseOptions()
	initHttpClients(opts.NumClients)
	replay(opts)
}
