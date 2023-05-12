package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	_ "io/ioutil"
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
	Errors   map[string]int `json:"errors"`
}

type Percentiles struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

type Info struct {
	IPAddress  string     `json:"ipAddress"`
	HTTPVersion int        `json:"httpVersion"`
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

func getModifiedLogs(logFilePath string, ipAddress string, maxDurationMinutes int, maxLogs int, useTLS bool) ([]Log, error) {
    f, err := os.Open(logFilePath)
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
			fmt.Println(err)
            continue
        }
		if log.Format == "" {
            continue
        }

		// TODO: TESTING
		// if !log.CacheHit || log.Status != 200 {
		// 	continue
		// }

        originalTimestamp := log.StartTime

        if oldestTimestamp.IsZero() {
            oldestTimestamp = originalTimestamp
            buffer := 3000 * time.Millisecond
            offset = time.Since(oldestTimestamp) + buffer
        }
        log.StartTime = originalTimestamp.Add(offset)

        if ipAddress != "" {
            log.URL.Host = ipAddress
        }
        if useTLS {
            log.URL.Scheme = "https"
        } else {
            log.URL.Scheme = "http"
        }
        logs = append(logs, log)

        if !oldestTimestamp.IsZero() && maxDurationMinutes != 0 {
            durationSoFar := originalTimestamp.Sub(oldestTimestamp)
            if durationSoFar.Minutes() > float64(maxDurationMinutes) {
                break
            }
        }
        count++

        if maxLogs != 0 && count >= maxLogs {
            break
        }
    }

    return logs, nil
}

func replayLogs(logs []Log, httpVersion int) ([]RequestResult, error) {
	var wg sync.WaitGroup
	var mutex sync.Mutex
	var results []RequestResult

	for i := 0; i < len(logs); {
		log := logs[i]

		if log.StartTime.Before(time.Now()) {
			i++

			wg.Add(1)
			go func(log Log) {
				defer wg.Done()
				result := sendRequest(log, httpVersion)

				mutex.Lock()
				results = append(results, result)
				mutex.Unlock()

				progress := float64(i) / float64(len(logs)) * 100.0
				fmt.Printf("%d, %.2f%%, %+v\n", i, progress, result)
			}(log)
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

func replay(logFilePath string, ipAddress string, maxDurationMinutes int, maxLogs int, httpVersion int, useTLS bool) error {
    logs, err := getModifiedLogs(logFilePath, ipAddress, maxDurationMinutes, maxLogs, useTLS)
    if err != nil {
        return err
    }

    results, err := replayLogs(logs, httpVersion)
    if err != nil {
        return err
    }
    metrics := calcMetrics(results)

    if ipAddress == "" {
        ipAddress = logs[0].URL.Hostname()
    }

    info := map[string]interface{}{
        "ipAddress":  ipAddress,
        "httpVersion": httpVersion,
		"lang"       : "Go",
        "date":        time.Now(),
        "numLogs":     len(logs),
        "metrics":     metrics,
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

func main() {
    args := os.Args[1:]
    cmd := args[0]

    switch cmd {
    case "replay":
        logFilePath := getFlagValue(args, "f", LOG_FILE)
        maxDurationMinutesStr := getFlagValue(args, "d", "")
        maxDurationMinutes, _ := strconv.Atoi(maxDurationMinutesStr)
        maxLogsStr := getFlagValue(args, "n", "")
        maxLogs, _ := strconv.Atoi(maxLogsStr)
        ipAddress := getFlagValue(args, "ip", "")
        httpVersionStr := getFlagValue(args, "http", "1")
        httpVersion, _ := strconv.Atoi(httpVersionStr)
        useTLSStr := getFlagValue(args, "tls", "true")
        useTLS, _ := strconv.ParseBool(useTLSStr)

        replay(logFilePath, ipAddress, maxDurationMinutes, maxLogs, httpVersion, useTLS)
    }
}

func getFlagValue(args []string, flagName string, defaultValue string) string {
    index := -1
    for i, arg := range args {
        if arg == "-"+flagName || arg == "--"+flagName {
            index = i
            break
        }
    }

    if index != -1 && len(args) > index+1 {
        return args[index+1]
    }

    return defaultValue
}
