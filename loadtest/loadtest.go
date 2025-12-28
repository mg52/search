package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		baseURL   = flag.String("url", "http://localhost:8080/search", "Search endpoint base URL (no query params)")
		indexName = flag.String("index", "songs", "Index query param value")
		page      = flag.Int("page", 0, "Page query param value")
		keywords  = flag.String("keywords", "/Documents/somedata/songs5m-4-10.txt", "Path to keywords file (one query per line)")
		workers   = flag.Int("workers", 8, "Number of parallel workers")
		requests  = flag.Int("requests", 10000, "Total number of requests to send (across all workers)")
		timeout   = flag.Duration("timeout", 10*time.Second, "HTTP client timeout per request")
		seed      = flag.Int64("seed", time.Now().UnixNano(), "Random seed (for reproducibility)")
		keepAlive = flag.Bool("keepalive", true, "Use HTTP keep-alive")
	)
	flag.Parse()

	home := os.Getenv("HOME")
	if home == "" {
		panic("HOME not set")
	}

	file := home + *keywords

	queries, err := readLinesNonEmpty(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read keywords file: %v\n", err)
		os.Exit(1)
	}
	if len(queries) == 0 {
		fmt.Fprintln(os.Stderr, "keywords file has no non-empty lines")
		os.Exit(1)
	}

	// Transport tuned for load testing.
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		// Keep-alive improves realism and throughput.
		DisableKeepAlives: !*keepAlive,

		// Reasonable defaults for local load tests:
		MaxIdleConns:        1000,
		MaxConnsPerHost:     0, // unlimited
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{
		Timeout:   *timeout,
		Transport: tr,
	}

	// Distribute work using an atomic counter rather than a huge jobs channel.
	var sent int64
	total := int64(*requests)

	// Collect results.
	results := make([]time.Duration, 0, *requests)
	var resultsMu sync.Mutex
	var errorsCount int64

	startAll := time.Now()

	var wg sync.WaitGroup
	wg.Add(*workers)

	fmt.Println("Starting...")
	// Make per-worker RNGs to avoid lock contention.
	for w := 0; w < *workers; w++ {
		go func(workerID int) {
			defer wg.Done()

			r := rand.New(rand.NewSource(*seed + int64(workerID)*1_000_003))

			for {
				i := atomic.AddInt64(&sent, 1)
				if i > total {
					return
				}

				q := queries[r.Intn(len(queries))]

				reqURL := buildURL(*baseURL, *indexName, q, *page)

				t0 := time.Now()
				req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, reqURL, nil)
				resp, err := client.Do(req)
				lat := time.Since(t0)

				if lat > 300*time.Millisecond {
					fmt.Println(q)
				}
				if err != nil {
					atomic.AddInt64(&errorsCount, 1)
					// Still record latency for visibility
					resultsMu.Lock()
					results = append(results, lat)
					resultsMu.Unlock()
					continue
				}

				// Always drain/close body to reuse connections.
				_ = resp.Body.Close()

				// Optionally treat non-2xx as errors (still record latency).
				if resp.StatusCode < 200 || resp.StatusCode >= 300 {
					atomic.AddInt64(&errorsCount, 1)
				}

				resultsMu.Lock()
				results = append(results, lat)
				resultsMu.Unlock()
			}
		}(w)
	}

	wg.Wait()
	elapsedAll := time.Since(startAll)

	// Stats
	if len(results) == 0 {
		fmt.Println("No results collected.")
		os.Exit(1)
	}

	// Copy + sort for percentile calc.
	sorted := make([]time.Duration, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	avg := average(sorted)
	p50 := percentile(sorted, 50)
	p95 := percentile(sorted, 95)
	p99 := percentile(sorted, 99)

	fmt.Printf("Requests: %d | Workers: %d | Errors: %d | Total wall time: %s | GOMAXPROCS: %d\n",
		len(results), *workers, atomic.LoadInt64(&errorsCount), elapsedAll.Round(time.Millisecond), runtime.GOMAXPROCS(0))

	fmt.Println("Latency (milliseconds):")
	fmt.Printf("avg: %.2f ms\n", float64(avg)/float64(time.Millisecond))
	fmt.Printf("p50: %.2f ms\n", float64(p50)/float64(time.Millisecond))
	fmt.Printf("p95: %.2f ms\n", float64(p95)/float64(time.Millisecond))
	fmt.Printf("p99: %.2f ms\n", float64(p99)/float64(time.Millisecond))
}

func readLinesNonEmpty(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	// Allow longer lines if needed.
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 1024*1024)

	for sc.Scan() {
		s := strings.TrimSpace(sc.Text())
		if s == "" {
			continue
		}
		lines = append(lines, s)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

func buildURL(baseURL, index, q string, page int) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		// Fall back to basic concatenation if baseURL is malformed.
		return fmt.Sprintf("%s?index=%s&q=%s&page=%d", baseURL, url.QueryEscape(index), url.QueryEscape(q), page)
	}
	params := u.Query()
	params.Set("index", index)
	params.Set("q", q)
	params.Set("page", fmt.Sprintf("%d", page))
	u.RawQuery = params.Encode()
	return u.String()
}

func average(sorted []time.Duration) time.Duration {
	var sum int64
	for _, d := range sorted {
		sum += int64(d)
	}
	return time.Duration(sum / int64(len(sorted)))
}

// percentile returns the nearest-rank percentile (common in latency reporting).
// Example: p=50 returns element at ceil(p/100*N)-1.
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	n := len(sorted)
	// nearest-rank index:
	// k = ceil(p/100 * n)
	// idx = k-1
	k := (p*n + 99) / 100 // integer ceil of p*n/100
	idx := k - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}
