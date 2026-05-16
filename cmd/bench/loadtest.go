package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func runLoadtest(args []string) {
	fs := flag.NewFlagSet("loadtest", flag.ExitOnError)
	baseURL   := fs.String("url", "http://localhost:8080/search", "Search endpoint URL")
	indexName := fs.String("index", "bench", "Index name (?index=)")
	vocabFile := fs.String("vocab", "vocab.txt", "Vocabulary file (produced by vocab command)")
	workers   := fs.Int("workers", 8, "Number of parallel workers")
	requests  := fs.Int("requests", 10_000, "Total requests to send")
	timeout   := fs.Duration("timeout", 10*time.Second, "Per-request HTTP timeout")
	seed      := fs.Int64("seed", time.Now().UnixNano(), "Random seed")
	keepAlive := fs.Bool("keepalive", true, "Use HTTP keep-alive")
	filterPct := fs.Int("filter-pct", 50, "Percentage of requests with a year filter (0-100)")
	multiPct  := fs.Int("multi-pct", 50, "Percentage of multi-term queries (0-100)")
	_ = fs.Parse(args)

	vocab, err := loadVocabFile(*vocabFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "vocab: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Vocabulary: %d words from %s\n", len(vocab), *vocabFile)

	tr := &http.Transport{
		DisableKeepAlives:   !*keepAlive,
		MaxIdleConns:        1000,
		MaxConnsPerHost:     0,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{Timeout: *timeout, Transport: tr}

	type ltMode struct{ multi, filter bool }
	type ltResult struct {
		lat  time.Duration
		mode ltMode
		err  bool
	}

	var (
		sent      int64
		errCount  int64
		resultsMu sync.Mutex
		results   = make([]ltResult, 0, *requests)
	)

	fmt.Printf("Target:  %s  index=%s\n", *baseURL, *indexName)
	fmt.Printf("Workers: %d  |  Requests: %d  |  filter-pct: %d%%  |  multi-pct: %d%%\n",
		*workers, *requests, *filterPct, *multiPct)
	fmt.Println("Starting...")

	startAll := time.Now()
	var wg sync.WaitGroup
	wg.Add(*workers)

	for w := 0; w < *workers; w++ {
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(*seed + int64(workerID)*1_000_003))

			for {
				if atomic.AddInt64(&sent, 1) > int64(*requests) {
					return
				}
				isMulti  := r.Intn(100) < *multiPct
				isFilter := r.Intn(100) < *filterPct

				var q string
				if isMulti {
					q = ltMultiQuery(r, vocab)
				} else {
					q = ltSingleQuery(r, vocab)
				}
				reqURL := buildSearchURL(*baseURL, *indexName, q, isFilter, r)

				t0 := time.Now()
				req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, reqURL, nil)
				resp, reqErr := client.Do(req)
				lat := time.Since(t0)

				res := ltResult{lat: lat, mode: ltMode{isMulti, isFilter}, err: reqErr != nil}
				if reqErr != nil {
					atomic.AddInt64(&errCount, 1)
				} else {
					if resp.StatusCode < 200 || resp.StatusCode >= 300 {
						atomic.AddInt64(&errCount, 1)
						res.err = true
					}
					_ = resp.Body.Close()
				}
				resultsMu.Lock()
				results = append(results, res)
				resultsMu.Unlock()
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(startAll)

	if len(results) == 0 {
		fmt.Println("No results collected.")
		os.Exit(1)
	}

	allLats := make([]time.Duration, len(results))
	for i, r := range results {
		allLats[i] = r.lat
	}
	sort.Slice(allLats, func(i, j int) bool { return allLats[i] < allLats[j] })
	rps := float64(len(results)) / elapsed.Seconds()

	fmt.Printf("\n──────────────────────────────────────────────────────\n")
	fmt.Printf("Requests: %-6d  Workers: %-3d  Errors: %-4d  GOMAXPROCS: %d\n",
		len(results), *workers, atomic.LoadInt64(&errCount), runtime.GOMAXPROCS(0))
	fmt.Printf("Wall time: %-12s  RPS: %.1f\n", elapsed.Round(time.Millisecond), rps)
	fmt.Printf("──────────────────────────────────────────────────────\n")
	fmt.Println("Overall latency:")
	printLTRow("  all", allLats)

	byMode := make(map[ltMode][]time.Duration)
	for _, r := range results {
		byMode[r.mode] = append(byMode[r.mode], r.lat)
	}
	orderedModes := []ltMode{{false, false}, {false, true}, {true, false}, {true, true}}

	fmt.Printf("\nPer-mode breakdown:\n")
	fmt.Printf("  %-24s  %8s  %8s  %8s  %8s  %6s\n", "mode", "avg", "p50", "p95", "p99", "count")
	fmt.Printf("  %-24s  %8s  %8s  %8s  %8s  %6s\n", "----", "---", "---", "---", "---", "-----")
	for _, m := range orderedModes {
		lats := byMode[m]
		if len(lats) == 0 {
			continue
		}
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		q, f := "Single", "NoFilter"
		if m.multi {
			q = "Multi "
		}
		if m.filter {
			f = "Filter  "
		}
		fmt.Printf("  %-24s  %8s  %8s  %8s  %8s  %6d\n",
			q+" / "+f,
			fmtDurMs(avgDur(lats)),
			fmtDurMs(pctDur(lats, 50)),
			fmtDurMs(pctDur(lats, 95)),
			fmtDurMs(pctDur(lats, 99)),
			len(lats),
		)
	}
	fmt.Printf("──────────────────────────────────────────────────────\n")
}

// ---- query helpers ----

func ltSingleQuery(r *rand.Rand, vocab []string) string {
	word := vocab[r.Intn(len(vocab))]
	roll := r.Float64()
	switch {
	case roll < 0.10:
		return misspellWord(r, word)
	case roll < 0.35:
		return prefixWord(r, word)
	default:
		return word
	}
}

func ltMultiQuery(r *rand.Rand, vocab []string) string {
	nTerms := 2 + r.Intn(3)
	terms := make([]string, nTerms)
	misspellIdx := -1
	if r.Float64() < 0.10 {
		misspellIdx = r.Intn(nTerms)
	}
	for j := 0; j < nTerms; j++ {
		w := vocab[r.Intn(len(vocab))]
		switch {
		case j == misspellIdx:
			w = misspellWord(r, w)
		case j == nTerms-1 && r.Float64() < 0.25:
			w = prefixWord(r, w)
		}
		terms[j] = w
	}
	return joinTerms(terms)
}

func joinTerms(terms []string) string {
	n := 0
	for _, t := range terms {
		n += len(t) + 1
	}
	if n == 0 {
		return ""
	}
	b := make([]byte, 0, n-1)
	for i, t := range terms {
		if i > 0 {
			b = append(b, ' ')
		}
		b = append(b, t...)
	}
	return string(b)
}

func buildSearchURL(base, index, q string, addFilter bool, r *rand.Rand) string {
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	params := u.Query()
	params.Set("index", index)
	params.Set("q", q)
	if addFilter {
		params.Set("filter", fmt.Sprintf("year:%d", 2000+r.Intn(25)))
	}
	u.RawQuery = params.Encode()
	return u.String()
}

// ---- duration stats ----

func printLTRow(label string, sorted []time.Duration) {
	fmt.Printf("  %-8s  avg=%-10s  p50=%-10s  p95=%-10s  p99=%s\n",
		label,
		fmtDurMs(avgDur(sorted)),
		fmtDurMs(pctDur(sorted, 50)),
		fmtDurMs(pctDur(sorted, 95)),
		fmtDurMs(pctDur(sorted, 99)),
	)
}

func fmtDurMs(d time.Duration) string {
	return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
}

func avgDur(lats []time.Duration) time.Duration {
	if len(lats) == 0 {
		return 0
	}
	var sum int64
	for _, d := range lats {
		sum += int64(d)
	}
	return time.Duration(sum / int64(len(lats)))
}

func pctDur(sorted []time.Duration, p int) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	k := (p*n + 99) / 100
	idx := k - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}
