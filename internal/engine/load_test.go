package engine

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var loadVocab = []string{
	"apple", "banana", "phone", "laptop", "music",
	"video", "cloud", "data", "server", "network",
	"mobile", "device", "smart", "fast", "secure",
	"tablet", "wireless", "charger", "camera", "display",
	"internet", "gaming", "storage", "memory", "processor",
	"software", "hardware", "digital", "streaming", "audio",
	"battery", "performance", "virtual", "system", "monitor",
	"keyboard", "mouse", "headphones", "bluetooth", "signal",
}

func randomLoadText(r *rand.Rand, n int) string {
	words := make([]string, n)
	for i := 0; i < n; i++ {
		words[i] = loadVocab[r.Intn(len(loadVocab))]
	}

	out := ""
	for _, w := range words {
		out += w + " "
	}
	return out
}

func generateLoadDocs(n int) []map[string]interface{} {
	r := rand.New(rand.NewSource(99))

	docs := make([]map[string]interface{}, n)

	for i := 0; i < n; i++ {
		docs[i] = map[string]interface{}{
			"id":    fmt.Sprintf("%d", i),
			"title": randomLoadText(r, 8+r.Intn(12)),
			"tags":  randomLoadText(r, 3+r.Intn(6)),
			"genre": []string{"tech", "media", "gaming"}[r.Intn(3)],
		}
	}

	return docs
}

func TestSearchEngine_Load(t *testing.T) {
	t.Skip("skipping heavy load test by default")
	const docCount = 100_000
	const queryCount = 5_000

	fmt.Println("Generating documents...")
	docs := generateLoadDocs(docCount)

	fmt.Println("Creating search engine...")
	se := NewSearchEngine(
		[]string{"title", "tags"},
		map[string]bool{"genre": true},
		10,
	)

	start := time.Now()
	se.Index(docs)
	indexDuration := time.Since(start)

	fmt.Printf("Indexed %d docs in %v\n", docCount, indexDuration)

	queries := []string{
		"apple phone",
		"cloud data",
		"gaming monitor",
		"wireless headphones",
		"battery performance",
		"video streaming",
		"secure network",
		"bluetooth signal",
	}

	fmt.Println("Running search load...")

	var totalSearchTime time.Duration
	var maxSearchTime time.Duration
	var totalResults int

	for i := 0; i < queryCount; i++ {
		q := queries[i%len(queries)]

		searchStart := time.Now()
		res := se.Search(q, nil)
		searchDuration := time.Since(searchStart)

		if res == nil {
			t.Fatalf("nil result for query: %s", q)
		}

		totalSearchTime += searchDuration
		totalResults += len(res.Docs)

		if searchDuration > maxSearchTime {
			maxSearchTime = searchDuration
		}
	}

	avgSearchTime := totalSearchTime / queryCount
	qps := float64(queryCount) / totalSearchTime.Seconds()

	fmt.Println("----- LOAD TEST SUMMARY -----")
	fmt.Printf("Documents indexed: %d\n", docCount)
	fmt.Printf("Queries executed: %d\n", queryCount)
	fmt.Printf("Index duration: %v\n", indexDuration)
	fmt.Printf("Average search latency: %v\n", avgSearchTime)
	fmt.Printf("Max search latency: %v\n", maxSearchTime)
	fmt.Printf("Queries/sec: %.2f\n", qps)
	fmt.Printf("Average results/query: %.2f\n", float64(totalResults)/queryCount)
}
