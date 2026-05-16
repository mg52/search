package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

func runDatagen(args []string) {
	fs := flag.NewFlagSet("datagen", flag.ExitOnError)
	count     := fs.Int("count", 1_000_000, "Number of documents to generate (e.g. 1000000 or 5000000)")
	out       := fs.String("out", "data.json", "Output JSON file path")
	vocabFile := fs.String("vocab", "vocab.txt", "Vocabulary file (produced by vocab command)")
	seed      := fs.Int64("seed", 42, "Random seed")
	_ = fs.Parse(args)

	vocab, err := loadVocabFile(*vocabFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load vocab: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Vocabulary: %d words from %s\n", len(vocab), *vocabFile)

	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", *out, err)
		os.Exit(1)
	}
	defer f.Close()

	r := rand.New(rand.NewSource(*seed))
	bw := bufio.NewWriterSize(f, 8*1024*1024)

	fmt.Printf("Generating %d documents → %s\n", *count, *out)
	start := time.Now()

	bw.WriteString("[\n")
	enc := json.NewEncoder(bw)
	for i := 0; i < *count; i++ {
		if i > 0 {
			bw.WriteString(",\n")
		}

		nTitle := 3 + r.Intn(18)
		titleParts := make([]string, nTitle)
		for j := range titleParts {
			titleParts[j] = vocab[r.Intn(len(vocab))]
		}

		nTags := 1 + r.Intn(10)
		tagParts := make([]string, nTags)
		for j := range tagParts {
			tagParts[j] = vocab[r.Intn(len(vocab))]
		}

		doc := map[string]interface{}{
			"id":    fmt.Sprintf("d%d", i),
			"title": strings.Join(titleParts, " "),
			"tags":  strings.Join(tagParts, " "),
			"year":  2000 + r.Intn(25),
		}
		if err := enc.Encode(doc); err != nil {
			fmt.Fprintf(os.Stderr, "encode doc %d: %v\n", i, err)
			os.Exit(1)
		}

		if (i+1)%500_000 == 0 || i+1 == *count {
			fmt.Printf("  %d / %d\n", i+1, *count)
		}
	}
	bw.WriteString("]\n")
	if err := bw.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "flush: %v\n", err)
		os.Exit(1)
	}

	fi, _ := f.Stat()
	fmt.Printf("Done in %s — %.1f MB\n", time.Since(start).Round(time.Millisecond), float64(fi.Size())/(1024*1024))
}
