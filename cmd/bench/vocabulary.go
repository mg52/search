package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
)

func runVocab(args []string) {
	fs := flag.NewFlagSet("vocab", flag.ExitOnError)
	size := fs.Int("size", 100_000, "Number of unique words to generate")
	out  := fs.String("out", "vocab.txt", "Output file path")
	seed := fs.Int64("seed", 42, "Random seed")
	_ = fs.Parse(args)

	words := generateVocab(*size, *seed)
	if err := writeVocab(*out, words); err != nil {
		fmt.Fprintf(os.Stderr, "write vocab: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Vocabulary: %d words → %s\n", len(words), *out)
}

func generateVocab(n int, seed int64) []string {
	const charset = "abcdefghijklmnopqrstuvwxyz"
	r := rand.New(rand.NewSource(seed))
	seen := make(map[string]struct{}, n)
	words := make([]string, 0, n)
	buf := make([]byte, 12)
	for len(words) < n {
		length := 3 + r.Intn(10)
		for i := 0; i < length; i++ {
			buf[i] = charset[r.Intn(26)]
		}
		w := string(buf[:length])
		if _, exists := seen[w]; !exists {
			seen[w] = struct{}{}
			words = append(words, w)
		}
	}
	return words
}

func writeVocab(path string, words []string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	for _, w := range words {
		bw.WriteString(w)
		bw.WriteByte('\n')
	}
	return bw.Flush()
}

func loadVocabFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	var words []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if w := strings.TrimSpace(sc.Text()); w != "" {
			words = append(words, w)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}
	if len(words) == 0 {
		return nil, fmt.Errorf("%s is empty", path)
	}
	return words, nil
}
