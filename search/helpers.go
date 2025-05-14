package search

import (
	"encoding/gob"
	"fmt"
	"os"
	"regexp"
	"strings"
)

func SaveMapToFile(m map[string]map[string]int, path string, shardID int) error {
	file, err := os.Create(fmt.Sprintf("%s-%d", path, shardID))
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(m)
}

// LoadMapFromFile opens the given file, decodes its gob contents,
// and returns the map. If anything goes wrong, an error is returned.
func LoadMapFromFile(path string, shardID int) (map[string]map[string]int, error) {
	file, err := os.Open(fmt.Sprintf("%s-%d", path, shardID))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	var m map[string]map[string]int
	if err := decoder.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

// Tokenize splits text into words, removes non-alphanumeric characters, and excludes stopwords.
func tokenize(content string) []string {
	nonAlphaNumeric := regexp.MustCompile(`[^a-zA-Z0-9]+`)

	words := strings.Fields(content)
	var tokens []string
	for _, word := range words {
		word = nonAlphaNumeric.ReplaceAllString(strings.ToLower(word), "")
		if word != "" && !stopWords[word] {
			tokens = append(tokens, word)
		}
	}
	return tokens
}

// FuzzyMatch checks if two strings are within a given Levenshtein distance.
func FuzzyMatch(a, b string, maxDistance int) bool {
	return levenshteinDistance(a, b) <= maxDistance
}

// LevenshteinDistance calculates the edit distance between two strings.
func levenshteinDistance(a, b string) int {
	m, n := len(a), len(b)
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}

	for i := 0; i <= m; i++ {
		for j := 0; j <= n; j++ {
			if i == 0 {
				dp[i][j] = j
			} else if j == 0 {
				dp[i][j] = i
			} else if a[i-1] == b[j-1] {
				dp[i][j] = dp[i-1][j-1]
			} else {
				dp[i][j] = 1 + min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1])
			}
		}
	}
	return dp[m][n]
}

// Min helper function.
func min(a, b, c int) int {
	if a < b && a < c {
		return a
	} else if b < c {
		return b
	}
	return c
}
