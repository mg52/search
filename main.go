package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mg52/search/search"
)

type HTTP struct {
	server *http.Server
	sec    *search.SearchEngineController
}

func ErrWriter(w http.ResponseWriter, err error) {
	var jsonBytes []byte
	jsonBytes, jsonErr := json.Marshal(map[string]interface{}{
		"err": fmt.Sprintf("%v", err),
	})
	if jsonErr != nil {
		jsonBytes = []byte(fmt.Sprintf("err: %v", err))
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	w.Write(jsonBytes)
}

func (ht *HTTP) Query(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		startTime := time.Now()
		filters := make(map[string][]interface{})
		// filters["year"] = append(filters["year"], 2017)

		for k, v := range r.URL.Query() {
			fmt.Printf("%s: %s\n", k, v)
		}
		query := r.URL.Query().Get("q")
		page := r.URL.Query().Get("page")

		pageInt, err := strconv.Atoi(page)
		if err != nil {
			ErrWriter(w, err)
			return
		}

		filterStr := r.URL.Query().Get("filter")
		if filterStr == "" {
			fmt.Println("No filters provided.")
		} else {
			// Split the filter string by commas
			filterItems := strings.Split(filterStr, ",")
			// Use a map where the key is the filter field (e.g., "year") and the value is a slice of values
			// filters := make(map[string][]string)
			for _, item := range filterItems {
				// Split each filter on the first ':' character.
				// SplitN is used in case there is an ':' in the value.
				parts := strings.SplitN(item, ":", 2)
				if len(parts) == 2 {
					key := parts[0]
					value := parts[1]
					filters[key] = append(filters[key], value)
				} else {
					fmt.Printf("Skipping invalid filter: %s\n", item)
				}
			}

			fmt.Printf("Parsed Filters: %+v\n", filters)
		}

		result := ht.sec.Search(query, pageInt, filters)

		duration := time.Since(startTime)
		fmt.Printf("Search took: %s for the word: %s\n", duration, query)

		jsonBytes, err := json.Marshal(map[string]interface{}{
			"status":     "success",
			"statusCode": 200,
			"query":      query,
			"response":   result,
			"duration":   duration,
		})
		if err != nil {
			ErrWriter(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonBytes)
		return
	default:
		ErrWriter(w, fmt.Errorf("unsupported method"))
		return
	}
}

func main() {
	filePath := flag.String("file", "products10m.json", "Path to the JSON file")
	seWorkers := flag.Int("workers", 1, "Number of search engines that run parallel")

	flag.Parse()
	workersValue := *seWorkers

	if *filePath == "" {
		fmt.Println("Please provide a file path using --file flag")
		os.Exit(1)
	}

	// Open the JSON file
	file, err := os.Open(*filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Read the file's content
	byteValue, err := io.ReadAll(file)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	var products []map[string]interface{}
	if err := json.Unmarshal(byteValue, &products); err != nil {
		fmt.Println("Error unmarshalling JSON:", err)
		return
	}

	// Define field weights
	weights := map[string]int{
		"name":        2,
		"description": 1,
		"tags":        1,
	}

	// Define field filters
	filters := map[string]bool{
		"year": true,
	}

	allIndexStartTime := time.Now()

	sec := search.NewSearchEngineController(weights, filters, 10, workersValue)
	sec.Index(products)

	// keys, trie := search.GenerateGlobalKeysAndTrie(products, weights)
	// var seList []*search.SearchEngine

	// for i := 0; i < workersValue; i++ {
	// 	seList = append(seList, search.NewSearchEngine(weights, filters, 2, 1000, 10, keys, trie))
	// }

	// for i := 0; i < workersValue; i++ {
	// 	chunkSize := (len(products) / workersValue)
	// 	dataStart := chunkSize * i
	// 	dataEnd := dataStart + chunkSize
	// 	if dataEnd > len(products) {
	// 		dataEnd = len(products)
	// 	}

	// 	// seList[i].CalculateAverageFieldCounts(products[dataStart:dataEnd])
	// 	// fmt.Println("Avg field counts:", seList[i].AverageFieldCounts)

	// 	seList[i].Index(products[dataStart:dataEnd])
	// }

	allIndexDuration := time.Since(allIndexStartTime)
	fmt.Printf("All Index took: %s\n", allIndexDuration)

	fmt.Printf("Loaded %d products from the JSON file\n", len(products))

	ht := &HTTP{
		server: &http.Server{
			Addr: ":8080",
		},
		sec: sec,
	}

	http.HandleFunc("/query", ht.Query)
	slog.Info("Listening...")
	slog.Error(ht.server.ListenAndServe().Error())

	/*
		go run . --file=products6m.json --workers=1

			test for GET:
			curl --location 'localhost:8080/query?q=comp'
			curl --location 'localhost:8080/query?q=people%20dro&start=0&stop=20'
	*/
}
