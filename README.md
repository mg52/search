![build](https://github.com/mg52/search/actions/workflows/go.yml/badge.svg)

# In-Memory Search Engine

A lightweight, fast, in-memory inverted-index search engine with HTTP handlers and on-disk persistence.  

## Features

- **Full-text indexing** on arbitrary JSON documents  
- **Prefix & fuzzy matching** prefix lists + SymSpell with Levenshtein distance calculation 
- **Filters** on arbitrary document fields (OR within a field, AND across fields)  
- **HTTP API** for index creation, search, single-doc upsert/delete, bulk add 
- **Persistence**: saves documents + index metadata and rebuilds all derived indexes on load  
- **Docker-ready**: runs as an unprivileged user, persists under a mounted volume  

---

## Benchmark

**Latency statistics for 5 million data**

|     Avg |      P50 |       P95 |       P99 | Index Time |
|-------: | -------: | --------: | --------: | ---------: |
| 6.76 ms |  3.52 ms |  20.95 ms |  56.55 ms |      1m42s |


#### Details

* **Total requests**: 10000 requests with 8 parallel workers in 9 seconds for 5 million data
* **Query lengths**: 1 to 4 terms e.g., `Last Night`, `The Only Thing Br`, `Modern`, `Real Ghod Time`
* **Prefix matching**: e.g., `Modern ta` matches `Modern Talking`
* **Typo tolerance**: \~10% of queries include deliberate misspellings (e.g., `Never Was an mngel` for “angel”)
* **Dataset**: MusicBrainz
* **Record format** (JSON lines):

  ```json
  {"artist":"Modern Talking","song":"Heart of an Angel","id":"c7eda459-c11c-362f-a5c9-2c108c4a27e4","album":"Universe: The 12th Album"}
  {"artist":"Modern Talking","song":"Who Will Be There","id":"366f83d1-bd0a-3ed8-9974-b148ae6d6dd9","album":"Universe: The 12th Album"}
  ```
* **Indexed fields**: `artist`, `song`, `album`
* **Machine**: Used MacBook Air M3, 24GB Memory
* **Pagination**: 10 pages per request
* loadtest/loadtest.go file is used to perform the test

---


## Quickstart

### Build & Run

```bash
# Run locally
cd cmd/service/
go run . 
````

### Docker

```bash
# Build the Docker image
docker build -t searchengine:latest .

# Run the container
docker run -d \
  -p 8080:8080 \
  -v search_data:/data \
  -e INDEX_DATA_DIR=/data \
  --name searchengine \
  searchengine:latest
```


---


## How It Works

### Index Design

This engine keeps the core inverted index in memory and uses tombstones for updates/deletes.

#### 1. **Inverted Index (`DataMap`)**

   ```go
   DataMap map[string]map[uint32]int
   // term -> internalDocID -> score
   ```

   * Each document is tokenized from the configured `IndexFields`.
   * Each token contributes a per-document score of `100000 ÷ (total tokens in that document)`. 
   * If a token appears multiple times in a doc, its score is summed.

#### 2. **Internal IDs + Tombstones**

   Documents are stored and referenced by an internal numeric ID:

   * `ExternalToInternal`: external string ID → current internal ID
   * `InternalToExternal`: internal ID → external ID
   * `DocDeleted[internalID] = true` tombstones old versions
  
   Update semantics:

   * Updating a doc creates a new internal ID and tombstones the old internal ID.
   * Searches always skip tombstoned internal IDs.
   
   This avoids expensive deletions from posting lists at update time.

#### 3. **Prefix & Fuzzy Structures**

   During indexing, tokens are also inserted into:

   * `Prefix map[string][]string`: a precomputed list of prefix → candidate terms (capped by `MaxPrefixTerms`)
   * `Keys`: a set of known terms (fast exact existence checks)
   * `SymSpell`: used to suggest fuzzy terms when prefix candidates are insufficient

  Single-term search prefers prefix candidates; when prefix isn’t enough it falls back to SymSpell suggestions.


## HTTP API

All endpoints return JSON and use HTTP status codes (`201 Created`, `200 OK`, `500` on errors).

### 1. Create Index

```http
POST /create-index
Content-Type: application/json

{
  "indexName":   "products",
  "indexFields": ["name","tags"],
  "filters":     ["year"],
  "pageCount":   10
}
```

Creates an *empty* index.

### 2. Add to Index

```http
POST /add-to-index?indexName=products
Content-Type: multipart/form-data
Content-Disposition: form-data; name="file"; filename="docs.json"
Content-Type: application/json

[ { "id":"1", "name":"foo", "tags":["a","b"], "year":"2020" }, ... ]
```

### 3. Search

```http
GET /search?index=products&q=laptop&page=0&filter=year:2020,category:electronics
```

* `index`: name of the index
* `q`: search query
* `page`: zero-based page number
* `filter`: comma-separated `field:value` pairs


### 4. Add or Update Single Document

```http
POST /document?indexName=products
Content-Type: application/json

{
  "document": { "id":"14", "name":"New Name", "tags":["x"], "year":"2021" }
}
```

Upserts one document (creates a new internal ID; tombstones old version if it exists).

### 6. Delete Single Document

```http
DELETE /document?indexName=products&id=14
```

Tombstones the current version of the document.

### 7. Save & Load Index (Persistence)

* **Save**:

```http
POST /save-controller
Content-Type: application/json

{ "indexName":"products" }
```

Writes `engine.gob` under `/data/<indexName>/engine.gob` (or `INDEX_DATA_DIR` if set).

* **Load**:

```http
POST /load-controller
Content-Type: application/json

{ "indexName":"products" }
```

Loads the snapshot and rebuilds indexes from stored documents.

### 8. Health Check

```http
GET /ping
```

Returns:

```json
{ "status":"ok", "duration":"5µs", "durationMs":0 }
```

---

## Testing

```bash
# Run tests without caching
go test -count=1 ./...

# With race detection
go test -race -count=1 ./...

# With coverage report
go test -race -count=1 ./... \
  -coverpkg=./... \
  -coverprofile=coverage.out

go tool cover -func=coverage.out
```

---

## License

MIT © mg52
