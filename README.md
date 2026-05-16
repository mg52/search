![build](https://github.com/mg52/search/actions/workflows/go.yml/badge.svg)

# In-Memory Search Engine

A lightweight, fast, in-memory inverted-index search engine with HTTP handlers and on-disk persistence.  

## Features

- **Full-text indexing** on arbitrary JSON documents  
- **Prefix & fuzzy matching** — prefix map for instant completions + SymSpell for Levenshtein fuzzy suggestions  
- **Filters** on arbitrary document fields (OR within a field, AND across fields)  
- **HTTP API** for index creation, search, single-doc upsert/delete, bulk add  
- **Persistence** — saves documents + metadata and rebuilds all derived indexes on load  
- **Docker-ready** — runs as an unprivileged user, persists under a mounted volume  

---

## Benchmark

All numbers are from an Apple M1 Pro. Dataset: 1 000 000 and 5 000 000 documents, **100 000 unique vocabulary words**, `title` (3–20 words) and `tags` (1–10 words) index fields, `year` (2000–2024) filter field. Query mix: 65 % exact, 25 % prefix, 10 % misspelled; multi-term queries use 2–4 tokens, 10 000 measurements per mode after 1 000 warmup iterations.

### In-process benchmark

```bash
go run ./cmd/bench benchmark -data data.json   -vocab vocab.txt -queries 10000 -warmup 1000
go run ./cmd/bench benchmark -data data5m.json -vocab vocab.txt -queries 10000 -warmup 1000
```

#### 1 M docs — heap delta ~800 MB

| Mode | avg | p50 | p99 | B/op | allocs/op |
|---|---:|---:|---:|---:|---:|
| SingleTerm / NoFilter | 29.4 µs | 21.2 µs |  88.9 µs | 14 136 | 10 |
| SingleTerm / Filter   |  8.8 µs |  6.6 µs |  26.1 µs |  3 457 | 13 |
| MultiTerm  / NoFilter | 16.6 µs | 12.9 µs |  73.2 µs |  2 831 | 25 |
| MultiTerm  / Filter   | 12.7 µs | 12.1 µs |  32.8 µs |  2 862 | 27 |

#### 5 M docs — heap delta ~2 773 MB

| Mode | avg | p50 | p99 | B/op | allocs/op |
|---|---:|---:|---:|---:|---:|
| SingleTerm / NoFilter | 67.7 µs | 40.4 µs | 318.6 µs | 14 148 | 10 |
| SingleTerm / Filter   | 53.3 µs | 36.5 µs | 230.7 µs |  6 715 | 13 |
| MultiTerm  / NoFilter | 82.0 µs | 41.8 µs | 594.6 µs |  3 472 | 25 |
| MultiTerm  / Filter   | 46.9 µs | 43.0 µs | 173.9 µs |  3 492 | 27 |

Filter queries are faster than no-filter equivalents because the bitset pre-prunes the candidate set before the posting-list scan.

---

## Quickstart

### Build & Run

```bash
go run ./cmd/service
```

### Docker

```bash
docker build -t searchengine:latest .

docker run -d \
  -p 8080:8080 \
  -v search_data:/data \
  -e INDEX_DATA_DIR=/data \
  --name searchengine \
  searchengine:latest
```

---

## How It Works

### 1. Inverted Index

```go
DataMap map[string]map[uint32]int
// term → internalDocID → score
```

Every document is tokenized from the configured `IndexFields`. Tokenization lowercases the text, strips every non-alphanumeric character (via a compiled regexp), and drops a fixed stop-word list (`a`, `the`, `and`). Each surviving token gets a score of `100 000 ÷ (total tokens in that document)`. If the same token appears multiple times its scores are summed, so denser matches rank higher. This single map is the only data structure the search hot-path reads.

### 2. Internal IDs and Tombstones

Documents are identified internally by a monotonically increasing `uint32`:

| Map | Purpose |
|---|---|
| `ExternalToInternal` | caller's string ID → current internal ID |
| `InternalToExternal` | internal ID → caller's string ID |
| `Documents` | internal ID → raw field map |
| `DocDeleted` | internal ID → tombstoned? |

**Update semantics**: updating a document assigns a new internal ID and sets `DocDeleted[oldID] = true`. The old posting-list entries are never removed; searches skip tombstoned IDs at scan time. This makes updates O(1) on the index at the cost of some wasted posting-list entries, which is the right trade-off for write-heavy workloads.

### 3. Prefix and Fuzzy Matching

At index time, every new term seeds two auxiliary structures:

- **`Prefix map[string][]string`** — maps every prefix of a term to a list of up to `MaxPrefixTerms` (400) completions. Lookup is a single map read — O(1). At query time the first 3 completions are used for single-term prefix search and up to 40 for the last token of a multi-term query.
- **`SymSpell`** — a Levenshtein-distance index used for fuzzy suggestions when the query token has no prefix candidates (i.e. it is not a known prefix of any indexed term). Only terms with length ≥ 4 are added to SymSpell to avoid noise from very short tokens.

Single-term search tries prefix candidates first (up to 3). If there are none it falls back to SymSpell suggestions. Multi-term search applies fuzzy expansion on all-but-last tokens, and prefix expansion on the last token (the partially-typed word), returning up to 40 prefix completions.

### 4. Bitset Filters

Filter fields (e.g. `year`) are stored as permanent per-value bitsets instead of per-query maps:

```go
FilterBits map[string][]uint64
// "year:2020" → []uint64  (bit i set ↔ internalDocID i matches)
```

At index time each matching internal ID flips one bit: `bits[id>>6] |= 1 << (id&63)`. At query time a single bit test replaces a map lookup:

```
filterBitTest(allowed, id)  →  bits[id>>6] & (1<<(id&63)) != 0
```

**Memory**: ~1.25 MB per filter value per 1 M documents, paid once at index time.  
**Per-query allocation**: zero for the common case (single field, single value) — `applyFilterLocked` returns a direct slice reference into `se.FilterBits` with no copy.

Multi-value filters within one field are ORed (bitwise union); multiple fields are ANDed (bitwise intersection). Both operations produce a new `[]uint64` and are O(N/64) where N is the highest internal ID.

### 5. Top-k Extraction with a Specialized Min-Heap

Search maintains a bounded min-heap of the top-k candidates:

```go
type internalHit struct { id uint32; score int }
```

The heap operations (`heapPushHit`, `heapReplaceTop`, `siftDownHit`) are inlined directly over `[]internalHit` with no interface dispatch or boxing — unlike `container/heap` which boxes every element into `any`. At k = 100 this saves ~200 allocations per query.

**Fill phase**: while `len(h) < k`, push every passing candidate. Once full, replace the root only when `candidate.score > h[0].score` (the current minimum).

**Extraction phase**: repeated inline heap-pop fills the result slice from the last index down, yielding results in descending score order without an extra sort pass.

### 6. Concurrency and Zero-Copy Filter Resolution

The engine uses a single `sync.RWMutex` plus a lock-free `termSet`:

- All search paths hold **`RLock`** — unlimited concurrent readers, no blocking between searches.
- Index writes (`InsertDocs`, `BuildDocumentIndex`, `AddOrUpdateDocument`) hold **`Lock`** — exclusive, blocks new readers until the write completes.
- **`termSet sync.Map`** — a lock-free set of every indexed term. `SingleTermSearch` and `MultiTermSearch` check exact-term existence here before acquiring `RLock`, avoiding a mutex round-trip for the common case.

`SearchOneTerm` and `SearchMultiTerms` acquire `RLock` **once at the top** and hold it across both filter resolution and the posting-map scan:

```
RLock acquired
  └─ applyFilterLocked()   →  returns direct []uint64 reference (no copy)
  └─ posting-map scan      →  filterBitTest reads from same reference
RLock released
```

For the common case (single field, single value), `applyFilterLocked` returns a slice header pointing directly into `se.FilterBits` with zero allocation.

### 7. Multi-Term Search: Anchor-Group Strategy

Multi-term queries use a boolean AND-across-groups, OR-within-group model. A "group" is a set of synonyms or expansions for one query token.

Rather than intersecting all groups eagerly, the engine picks the **smallest group** (fewest total posting entries) as the anchor and iterates only its candidates. For each candidate it checks membership in every other group with a map lookup — O(1) per group. This avoids materialising a full intersection set and keeps multi-term search fast even when individual terms are common.

Score for a matching document is the sum of its scores across all matched groups.

### 8. Persistence

`SaveAll` serialises only the raw document store and metadata (IDs, field config) to a single gob file. `LoadAll` restores the documents and then rebuilds all derived structures — `DataMap`, `FilterBits`, `Prefix`, `SymSpell` — by replaying the tokenisation pass. This keeps the snapshot compact and means the on-disk format never needs a schema migration when internal data structures change.

---

## HTTP API

All endpoints return JSON and use HTTP status codes (`201 Created`, `200 OK`, `400 Bad Request`, `404 Not Found`, `405 Method Not Allowed`, `409 Conflict`, `500 Internal Server Error`).

### 1. Create Index

```bash
curl -X POST http://localhost:8080/create-index \
  -H 'Content-Type: application/json' \
  -d '{
    "indexName":   "products",
    "indexFields": ["name", "tags"],
    "filters":     ["year"],
    "resultCount": 10
  }'
```

### 2. Bulk Add to Index

```bash
curl -X POST 'http://localhost:8080/add-to-index?indexName=products' \
  -F 'file=@docs.json'
```

`docs.json` must be a JSON array:

```json
[
  { "id": "1", "name": "foo", "tags": ["a", "b"], "year": "2020" },
  { "id": "2", "name": "bar", "tags": ["c"],       "year": "2021" }
]
```

### 3. Search

```bash
# Simple query
curl 'http://localhost:8080/search?index=products&q=laptop'

# With a single filter
curl 'http://localhost:8080/search?index=products&q=laptop&filter=year:2020'

# With multiple filters (AND across fields, OR within a field)
curl 'http://localhost:8080/search?index=products&q=laptop&filter=year:2020,year:2021,category:electronics'
```

| Param | Description |
|---|---|
| `index` | Index name |
| `q` | Search query (single or multi-term) |
| `filter` | Comma-separated `field:value` pairs |

### 4. Add or Update Single Document

```bash
curl -X POST 'http://localhost:8080/document?indexName=products' \
  -H 'Content-Type: application/json' \
  -d '{
    "document": { "id": "14", "name": "New Name", "tags": ["x"], "year": "2021" }
  }'
```

### 5. Delete Single Document

```bash
curl -X DELETE 'http://localhost:8080/document?indexName=products&id=14'
```

### 6. Save Index

```bash
curl -X POST http://localhost:8080/save-controller \
  -H 'Content-Type: application/json' \
  -d '{ "indexName": "products" }'
```

### 7. Load Index

```bash
curl -X POST http://localhost:8080/load-controller \
  -H 'Content-Type: application/json' \
  -d '{ "indexName": "products" }'
```

### 8. Health Check

```bash
curl http://localhost:8080/health
```

```json
{ "status": "ok", "duration": "5µs", "durationMs": 0 }
```

---

## Testing

```bash
# Run all tests
go test -count=1 ./...

# With race detection
go test -race -count=1 ./...

# With coverage
go test -race -count=1 ./... -coverpkg=./... -coverprofile=coverage.out
go tool cover -func=coverage.out
```

---

## Benchmarking & Load Testing

All data-generation and testing tools live in a single binary under `cmd/bench`.

```
go run ./cmd/bench <command> [flags]

commands:
  vocab      generate vocabulary.txt
  datagen    generate a JSON document file
  benchmark  run an in-process latency benchmark from a JSON file
  loadtest   hammer a running HTTP search endpoint
```

### Step 1 — generate vocabulary

```bash
go run ./cmd/bench vocab -size 100000 -out vocab.txt
```

| Flag | Default | Description |
|---|---|---|
| `-size` | `100000` | Number of unique words |
| `-out` | `vocab.txt` | Output file |
| `-seed` | `42` | RNG seed |

### Step 2 — generate documents

```bash
# 1 million documents
go run ./cmd/bench datagen -count 1000000 -vocab vocab.txt -out data.json

# 5 million documents
go run ./cmd/bench datagen -count 5000000 -vocab vocab.txt -out data5m.json
```

Generated documents look like:

```json
{"id":"d0","title":"rukpttue nhuks ghejk vgzadxl ptneuv","tags":"ghejk nhuks","year":2017}
```

Fields: `title` (3–20 words), `tags` (1–10 words), `year` (2000–2024).

| Flag | Default | Description |
|---|---|---|
| `-count` | `1000000` | Number of documents |
| `-vocab` | `vocab.txt` | Vocabulary file (from `vocab` command) |
| `-out` | `data.json` | Output JSON file |
| `-seed` | `42` | RNG seed |

### Step 3 — in-process benchmark

Reads the JSON file, builds the engine in memory, then runs search queries measuring latency. No HTTP server needed.

```bash
go run ./cmd/bench benchmark -data data.json -vocab vocab.txt -queries 10000 -warmup 1000
```

| Flag | Default | Description |
|---|---|---|
| `-data` | `data.json` | JSON data file |
| `-vocab` | `vocab.txt` | Vocabulary file |
| `-queries` | `5000` | Queries measured per mode |
| `-warmup` | `500` | Warmup iterations before measuring |
| `-result-size` | `100` | Top-k result size |
| `-seed` | `99` | RNG seed for query generation |

### Step 4 — HTTP load test

Start the service and load data, then run the load test.

```bash
go run ./cmd/service

# create index
curl -X POST http://localhost:8080/create-index \
  -H 'Content-Type: application/json' \
  -d '{"indexName":"bench","indexFields":["title","tags"],"filters":["year"],"resultCount":100}'

# upload 1 M docs
curl -X POST 'http://localhost:8080/add-to-index?indexName=bench' \
  -F 'file=@data.json'

# run load test
go run ./cmd/bench loadtest \
  -url http://localhost:8080/search \
  -vocab vocab.txt \
  -index bench \
  -requests 10000 \
  -workers 16 \
  -filter-pct 50 \
  -multi-pct 50
```

| Flag | Default | Description |
|---|---|---|
| `-url` | `http://localhost:8080/search` | Search endpoint |
| `-vocab` | `vocab.txt` | Vocabulary file |
| `-index` | `bench` | Index name |
| `-workers` | `8` | Parallel goroutines |
| `-requests` | `10000` | Total requests |
| `-filter-pct` | `50` | % of requests with a year filter |
| `-multi-pct` | `50` | % of multi-term queries |
| `-timeout` | `10s` | Per-request HTTP timeout |
| `-seed` | random | RNG seed for reproducibility |
| `-keepalive` | `true` | Use HTTP keep-alive |

---

## License

MIT © mg52
