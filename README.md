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

All numbers are from an Apple M1 Pro. Dataset uses a synthetic corpus with 1 000+ unique vocabulary words, `title` (3–20 words) and `tags` (1–10 words) index fields, and a `year` (2000–2024) filter field. Queries are a realistic mix: 65% exact, 25% prefix, 10% misspelled; multi-term queries use 2–4 tokens.

### Indexing

| Corpus | Index time |
|-------:|----------:|
| 1 M docs | ~10.1 s |
| 5 M docs | ~1 m 16 s |

### Go benchmark — in-process (p50 / p99 / memory per query)

`go test ./internal/engine/ -bench=BenchmarkSearch -benchmem -benchtime=5s`

#### 1 M docs

| Mode | p50 | p99 | B/op | allocs/op |
|---|---:|---:|---:|---:|
| SingleTerm / NoFilter | 222 µs | 888 µs | 16 KB | 12 |
| SingleTerm / Filter   | 224 µs | 856 µs | 16 KB | 15 |
| MultiTerm  / NoFilter | 542 µs | 3 011 µs | 15 KB | 34 |
| MultiTerm  / Filter   | 281 µs | 1 167 µs | 14 KB | 36 |

#### 5 M docs

| Mode | p50 | p99 | B/op | allocs/op |
|---|---:|---:|---:|---:|
| SingleTerm / NoFilter | 894 µs | 3 551 µs | 16 KB | 12 |
| SingleTerm / Filter   | 1 028 µs | 3 883 µs | 17 KB | 15 |
| MultiTerm  / NoFilter | 3 560 µs | 24 594 µs | 49 KB | 41 |
| MultiTerm  / Filter   | 1 473 µs | 7 015 µs | 50 KB | 43 |

### HTTP load test — 1 M docs, 16 workers, 10 000 requests

50 % of requests include a `year` filter; 50 % are multi-term.

| Mode | avg | p50 | p95 | p99 | count |
|---|---:|---:|---:|---:|---:|
| **Overall** | 1.73 ms | 1.48 ms | 3.43 ms | 5.84 ms | 10 000 |
| Single / NoFilter | 1.56 ms | 1.42 ms | 2.71 ms | 4.51 ms | 2 546 |
| Single / Filter   | 1.63 ms | 1.43 ms | 2.90 ms | 5.48 ms | 2 524 |
| Multi  / NoFilter | 2.35 ms | 1.96 ms | 4.69 ms | 9.37 ms | 2 487 |
| Multi  / Filter   | 1.38 ms | 1.24 ms | 2.43 ms | 3.89 ms | 2 443 |

**RPS: 9 128** at 16 workers with 0 errors.

### HTTP load test — 5 M docs, 16 workers, 10 000 requests

50 % of requests include a `year` filter; 50 % are multi-term.

| Mode | avg | p50 | p95 | p99 | count |
|---|---:|---:|---:|---:|---:|
| **Overall** | 12.09 ms | 9.44 ms | 27.92 ms | 57.35 ms | 10 000 |
| Single / NoFilter | 8.61 ms | 7.58 ms | 17.45 ms | 26.26 ms | 2 576 |
| Single / Filter   | 10.16 ms | 8.92 ms | 19.73 ms | 31.41 ms | 2 451 |
| Multi  / NoFilter | 20.04 ms | 14.86 ms | 51.32 ms | 89.20 ms | 2 494 |
| Multi  / Filter   | 9.61 ms | 8.22 ms | 20.09 ms | 31.29 ms | 2 479 |

**RPS: 1 316** at 16 workers with 0 errors.

Multi/Filter is faster than Multi/NoFilter because the bitset pre-prunes the candidate set before the anchor-group scan.

---


## Quickstart

### Build & Run

```bash
# Run locally
go run ./cmd/service
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

### 1. Inverted Index

```go
DataMap map[string]map[uint32]int
// term → internalDocID → score
```

Every document is tokenized from the configured `IndexFields` (lowercased, non-alphanumeric stripped, stop-words removed). Each token gets a score of `100 000 ÷ (total tokens in that document)`. If the same token appears multiple times its scores are summed, so denser matches rank higher. This single map is the only data structure the search hot-path reads.

### 2. Internal IDs and Tombstones

Documents are identified internally by a monotonically increasing `uint32`:

| Map | Purpose |
|---|---|
| `ExternalToInternal` | caller’s string ID → current internal ID |
| `InternalToExternal` | internal ID → caller’s string ID |
| `Documents` | internal ID → raw field map |
| `DocDeleted` | internal ID → tombstoned? |

**Update semantics**: updating a document assigns a new internal ID and sets `DocDeleted[oldID] = true`. The old posting-list entries are never removed; searches skip tombstoned IDs at scan time. This makes updates O(1) on the index at the cost of some wasted posting-list entries, which is the right trade-off for write-heavy workloads.

### 3. Prefix and Fuzzy Matching

At index time, every new term seeds three auxiliary structures:

- **`Prefix map[string][]string`** — each prefix of length 1…n−1 maps to a list of matching full terms (capped at `MaxPrefixTerms = 400`). Prefix lookup is O(1) at query time.
- **`SymSpell`** — a Levenshtein-distance index used for fuzzy suggestions when prefix candidates are insufficient.

Single-term search tries prefix candidates first. If there are none it falls back to SymSpell suggestions. Multi-term search applies fuzzy expansion on all-but-last tokens, and prefix expansion on the last token (the partially-typed word).

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
**Per-query allocation**: zero for the common case (single field, single value) — see §6.

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

The engine uses a single `sync.RWMutex`:

- All search paths hold **`RLock`** — unlimited concurrent readers, no blocking between searches.
- Index writes (`addToDocumentIndex`, `BuildDocumentIndex`, `AddOrUpdateDocument`) hold **`Lock`** — exclusive, blocks new readers until the write completes.

`SearchOneTerm` and `SearchMultiTerms` acquire `RLock` **once at the top** and hold it across both filter resolution and the posting-map scan:

```
RLock acquired
  └─ applyFilterLocked()   →  returns direct []uint64 reference (no copy)
  └─ posting-map scan      →  filterBitTest reads from same reference
RLock released
```

For the common case (single field, single value), `applyFilterLocked` returns a slice header pointing directly into `se.FilterBits` with zero allocation. The RLock guarantees the backing array is immutable for the duration of the search. This is why filtered and unfiltered queries show identical B/op in the benchmarks.

### 7. Multi-Term Search: Anchor-Group Strategy

Multi-term queries use a boolean AND-across-groups, OR-within-group model. A "group" is a set of synonyms or expansions for one query token.

Rather than intersecting all groups eagerly, the engine picks the **smallest group** (fewest total posting entries) as the anchor and iterates only its candidates. For each candidate it checks membership in every other group with a map lookup — O(1) per group. This avoids materialising a full intersection set and keeps multi-term search fast even when individual terms are common.

Score for a matching document is the sum of its scores across all matched groups.

### 8. Persistence

`SaveAll` serialises only the raw document store and metadata (IDs, field config) to a single gob file. `LoadAll` restores the documents and then rebuilds all derived structures — `DataMap`, `FilterBits`, `Prefix`, `SymSpell` — by replaying the tokenisation pass. This keeps the snapshot compact and means the on-disk format never needs a schema migration when internal data structures change.


## HTTP API

All endpoints return JSON and use HTTP status codes (`201 Created`, `200 OK`, `400 Bad Request`, `404 Not Found`, `405 Method Not Allowed`, `409 Conflict`, `500 Internal Server Error`).

### 1. Create Index

Creates an empty index with the given fields and filters.

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

Uploads a JSON file of documents and indexes them all.

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

Upserts one document. If the `id` already exists the old version is tombstoned and a new one is created.

```bash
curl -X POST 'http://localhost:8080/document?indexName=products' \
  -H 'Content-Type: application/json' \
  -d '{
    "document": { "id": "14", "name": "New Name", "tags": ["x"], "year": "2021" }
  }'
```

### 5. Delete Single Document

Tombstones the document with the given `id`.

```bash
curl -X DELETE 'http://localhost:8080/document?indexName=products&id=14'
```

### 6. Save Index

Writes `engine.gob` to `/data/<indexName>/engine.gob` (or `$INDEX_DATA_DIR/<indexName>/engine.gob`).

```bash
curl -X POST http://localhost:8080/save-controller \
  -H 'Content-Type: application/json' \
  -d '{ "indexName": "products" }'
```

### 7. Load Index

Loads the snapshot and rebuilds all derived indexes from stored documents.

```bash
curl -X POST http://localhost:8080/load-controller \
  -H 'Content-Type: application/json' \
  -d '{ "indexName": "products" }'
```

### 8. Health Check

```bash
curl http://localhost:8080/health
```

Response:

```json
{ "status": "ok", "duration": "5µs", "durationMs": 0 }
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

## Load Testing

### Step 1 — generate the dataset

```bash
# 1 million documents (~200 MB JSON)
go run ./cmd/datagen -count 1000000 -out data.json

# 5 million documents
go run ./cmd/datagen -count 5000000 -out data5m.json
```

Generated documents look like:

```json
{"id":"d0","title":"forge silent lunar craft vast","tags":"swift rural","year":2017}
```

Fields: `title` (3–20 words), `tags` (1–10 words), `year` (2000–2024).

### Step 2 — start the server and load data

```bash
go run ./cmd/service

# create index — title + tags indexed, year as filter field
curl -X POST http://localhost:8080/create-index \
  -H 'Content-Type: application/json' \
  -d '{"indexName":"bench","indexFields":["title","tags"],"filters":["year"],"resultCount":100}'

# upload and index the JSON file
curl -X POST 'http://localhost:8080/add-to-index?indexName=bench' \
  -F 'file=@data.json'
```

### Step 3 — run the load test

```bash
go run ./cmd/loadtest \
  -index bench \
  -requests 10000 \
  -workers 16 \
  -filter-pct 50 \
  -multi-pct 50
```

| Flag | Default | Description |
|---|---|---|
| `-url` | `http://localhost:8080/search` | Search endpoint |
| `-index` | `bench` | Index name |
| `-workers` | `8` | Parallel goroutines |
| `-requests` | `10000` | Total requests |
| `-filter-pct` | `50` | % of requests with a year filter |
| `-multi-pct` | `50` | % of requests using multi-term queries |
| `-timeout` | `10s` | Per-request HTTP timeout |
| `-seed` | random | RNG seed for reproducibility |

---

## License

MIT © mg52
