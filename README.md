# Go In-Memory Search Engine

A lightweight, in-memory inverted-index search engine in Go, with HTTP handlers and on-disk persistence.  

## Features

- **Full-text indexing** on arbitrary JSON documents  
- **Prefix & fuzzy matching** via a Trie + Levenshtein distance calculation 
- **Sharded engines** for parallel indexing & search  
- **Filters** on arbitrary document fields  
- **HTTP API** for index creation, search, updates, removal  
- **Persistence**: snapshot & load shards as Gob files under `/data/<indexName>/shard-<id>/`  
- **Docker-ready**: runs as an unprivileged user, persists under a mounted volume  

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

This engine uses two primary in-memory structures to enable fast, score-based search:

#### 1. **Inverted Index (`Data`)**

   ```go
   Data map[string]map[string]int
   // └── term → (docID → weight)
   ```

   * Whenever a document is indexed, each token (word) is recorded under `Data[token][docID] = weight`.
   * The weight for each occurrence is calculated as `100000 ÷ (total tokens in that document)`. If the same token appears multiple times in a document, its weights are summed.

#### 2. **Sorted Posting List (`ScoreIndex`)**

   ```go
   ScoreIndex map[string][]Document
   // └── term → sorted slice of {ID, ScoreWeight}
   ```

   * After indexing completes, we call `BuildScoreIndex()`. For each term, this method collects all `(docID, weight)` pairs from `Data[term]`, sorts them in descending order by weight, and stores the result as `ScoreIndex[term]`.
   * Because `ScoreIndex[term]` is already sorted, a single-term lookup is extremely fast: to retrieve page 0 of “book,” the engine simply returns `ScoreIndex["book"][0:PageSize]` (which is O(1) to look up and O(PageSize) to copy).

#### 3. **Keys and Trie**

   * During indexing, every unique token is also added to a `Keys` map (for quick existence checks) and inserted into a `Trie` (for prefix lookups).
   * The trie supports up to 5 prefix completions, and `Keys` is used for fuzzy matching (Levenshtein distance ≤ 2) when no exact or prefix match is found.

---

### Single-Term Search Example

Assume we have indexed these two documents (indexing only the `"name"` and `"tags"` fields):

```json
{ "id": "17", "name": "affordable book", "tags": ["book","shop"], "year": 2015 }
{ "id": "42", "name": "used book sale", "tags": ["book","discount"], "year": 2018 }
```

#### 1. **Tokenize & Weight**

   * Document 17’s tokens: `["affordable","book","book","shop"]` (4 tokens total)
   * Each occurrence of “book” gets a weight of ⌊100000 ÷ 4⌋ = 25000, and since “book” appears twice, `Data["book"]["17"] = 50000`.
   * Document 42’s tokens: `["used","book","sale","book","discount"]` (5 tokens)
   * Each “book” is weighted ⌊100000 ÷ 5⌋ = 20000, so `Data["book"]["42"] = 40000`.

#### 2. **BuildScoreIndex**

   * After indexing both docs, `BuildScoreIndex()` sorts the posting list for “book” by weight:

     ```go
     ScoreIndex["book"] = []Document{
       {ID:"17", ScoreWeight:50000},
       {ID:"42", ScoreWeight:40000},
       // …any other docs that contain “book”
     }
     ```

#### 3. **Search**
   A request like:

   ```
   GET /search?index=products&q=book&page=0
   ```

   simply returns:

   ```json
   [
     { "ID": "17", "ScoreWeight": 50000, "Data": {...} },
     { "ID": "42", "ScoreWeight": 40000, "Data": {...} }
   ]
   ```

   in descending weight order.
   If “book” were not in the `Keys` map, the engine would attempt a fuzzy match (edit distance ≤ 2).

---

### Multi-Term Search Example

Now search for `"affordable book"`:

#### 1. **Resolve Tokens**

   * The first token (“affordable”) is treated as an exact match if it exists in `Keys`.
   * The last token (“book”) is treated as a prefix query (i.e. `Trie.SearchPrefix("book")`), returning up to 5 completions (in the first search) such as `["book","booking","booked"]`. If there is no data within 5 prefix search, it makes a second search with 1000 prefix completions.

#### 2. **Primary Posting List**

   * We start by scanning `ScoreIndex["affordable"]` (e.g. `[{ID:"17",Weight:60000}, …]`) in descending order.

#### 3. **Combine & Filter**

   * For each document in descending “affordable” weight, we check if that `docID` also appears under any of the prefix matches (`Data["book"]`, `Data["booking"]`, `Data["booked"]`, etc.).
   * If so, we compute a combined weight, for example:

     ```
     Data["affordable"]["17"] (60000) + Data["book"]["17"] (50000) = 110000
     ```
   * We collect `{ID:"17", ScoreWeight:110000}` into a result slice.
   * We stop scanning once we have enough documents to fill the requested page (e.g. 10 matches).
   * A document must match at least one prefix term to be included.

#### 4. **Return Sorted Page**

   * Since we scanned in descending “affordable” order and only kept docs that matched one of the prefix terms, our partial list is already roughly sorted by combined weight. We then truncate to exactly `PageSize` results and return that slice.

If only document 17 contains both “affordable” and any “book\*” prefix, then:

```json
[
  { "ID": "17", "ScoreWeight": 110000, "Data": {...} }
]
```


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
  "pageCount":   10,
  "workers":     8
}
```

Creates an *empty* index.
**Response**:

```json
{
  "indexName": "products",
  "duration": "123µs",
  "durationMs": 0
}
```

### 2. Add to Index

```http
POST /add-to-index?indexName=products
Content-Type: multipart/form-data
Content-Disposition: form-data; name="file"; filename="docs.json"
Content-Type: application/json

[ { "id":"1", "name":"foo", "tags":["a","b"], "year":2020 }, … ]
```

Appends documents from a JSON array into an existing index.
**Response**:

```json
{
  "indexName": "products",
  "addedCount": 100,
  "duration": "12ms",
  "durationMs": 12
}
```

### 3. Search

```http
GET /search?index=products&q=laptop&page=0&filter=year:2020,category:electronics
```

* `index`: name of the index
* `q`: search query
* `page`: zero-based page number
* `filter`: comma-separated `field:value` pairs

**Response**:

```json
{
  "status": "success",
  "statusCode": 200,
  "index": "products",
  "query": "laptop",
  "response": [
    { "ID":"11","ScoreWeight":66666, "Data": {...} },
    …
  ],
  "duration": "1.23ms",
  "durationMs": 1
}
```

### 4. Add or Update Single Document

```http
POST /add-or-update-document?index=products
Content-Type: application/json

{ "id":"14", "name":"New Name", "tags":["x"], "year":2021 }
```

Inserts or updates one document, re-indexing as needed.
**Response**:

```json
{
  "status":"success",
  "statusCode":200,
  "index":"products",
  "document": { /* the doc you sent */ }
}
```

### 5. Bulk Add or Update

```http
POST /add-or-update-document-bulk?index=products
Content-Type: application/json

[
  { "id":"2", … },
  { "id":"3", … }
]
```

Parallelized with a worker pool.
**Response**:

```json
{
  "status":"success",
  "statusCode":200,
  "index":"products",
  "documentCount":2,
  "duration":"5ms",
  "durationMs":5
}
```

### 6. Remove Document by ID

```http
DELETE /remove-document-by-id?index=products&id=14
```

Removes a document from all shards.
**Response**:

```json
{
  "status":"success",
  "statusCode":200,
  "index":"products",
  "removedID":"14"
}
```

### 7. Save & Load Index (Persistence)

* **Save** writes each shard under `/data/<indexName>/shard-<n>/…`:

  ```http
  POST /save-controller
  Content-Type: application/json

  { "indexName":"products" }
  ```

* **Load** reconstructs them at startup:

  ```http
  POST /load-controller
  Content-Type: application/json

  { "indexName":"products" }
  ```

**Response**:

```json
{
  "status":"success",
  "statusCode":200,
  "indexName":"products",
  "shards":3,
  "duration":"10ms",
  "durationMs":10
}
```

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
