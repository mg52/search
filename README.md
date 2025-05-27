# Go In-Memory Search Engine

A lightweight, in-memory inverted-index search engine in Go, with HTTP handlers and on-disk persistence.  

## Features

- **Full-text indexing** on arbitrary JSON documents  
- **Prefix & fuzzy matching** via a Trie + edit-distance  
- **Sharded engines** for parallel indexing & search  
- **Filters** on arbitrary document fields  
- **HTTP API** for index creation, search, updates, removal  
- **Persistence**: snapshot & load shards as Gob files under `/data/<indexName>/shard-<id>/`  
- **Docker-ready**: runs as an unprivileged user, persists under a mounted volume  

---

## Quickstart

### Build & Run

```bash
# Build the binary
go build -o searchengine .

# Run locally, persisting index data in ./data
./searchengine --data-dir ./data
````

### Docker

```bash
# Build the Docker image
docker build -t searchengine:latest .

# Run with a named volume at /data
docker run -d \
  -p 8080:8080 \
  -v search_data:/data \
  --name searchengine \
  searchengine:latest
```

---

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

--boundary
Content-Disposition: form-data; name="file"; filename="docs.json"
Content-Type: application/json

[ { "id":"1", "name":"foo", "tags":["a","b"], "year":2020 }, … ]
--boundary--
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
    { "ID":"11","ScoreWeight":66666 },
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

```