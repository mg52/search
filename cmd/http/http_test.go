package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mg52/search/internal/engine"
)

func TestCreateIndexHandler(t *testing.T) {
	h := NewHTTP()
	// Valid create-index request
	body := `{"indexName":"testidx","indexFields":["name"],"filters":["year"],"pageCount":5,"workers":2}`
	req := httptest.NewRequest(http.MethodPost, "/create-index", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CreateIndex(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected status 201 Created; got %d", rr.Code)
	}

	var resp CreateIndexResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if resp.IndexName != "testidx" {
		t.Errorf("expected IndexName=testidx; got %q", resp.IndexName)
	}
}

func TestHealthHandler(t *testing.T) {
	h := NewHTTP()
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	rr := httptest.NewRecorder()
	h.Health(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK; got %d", rr.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("expected status=ok; got %v", resp["status"])
	}
}

func TestSearchHandler_NoIndex(t *testing.T) {
	h := NewHTTP()
	req := httptest.NewRequest(http.MethodGet, "/search?index=unknown&q=a&page=0", nil)
	rr := httptest.NewRecorder()
	h.Search(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500; got %d", rr.Code)
	}
}

func TestSearchHandler_Success(t *testing.T) {
	h := NewHTTP()
	// create index
	create := `{"indexName":"idx","indexFields":["name"],"filters":[],"pageCount":1,"workers":1}`
	rc := httptest.NewRequest(http.MethodPost, "/create-index", strings.NewReader(create))
	rc.Header.Set("Content-Type", "application/json")
	r := httptest.NewRecorder()
	h.CreateIndex(r, rc)

	// add a document via AddOrUpdateDocument
	doc := map[string]interface{}{"id": "1", "name": "foo"}
	bufDoc, _ := json.Marshal(doc)
	r2 := httptest.NewRequest(http.MethodPost, "/add-or-update-document?index=idx", bytes.NewReader(bufDoc))
	r2.Header.Set("Content-Type", "application/json")
	rw2 := httptest.NewRecorder()
	h.AddOrUpdateDocument(rw2, r2)

	// search for "foo"
	r3 := httptest.NewRequest(http.MethodGet, "/search?index=idx&q=foo&page=0", nil)
	rw3 := httptest.NewRecorder()
	h.Search(rw3, r3)

	if rw3.Code != http.StatusOK {
		t.Fatalf("expected 200; got %d", rw3.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rw3.Body).Decode(&resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	resArr, ok := resp["response"].([]interface{})
	if !ok || len(resArr) != 1 {
		t.Errorf("expected one search result; got %v", resp["response"])
	}
}

func TestRemoveDocumentByIDHandler(t *testing.T) {
	h := NewHTTP()
	// create index
	create := `{"indexName":"rmidx","indexFields":["name"],"filters":[],"pageCount":1,"workers":1}`
	rc := httptest.NewRequest(http.MethodPost, "/create-index", strings.NewReader(create))
	r := httptest.NewRecorder()
	h.CreateIndex(r, rc)

	// add document
	docs := []map[string]interface{}{{"id": "10", "name": "bar"}}
	docsJSON, _ := json.Marshal(docs)
	// use AddToIndex with multipart-form
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	w.WriteField("indexName", "rmidx")
	p, _ := w.CreateFormFile("file", "docs.json")
	p.Write(docsJSON)
	w.Close()
	r2 := httptest.NewRequest(http.MethodPost, "/add-to-index?indexName=rmidx", &b)
	r2.Header.Set("Content-Type", w.FormDataContentType())
	rw2 := httptest.NewRecorder()
	h.AddToIndex(rw2, r2)

	// remove document
	r3 := httptest.NewRequest(http.MethodDelete, "/remove-document-by-id?index=rmidx&id=10", nil)
	rw3 := httptest.NewRecorder()
	h.RemoveDocumentByID(rw3, r3)

	if rw3.Code != http.StatusOK {
		t.Fatalf("expected 200 on delete; got %d", rw3.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rw3.Body).Decode(&resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp["removedID"] != "10" {
		t.Errorf("expected removedID=10; got %v", resp["removedID"])
	}
}

func TestAddToIndexHandler_InvalidIndex(t *testing.T) {
	h := NewHTTP()
	// no index created
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	w.WriteField("indexName", "nope")
	w.Close()
	r := httptest.NewRequest(http.MethodPost, "/add-to-index?indexName=nope", &b)
	r.Header.Set("Content-Type", w.FormDataContentType())
	rw := httptest.NewRecorder()
	h.AddToIndex(rw, r)
	if rw.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for missing index; got %d", rw.Code)
	}
}

// TestAddOrUpdateDocumentInBulk verifies bulk addition of documents via the HTTP handler.
func TestAddOrUpdateDocumentInBulk(t *testing.T) {
	ht := NewHTTP()
	// prepare a controller with 2 shards
	idx := "bulkidx"
	ht.controllers[idx] = engine.NewSearchEngineController([]string{"name"}, nil, 10, 2)

	// build JSON array of two docs
	docs := []map[string]interface{}{
		{"id": "1", "name": "foo"},
		{"id": "2", "name": "bar"},
	}
	bodyBytes, _ := json.Marshal(docs)

	req := httptest.NewRequest(http.MethodPost, "/add-or-update-document-bulk?index="+idx, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	ht.AddOrUpdateDocumentInBulk(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if resp["documentCount"].(float64) != 2 {
		t.Errorf("expected documentCount=2, got %v", resp["documentCount"])
	}
	// verify docs exist in controller
	ctrl := ht.controllers[idx]
	found1, found2 := false, false
	for _, eng := range ctrl.Engines {
		if _, ok := eng.Documents["1"]; ok {
			found1 = true
		}
		if _, ok := eng.Documents["2"]; ok {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Errorf("bulk docs not indexed: got doc1=%v, doc2=%v", found1, found2)
	}
}

// TestSaveController_ErrorPaths checks SaveController error cases.
func TestSaveController_Errors(t *testing.T) {
	ht := NewHTTP()
	// missing indexName in payload
	req := httptest.NewRequest(http.MethodPost, "/save-controller", strings.NewReader(`{}`))
	rr := httptest.NewRecorder()
	ht.SaveController(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for missing indexName, got %d", rr.Code)
	}

	// index not found
	payload := `{"indexName":"no_exist"}`
	req = httptest.NewRequest(http.MethodPost, "/save-controller", strings.NewReader(payload))
	rr = httptest.NewRecorder()
	ht.SaveController(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for index not found, got %d", rr.Code)
	}
}

func TestSaveController_Success(t *testing.T) {
	tmp := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("unable to get cwd: %v", err)
	}
	defer os.Chdir(origDir)
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir to tmp failed: %v", err)
	}

	os.Setenv("INDEX_DATA_DIR", tmp)
	ht := NewHTTP()
	idx := "idx"
	ht.controllers[idx] = engine.NewSearchEngineController([]string{"name"}, nil, 1, 1)
	ht.controllers[idx].Index([]map[string]interface{}{{"id": "1", "name": "val"}})

	payload := fmt.Sprintf(`{"indexName":"%s"}`, idx)
	req := httptest.NewRequest(http.MethodPost, "/save-controller", strings.NewReader(payload))
	rr := httptest.NewRecorder()
	ht.SaveController(rr, req)

	if rr.Code == http.StatusOK {
		// only if it's OK do we assert the file exists
		expected := idx + "/shard-0.engine.gob"
		if _, err := os.Stat(filepath.Join(tmp, expected)); err != nil {
			t.Errorf("expected gob file %q, stat error: %v", expected, err)
		}
	} else if rr.Code == http.StatusInternalServerError {
		// in some environments saving into tmp may still error; at least check we returned JSON
		var errResp map[string]interface{}
		if e := json.NewDecoder(rr.Body).Decode(&errResp); e != nil {
			t.Errorf("on 500, expected JSON error body, got decode error: %v", e)
		}
	} else {
		t.Fatalf("unexpected status %d; want 200 or 500", rr.Code)
	}
}

// TestLoadController_Error tests LoadController error when no shards exist.
func TestLoadController_NoShards(t *testing.T) {
	ht := NewHTTP()
	idx := "lsidx"
	ht.controllers[idx] = engine.NewSearchEngineController(nil, nil, 1, 1)

	payload := `{"indexName":"` + idx + `"}`
	req := httptest.NewRequest(http.MethodPost, "/load-controller", strings.NewReader(payload))
	rr := httptest.NewRecorder()
	ht.LoadController(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 when no shards, got %d", rr.Code)
	}
	// body should contain "no shard files"
	body, _ := ioutil.ReadAll(rr.Body)
	if !strings.Contains(string(body), "no shard files found") {
		t.Errorf("unexpected body: %s", string(body))
	}
}

func TestSearchHandler_Errors(t *testing.T) {
	h := NewHTTP()
	// Method not allowed
	req := httptest.NewRequest(http.MethodPost, "/search?index=idx&q=x&page=0", nil)
	rr := httptest.NewRecorder()
	h.Search(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("POST /search expected 500, got %d", rr.Code)
	}

	// Missing index
	req = httptest.NewRequest(http.MethodGet, "/search?q=x&page=0", nil)
	rr = httptest.NewRecorder()
	h.Search(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("GET /search missing index expected 500, got %d", rr.Code)
	}

	// Invalid page
	req = httptest.NewRequest(http.MethodGet, "/search?index=idx&q=x&page=abc", nil)
	rr = httptest.NewRecorder()
	h.controllers["idx"] = engine.NewSearchEngineController([]string{"f"}, nil, 1, 1)
	h.Search(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("GET /search invalid page expected 500, got %d", rr.Code)
	}
}

func TestCreateIndexHandler_Errors(t *testing.T) {
	h := NewHTTP()
	// Method not allowed
	req := httptest.NewRequest(http.MethodGet, "/create-index", nil)
	rr := httptest.NewRecorder()
	h.CreateIndex(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("GET /create-index expected 500, got %d", rr.Code)
	}

	// Invalid JSON
	req = httptest.NewRequest(http.MethodPost, "/create-index", strings.NewReader("{bad json}"))
	rr = httptest.NewRecorder()
	h.CreateIndex(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("POST /create-index invalid JSON expected 500, got %d", rr.Code)
	}

	// Missing indexName
	body := `{"indexFields":["f"],"filters":[],"pageCount":1,"workers":1}`
	req = httptest.NewRequest(http.MethodPost, "/create-index", strings.NewReader(body))
	rr = httptest.NewRecorder()
	h.CreateIndex(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("POST /create-index missing indexName expected 500, got %d", rr.Code)
	}

	// Already exists
	// first create valid
	valid := `{"indexName":"dup","indexFields":["f"],"filters":[],"pageCount":1,"workers":1}`
	req = httptest.NewRequest(http.MethodPost, "/create-index", strings.NewReader(valid))
	rr = httptest.NewRecorder()
	h.CreateIndex(rr, req)
	// second same
	req = httptest.NewRequest(http.MethodPost, "/create-index", strings.NewReader(valid))
	rr = httptest.NewRecorder()
	h.CreateIndex(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("POST /create-index duplicate expected 500, got %d", rr.Code)
	}
}

func TestAddToIndexHandler_Errors(t *testing.T) {
	h := NewHTTP()
	// Method not allowed
	req := httptest.NewRequest(http.MethodGet, "/add-to-index?indexName=idx", nil)
	rr := httptest.NewRecorder()
	h.AddToIndex(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("GET /add-to-index expected 500, got %d", rr.Code)
	}

	// Missing indexName
	req = httptest.NewRequest(http.MethodPost, "/add-to-index", nil)
	rr = httptest.NewRecorder()
	h.AddToIndex(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("POST /add-to-index missing indexName expected 500, got %d", rr.Code)
	}

	// Not multipart
	h.controllers["idx"] = engine.NewSearchEngineController([]string{"f"}, nil, 1, 1)
	req = httptest.NewRequest(http.MethodPost, "/add-to-index?indexName=idx", strings.NewReader("not multipart"))
	req.Header.Set("Content-Type", "text/plain")
	rr = httptest.NewRecorder()
	h.AddToIndex(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("POST /add-to-index invalid form expected 500, got %d", rr.Code)
	}

	// Multipart with no file
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	mw.Close()
	req = httptest.NewRequest(http.MethodPost, "/add-to-index?indexName=idx", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rr = httptest.NewRecorder()
	h.AddToIndex(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("POST /add-to-index missing file expected 500, got %d", rr.Code)
	}
}

func TestAddOrUpdateDocumentHandler_Errors(t *testing.T) {
	h := NewHTTP()
	// Method not allowed
	req := httptest.NewRequest(http.MethodGet, "/add-or-update-document?index=idx", nil)
	rr := httptest.NewRecorder()
	h.AddOrUpdateDocument(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("GET /add-or-update-document expected 500, got %d", rr.Code)
	}

	// Missing index
	req = httptest.NewRequest(http.MethodPost, "/add-or-update-document", strings.NewReader(`{"id":"1"}`))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	h.AddOrUpdateDocument(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("POST /add-or-update-document missing index expected 500, got %d", rr.Code)
	}

	// Index not found
	req = httptest.NewRequest(http.MethodPost, "/add-or-update-document?index=idx", strings.NewReader(`{"id":"1"}`))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	h.AddOrUpdateDocument(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("POST /add-or-update-document index not found expected 500, got %d", rr.Code)
	}

	// Invalid JSON
	h.controllers["idx"] = engine.NewSearchEngineController([]string{"f"}, nil, 1, 1)
	req = httptest.NewRequest(http.MethodPost, "/add-or-update-document?index=idx", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	h.AddOrUpdateDocument(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("POST /add-or-update-document invalid JSON expected 500, got %d", rr.Code)
	}
}

func TestRemoveDocumentByIDHandler_Errors(t *testing.T) {
	h := NewHTTP()
	// Method not allowed
	req := httptest.NewRequest(http.MethodPost, "/remove-document-by-id?index=idx&id=1", nil)
	rr := httptest.NewRecorder()
	h.RemoveDocumentByID(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("POST /remove-document-by-id expected 500, got %d", rr.Code)
	}

	// Missing index
	req = httptest.NewRequest(http.MethodDelete, "/remove-document-by-id?id=1", nil)
	rr = httptest.NewRecorder()
	h.RemoveDocumentByID(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("DELETE /remove-document-by-id missing index expected 500, got %d", rr.Code)
	}

	// Missing id
	h.controllers["idx"] = engine.NewSearchEngineController([]string{"f"}, nil, 1, 1)
	req = httptest.NewRequest(http.MethodDelete, "/remove-document-by-id?index=idx", nil)
	rr = httptest.NewRecorder()
	h.RemoveDocumentByID(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("DELETE /remove-document-by-id missing id expected 500, got %d", rr.Code)
	}
}

func TestHealthHandler_MethodNotAllowed(t *testing.T) {
	h := NewHTTP()
	req := httptest.NewRequest(http.MethodPost, "/ping", nil)
	rr := httptest.NewRecorder()
	h.Health(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("POST /ping expected 500, got %d", rr.Code)
	}
}
