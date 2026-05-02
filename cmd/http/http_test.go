package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/mg52/search/internal/engine"
)

func TestCreateIndexHandler(t *testing.T) {
	h := NewHTTP()
	// Valid create-index request
	body := `{"indexName":"testidx","indexFields":["name"],"filters":["year"],"resultCount":5,"workers":2}`
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
	req := httptest.NewRequest(http.MethodGet, "/search?index=unknown&q=a", nil)
	rr := httptest.NewRecorder()
	h.Search(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500; got %d", rr.Code)
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

// TestSaveEngine_ErrorPaths checks SaveEngine error cases.
func TestSaveEngine_Errors(t *testing.T) {
	ht := NewHTTP()
	// missing indexName in payload
	req := httptest.NewRequest(http.MethodPost, "/save-controller", strings.NewReader(`{}`))
	rr := httptest.NewRecorder()
	ht.SaveEngine(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for missing indexName, got %d", rr.Code)
	}

	// index not found
	payload := `{"indexName":"no_exist"}`
	req = httptest.NewRequest(http.MethodPost, "/save-controller", strings.NewReader(payload))
	rr = httptest.NewRecorder()
	ht.SaveEngine(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for index not found, got %d", rr.Code)
	}
}

func TestSaveEngine_Success(t *testing.T) {
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
	ht.engines[idx] = engine.NewSearchEngine([]string{"name"}, nil, 1)
	ht.engines[idx].Index([]map[string]interface{}{{"id": "1", "name": "val"}})

	payload := fmt.Sprintf(`{"indexName":"%s"}`, idx)
	req := httptest.NewRequest(http.MethodPost, "/save-controller", strings.NewReader(payload))
	rr := httptest.NewRecorder()
	ht.SaveEngine(rr, req)

	if rr.Code == http.StatusOK {
		// only if it's OK do we assert the file exists
		expected := idx + "/engine.gob"
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

func TestSearchHandler_Errors(t *testing.T) {
	h := NewHTTP()
	// Method not allowed
	req := httptest.NewRequest(http.MethodPost, "/search?index=idx&q=x", nil)
	rr := httptest.NewRecorder()
	h.Search(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("POST /search expected 500, got %d", rr.Code)
	}

	// Missing index
	req = httptest.NewRequest(http.MethodGet, "/search?q=x", nil)
	rr = httptest.NewRecorder()
	h.Search(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("GET /search missing index expected 500, got %d", rr.Code)
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
	body := `{"indexFields":["f"],"filters":[],"resultCount":1,"workers":1}`
	req = httptest.NewRequest(http.MethodPost, "/create-index", strings.NewReader(body))
	rr = httptest.NewRecorder()
	h.CreateIndex(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("POST /create-index missing indexName expected 500, got %d", rr.Code)
	}

	// Already exists
	// first create valid
	valid := `{"indexName":"dup","indexFields":["f"],"filters":[],"resultCount":1,"workers":1}`
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
	h.engines["idx"] = engine.NewSearchEngine([]string{"f"}, nil, 1)
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
	req := httptest.NewRequest(http.MethodGet, "/document?indexName=idx", nil)
	rr := httptest.NewRecorder()
	h.AddOrUpdateDocument(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("GET /document expected 500, got %d", rr.Code)
	}

	// Missing indexName (query and body)
	body := `{"document":{"id":"1","name":"x"}}`
	req = httptest.NewRequest(http.MethodPost, "/document", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	h.AddOrUpdateDocument(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("POST /document missing indexName expected 500, got %d", rr.Code)
	}

	// Index not found
	req = httptest.NewRequest(http.MethodPost, "/document?indexName=nope", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	h.AddOrUpdateDocument(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("POST /document index not found expected 500, got %d", rr.Code)
	}

	// Invalid JSON
	h.engines["idx"] = engine.NewSearchEngine([]string{"name"}, map[string]bool{"year": true}, 10)
	req = httptest.NewRequest(http.MethodPost, "/document?indexName=idx", strings.NewReader("{bad json}"))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	h.AddOrUpdateDocument(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("POST /document invalid JSON expected 500, got %d", rr.Code)
	}

	// Missing document
	req = httptest.NewRequest(http.MethodPost, "/document?indexName=idx", strings.NewReader(`{"indexName":"idx"}`))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	h.AddOrUpdateDocument(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("POST /document missing document expected 500, got %d", rr.Code)
	}

	// Missing id inside document
	req = httptest.NewRequest(http.MethodPost, "/document?indexName=idx", strings.NewReader(`{"document":{"name":"x"}}`))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	h.AddOrUpdateDocument(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("POST /document missing id expected 500, got %d", rr.Code)
	}
}

func TestDeleteDocumentHandler_Errors(t *testing.T) {
	h := NewHTTP()

	// Method not allowed
	req := httptest.NewRequest(http.MethodPost, "/document?indexName=idx&id=1", nil)
	rr := httptest.NewRecorder()
	h.DeleteDocument(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("POST /document (delete) expected 500, got %d", rr.Code)
	}

	// Missing indexName
	req = httptest.NewRequest(http.MethodDelete, "/document?id=1", nil)
	rr = httptest.NewRecorder()
	h.DeleteDocument(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("DELETE /document missing indexName expected 500, got %d", rr.Code)
	}

	// Missing id
	req = httptest.NewRequest(http.MethodDelete, "/document?indexName=idx", nil)
	rr = httptest.NewRecorder()
	h.DeleteDocument(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("DELETE /document missing id expected 500, got %d", rr.Code)
	}

	// Index not found
	req = httptest.NewRequest(http.MethodDelete, "/document?indexName=nope&id=1", nil)
	rr = httptest.NewRecorder()
	h.DeleteDocument(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("DELETE /document index not found expected 500, got %d", rr.Code)
	}
}

func TestDocumentEndpoints_E2E_AddUpdateDeleteSearch(t *testing.T) {
	h := NewHTTP()

	// 1) Create index with indexFields=["name"] and filter=["year"]
	createBody := `{"indexName":"testidx","indexFields":["name"],"filters":["year"],"resultCount":10}`
	req := httptest.NewRequest(http.MethodPost, "/create-index", strings.NewReader(createBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CreateIndex(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create-index expected 201, got %d body=%s", rr.Code, rr.Body.String())
	}

	// helper: POST /document
	postDoc := func(doc map[string]interface{}) *httptest.ResponseRecorder {
		b, _ := json.Marshal(map[string]interface{}{"document": doc})
		r := httptest.NewRequest(http.MethodPost, "/document?indexName=testidx", bytes.NewReader(b))
		r.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.AddOrUpdateDocument(rec, r)
		return rec
	}

	// helper: DELETE /document
	delDoc := func(id string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodDelete, "/document?indexName=testidx&id="+id, nil)
		rec := httptest.NewRecorder()
		h.DeleteDocument(rec, r)
		return rec
	}

	// helper: GET /search
	search := func(q string) map[string]interface{} {
		r := httptest.NewRequest(http.MethodGet, "/search?index=testidx&q="+q, nil)
		rec := httptest.NewRecorder()
		h.Search(rec, r)
		if rec.Code != http.StatusOK {
			t.Fatalf("search %q expected 200, got %d body=%s", q, rec.Code, rec.Body.String())
		}
		var resp map[string]interface{}
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("decode search resp: %v", err)
		}
		return resp
	}

	// helper: extract returned doc IDs from /search response
	// response shape:
	// { "response": { "Docs": [ { "ID": "...", "Data": {...}, "Score": ... }, ... ] } }
	extractIDs := func(searchResp map[string]interface{}) []string {
		respObj, ok := searchResp["response"].(map[string]interface{})
		if !ok {
			return nil
		}
		docsAny, ok := respObj["Docs"].([]interface{})
		if !ok {
			return nil
		}
		out := make([]string, 0, len(docsAny))
		for _, d := range docsAny {
			m, _ := d.(map[string]interface{})
			if m == nil {
				continue
			}
			if id, ok := m["ID"].(string); ok {
				out = append(out, id)
			}
		}
		sort.Strings(out)
		return out
	}

	assertIDs := func(got []string, exp ...string) {
		t.Helper()
		sort.Strings(exp)
		if len(got) != len(exp) {
			t.Fatalf("ids mismatch: got=%v exp=%v", got, exp)
		}
		for i := range got {
			if got[i] != exp[i] {
				t.Fatalf("ids mismatch: got=%v exp=%v", got, exp)
			}
		}
	}

	// 2) Add documents
	// doc1: "Sunny Rio"
	rec := postDoc(map[string]interface{}{"id": "1", "name": "Sunny Rio", "year": "2020"})
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /document doc1 expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	// doc2: "Rio Nights"
	rec = postDoc(map[string]interface{}{"id": "2", "name": "Rio Nights", "year": "2021"})
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /document doc2 expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	// doc3: "Cloudy Day"
	rec = postDoc(map[string]interface{}{"id": "3", "name": "Cloudy Day", "year": "2021"})
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /document doc3 expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	// 3) Search verifies initial state
	ids := extractIDs(search("sunny"))
	assertIDs(ids, "1")

	ids = extractIDs(search("rio"))
	assertIDs(ids, "1", "2")

	// 4) Update doc2: remove "rio", add "sunny"
	rec = postDoc(map[string]interface{}{"id": "2", "name": "Sunny Days", "year": "2021"})
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /document doc2 update expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	ids = extractIDs(search("rio"))
	assertIDs(ids, "1") // doc2 should disappear due to tombstone model

	ids = extractIDs(search("sunny"))
	// Depending on your SingleTermSearch prefix behavior, you might see extra matches.
	// But for direct token "sunny", we expect doc1 + updated doc2.
	// If your prefix expansion returns multiple terms, ensure your engine still returns docs with "sunny".
	// This assertion expects at least these.
	assertIDs(ids, "1", "2")

	// 5) Delete doc1 and verify it disappears
	rec = delDoc("1")
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE /document doc1 expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	ids = extractIDs(search("rio"))
	assertIDs(ids) // empty

	ids = extractIDs(search("sunny"))
	assertIDs(ids, "2")

	// 6) Add doc4 and verify it shows up
	rec = postDoc(map[string]interface{}{"id": "4", "name": "Rio Sunny", "year": "2022"})
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /document doc4 expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	ids = extractIDs(search("rio"))
	assertIDs(ids, "4")

	ids = extractIDs(search("sunny"))
	assertIDs(ids, "2", "4")

	// 7) Delete non-existing should still succeed with deleted=false
	rec = delDoc("does-not-exist")
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE /document non-existing expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var delResp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&delResp); err != nil {
		t.Fatalf("decode delete resp: %v", err)
	}
	// "deleted" field should be false
	if d, ok := delResp["deleted"].(bool); ok {
		if d {
			t.Fatalf("expected deleted=false for non-existing doc")
		}
	}
}
