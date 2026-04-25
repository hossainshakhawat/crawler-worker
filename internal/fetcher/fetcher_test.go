package fetcher

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFetch_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("<html>hello</html>"))
	}))
	defer srv.Close()

	f := New(5*time.Second, 1<<20, "test-agent")
	resp := f.Fetch(context.Background(), srv.URL)
	if resp.Err != nil {
		t.Fatalf("unexpected error: %v", resp.Err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
	if string(resp.Body) != "<html>hello</html>" {
		t.Errorf("body: got %q", resp.Body)
	}
}

func TestFetch_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	f := New(5*time.Second, 1<<20, "test-agent")
	resp := f.Fetch(context.Background(), srv.URL)
	if resp.Err != nil {
		t.Fatalf("unexpected error: %v", resp.Err)
	}
	if resp.StatusCode != 404 {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

func TestFetch_SetsUserAgent(t *testing.T) {
	var gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := New(5*time.Second, 1<<20, "my-crawler/1.0")
	f.Fetch(context.Background(), srv.URL)
	if gotUA != "my-crawler/1.0" {
		t.Errorf("User-Agent: got %q, want %q", gotUA, "my-crawler/1.0")
	}
}

func TestFetch_MaxBytes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("0123456789"))
	}))
	defer srv.Close()

	f := New(5*time.Second, 5, "test-agent") // limit to 5 bytes
	resp := f.Fetch(context.Background(), srv.URL)
	if resp.Err != nil {
		t.Fatalf("unexpected error: %v", resp.Err)
	}
	if len(resp.Body) != 5 {
		t.Errorf("body length: got %d, want 5", len(resp.Body))
	}
}

func TestFetch_InvalidURL(t *testing.T) {
	f := New(5*time.Second, 1<<20, "test-agent")
	resp := f.Fetch(context.Background(), "://invalid")
	if resp.Err == nil {
		t.Error("expected error for invalid URL")
	}
}

func TestFetch_CancelledContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	f := New(5*time.Second, 1<<20, "test-agent")
	resp := f.Fetch(ctx, srv.URL)
	if resp.Err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestFetch_FinalURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := New(5*time.Second, 1<<20, "test-agent")
	resp := f.Fetch(context.Background(), srv.URL)
	if resp.FinalURL == "" {
		t.Error("FinalURL should not be empty")
	}
}
