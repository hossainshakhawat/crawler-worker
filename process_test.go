package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"
)

func TestGzipCompress_RoundTrip(t *testing.T) {
	input := []byte("hello, world! this is some test data for gzip compression")
	compressed, err := gzipCompress(input)
	if err != nil {
		t.Fatalf("gzipCompress: %v", err)
	}
	r, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, input) {
		t.Errorf("got %q, want %q", got, input)
	}
}

func TestGzipCompress_Empty(t *testing.T) {
	_, err := gzipCompress([]byte{})
	if err != nil {
		t.Fatalf("gzipCompress(empty): %v", err)
	}
}

func TestGzipCompress_Reduces_Size(t *testing.T) {
	// Repeated bytes compress very well
	input := bytes.Repeat([]byte("abcdefgh"), 1000)
	compressed, err := gzipCompress(input)
	if err != nil {
		t.Fatalf("gzipCompress: %v", err)
	}
	if len(compressed) >= len(input) {
		t.Errorf("expected compressed size < input size: %d vs %d", len(compressed), len(input))
	}
}
