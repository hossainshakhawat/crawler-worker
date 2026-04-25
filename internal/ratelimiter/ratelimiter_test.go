package ratelimiter

import (
	"testing"
	"time"
)

func TestNew_DefaultDelay(t *testing.T) {
	rl := New(0) // zero should be replaced by 1s default
	if rl.defaultDelay != time.Second {
		t.Errorf("defaultDelay: got %v, want 1s", rl.defaultDelay)
	}
}

func TestNew_NegativeDelay(t *testing.T) {
	rl := New(-1 * time.Second)
	if rl.defaultDelay != time.Second {
		t.Errorf("defaultDelay: got %v, want 1s for negative input", rl.defaultDelay)
	}
}

func TestNew_CustomDelay(t *testing.T) {
	rl := New(500 * time.Millisecond)
	if rl.defaultDelay != 500*time.Millisecond {
		t.Errorf("defaultDelay: got %v, want 500ms", rl.defaultDelay)
	}
}

func TestWait_FirstCallNoDelay(t *testing.T) {
	rl := New(50 * time.Millisecond)
	start := time.Now()
	rl.Wait("http://example.com/page")
	elapsed := time.Since(start)
	if elapsed > 10*time.Millisecond {
		t.Errorf("first Wait for a new domain took too long: %v (want ~0)", elapsed)
	}
}

func TestWait_SecondCallOnSameDomain(t *testing.T) {
	delay := 50 * time.Millisecond
	rl := New(delay)
	rl.Wait("http://example.com/page1") // first call: no sleep
	start := time.Now()
	rl.Wait("http://example.com/page2") // second call: should sleep ~delay
	elapsed := time.Since(start)
	if elapsed < delay-10*time.Millisecond {
		t.Errorf("second Wait on same domain too fast: %v (want >= %v)", elapsed, delay)
	}
}

func TestWait_DifferentDomainsNoDelay(t *testing.T) {
	rl := New(50 * time.Millisecond)
	rl.Wait("http://a.example.com/page")
	start := time.Now()
	rl.Wait("http://b.example.com/page") // different domain: no delay
	elapsed := time.Since(start)
	if elapsed > 10*time.Millisecond {
		t.Errorf("Wait on different domain took too long: %v (want ~0)", elapsed)
	}
}

func TestWait_EmptyURL(t *testing.T) {
	rl := New(50 * time.Millisecond)
	// Should not panic or block
	start := time.Now()
	rl.Wait("")
	elapsed := time.Since(start)
	if elapsed > 5*time.Millisecond {
		t.Errorf("Wait on empty URL took too long: %v", elapsed)
	}
}

func TestHostOf_Valid(t *testing.T) {
	got := hostOf("http://www.example.com/path")
	if got != "www.example.com" {
		t.Errorf("hostOf: got %q, want %q", got, "www.example.com")
	}
}

func TestHostOf_WithPort(t *testing.T) {
	got := hostOf("http://example.com:8080/path")
	if got != "example.com" {
		t.Errorf("hostOf: got %q, want %q", got, "example.com")
	}
}

func TestHostOf_Invalid(t *testing.T) {
	got := hostOf("://bad-url")
	if got != "" {
		t.Errorf("hostOf(invalid): got %q, want empty string", got)
	}
}
