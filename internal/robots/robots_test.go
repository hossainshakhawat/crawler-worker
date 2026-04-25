package robots

import (
	"strings"
	"testing"
)

func TestParseRobots_WildcardDisallow(t *testing.T) {
	robotsTxt := "User-agent: *\nDisallow: /private/\nDisallow: /admin/\n"
	r := parseRobots(strings.NewReader(robotsTxt), "test-bot")
	if len(r.disallowed) != 2 {
		t.Fatalf("expected 2 disallowed rules, got %d", len(r.disallowed))
	}
}

func TestParseRobots_AgentSpecific(t *testing.T) {
	robotsTxt := "User-agent: test-bot\nDisallow: /secret/\n\nUser-agent: *\nDisallow: /public-forbidden/\n"
	r := parseRobots(strings.NewReader(robotsTxt), "test-bot")
	found := false
	for _, d := range r.disallowed {
		if d == "/secret/" {
			found = true
		}
	}
	if !found {
		t.Error("expected /secret/ to be disallowed for test-bot")
	}
}

func TestParseRobots_EmptyDisallow(t *testing.T) {
	robotsTxt := "User-agent: *\nDisallow:\n"
	r := parseRobots(strings.NewReader(robotsTxt), "test-bot")
	if len(r.disallowed) != 0 {
		t.Errorf("expected 0 disallowed (empty Disallow line), got %d", len(r.disallowed))
	}
}

func TestParseRobots_Comments(t *testing.T) {
	robotsTxt := "# This is a comment\nUser-agent: *\n# another comment\nDisallow: /hidden/\n"
	r := parseRobots(strings.NewReader(robotsTxt), "test-bot")
	if len(r.disallowed) != 1 {
		t.Fatalf("expected 1 disallowed, got %d", len(r.disallowed))
	}
	if r.disallowed[0] != "/hidden/" {
		t.Errorf("expected /hidden/, got %q", r.disallowed[0])
	}
}

func TestAllowed_Disallowed(t *testing.T) {
	c := New("test-bot")
	c.mu.Lock()
	c.cache["example.com"] = &rules{disallowed: []string{"/private/", "/admin/"}}
	c.mu.Unlock()

	if c.Allowed("http://example.com/private/page") {
		t.Error("expected /private/page to be disallowed")
	}
	if c.Allowed("http://example.com/admin/dashboard") {
		t.Error("expected /admin/dashboard to be disallowed")
	}
}

func TestAllowed_Allowed(t *testing.T) {
	c := New("test-bot")
	c.mu.Lock()
	c.cache["example.com"] = &rules{disallowed: []string{"/private/"}}
	c.mu.Unlock()

	if !c.Allowed("http://example.com/public/page") {
		t.Error("expected /public/page to be allowed")
	}
}

func TestAllowed_NoCacheEntry(t *testing.T) {
	// When getRules returns nil (no robots.txt), everything is allowed
	c := New("test-bot")
	c.mu.Lock()
	c.cache["nocache.example.com"] = nil
	c.mu.Unlock()

	if !c.Allowed("http://nocache.example.com/any/path") {
		t.Error("expected everything to be allowed when rules are nil")
	}
}

func TestAllowed_InvalidURL(t *testing.T) {
	c := New("test-bot")
	// URLs without a scheme - hostAndPath returns empty, Allowed returns true
	if !c.Allowed("not-a-url") {
		t.Error("expected invalid URL to be allowed (fail open)")
	}
}

func TestHostAndPath(t *testing.T) {
	tests := []struct {
		url      string
		wantHost string
		wantPath string
	}{
		{"http://example.com/path/to/page", "example.com", "/path/to/page"},
		{"https://example.com/", "example.com", "/"},
		{"https://example.com", "example.com", "/"},
		{"no-scheme", "", ""},
	}
	for _, tt := range tests {
		host, path, err := hostAndPath(tt.url)
		if err != nil {
			t.Errorf("hostAndPath(%q) returned error: %v", tt.url, err)
			continue
		}
		if host != tt.wantHost {
			t.Errorf("hostAndPath(%q) host = %q, want %q", tt.url, host, tt.wantHost)
		}
		if path != tt.wantPath {
			t.Errorf("hostAndPath(%q) path = %q, want %q", tt.url, path, tt.wantPath)
		}
	}
}
