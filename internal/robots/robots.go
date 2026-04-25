package robots

import (
	"bufio"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type rules struct{ disallowed []string }

type Checker struct {
	mu     sync.Mutex
	cache  map[string]*rules
	client *http.Client
	agent  string
}

func New(agent string) *Checker {
	return &Checker{
		cache:  make(map[string]*rules),
		client: &http.Client{Timeout: 10 * time.Second},
		agent:  agent,
	}
}

func (c *Checker) Allowed(u string) bool {
	host, path, err := hostAndPath(u)
	if err != nil {
		return true
	}
	r := c.getRules(host)
	if r == nil {
		return true
	}
	for _, prefix := range r.disallowed {
		if strings.HasPrefix(path, prefix) {
			return false
		}
	}
	return true
}

func hostAndPath(u string) (string, string, error) {
	idx := strings.Index(u, "://")
	if idx < 0 {
		return "", "", nil
	}
	rest := u[idx+3:]
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return rest, "/", nil
	}
	return rest[:slash], rest[slash:], nil
}

func (c *Checker) getRules(host string) *rules {
	c.mu.Lock()
	if r, ok := c.cache[host]; ok {
		c.mu.Unlock()
		return r
	}
	c.mu.Unlock()
	r := c.fetchRules(host)
	c.mu.Lock()
	c.cache[host] = r
	c.mu.Unlock()
	return r
}

func (c *Checker) fetchRules(host string) *rules {
	resp, err := c.client.Get("https://" + host + "/robots.txt") //nolint:noctx
	if err != nil {
		resp, err = c.client.Get("http://" + host + "/robots.txt") //nolint:noctx
		if err != nil {
			return nil
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	return parseRobots(resp.Body, c.agent)
}

func parseRobots(r io.Reader, agent string) *rules {
	scanner := bufio.NewScanner(r)
	var applies bool
	var result rules
	agentLower := strings.ToLower(agent)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(parts[0]))
		val := strings.TrimSpace(parts[1])
		switch key {
		case "user-agent":
			v := strings.ToLower(val)
			applies = v == "*" || strings.Contains(agentLower, v)
		case "disallow":
			if applies && val != "" {
				result.disallowed = append(result.disallowed, val)
			}
		}
	}
	return &result
}
