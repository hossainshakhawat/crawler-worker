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

func (c *Checker) Allowed(rawURL string) bool {
	host, path, err := hostAndPath(rawURL)
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

func hostAndPath(rawURL string) (string, string, error) {
	schemeIndex := strings.Index(rawURL, "://")
	if schemeIndex < 0 {
		return "", "", nil
	}
	withoutScheme := rawURL[schemeIndex+3:]
	slashIndex := strings.IndexByte(withoutScheme, '/')
	if slashIndex < 0 {
		return withoutScheme, "/", nil
	}
	return withoutScheme[:slashIndex], withoutScheme[slashIndex:], nil
}

func (c *Checker) getRules(host string) *rules {
	c.mu.Lock()
	if cachedRules, ok := c.cache[host]; ok {
		c.mu.Unlock()
		return cachedRules
	}
	c.mu.Unlock()
	fetchedRules := c.fetchRules(host)
	c.mu.Lock()
	c.cache[host] = fetchedRules
	c.mu.Unlock()
	return fetchedRules
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

func parseRobots(reader io.Reader, agent string) *rules {
	scanner := bufio.NewScanner(reader)
	var agentApplies bool
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
		fieldKey := strings.ToLower(strings.TrimSpace(parts[0]))
		fieldValue := strings.TrimSpace(parts[1])
		switch fieldKey {
		case "user-agent":
			agentName := strings.ToLower(fieldValue)
			agentApplies = agentName == "*" || strings.Contains(agentLower, agentName)
		case "disallow":
			if agentApplies && fieldValue != "" {
				result.disallowed = append(result.disallowed, fieldValue)
			}
		}
	}
	return &result
}
