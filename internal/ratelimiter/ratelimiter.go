package ratelimiter

import (
	"net/url"
	"sync"
	"time"
)

type DomainLimiter struct {
	mu       sync.Mutex
	nextAt   map[string]time.Time
	defDelay time.Duration
}

func New(defaultDelay time.Duration) *DomainLimiter {
	if defaultDelay <= 0 {
		defaultDelay = time.Second
	}
	return &DomainLimiter{
		nextAt:   make(map[string]time.Time),
		defDelay: defaultDelay,
	}
}

func (d *DomainLimiter) Wait(rawURL string) {
	host := hostOf(rawURL)
	if host == "" {
		return
	}
	d.mu.Lock()
	next := d.nextAt[host]
	now := time.Now()
	var sleep time.Duration
	if now.Before(next) {
		sleep = next.Sub(now)
		d.nextAt[host] = next.Add(d.defDelay)
	} else {
		d.nextAt[host] = now.Add(d.defDelay)
	}
	d.mu.Unlock()
	if sleep > 0 {
		time.Sleep(sleep)
	}
}

func hostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}
