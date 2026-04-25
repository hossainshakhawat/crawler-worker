package ratelimiter

import (
	"net/url"
	"sync"
	"time"
)

type DomainLimiter struct {
	mu            sync.Mutex
	nextAllowedAt map[string]time.Time
	defaultDelay  time.Duration
}

func New(defaultDelay time.Duration) *DomainLimiter {
	if defaultDelay <= 0 {
		defaultDelay = time.Second
	}
	return &DomainLimiter{
		nextAllowedAt: make(map[string]time.Time),
		defaultDelay:  defaultDelay,
	}
}

func (d *DomainLimiter) Wait(rawURL string) {
	host := hostOf(rawURL)
	if host == "" {
		return
	}
	d.mu.Lock()
	next := d.nextAllowedAt[host]
	now := time.Now()
	var sleep time.Duration
	if now.Before(next) {
		sleep = next.Sub(now)
		d.nextAllowedAt[host] = next.Add(d.defaultDelay)
	} else {
		d.nextAllowedAt[host] = now.Add(d.defaultDelay)
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
