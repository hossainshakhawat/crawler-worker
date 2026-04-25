package fetcher

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Response struct {
	URL        string
	FinalURL   string
	StatusCode int
	Body       []byte
	Err        error
}

type Fetcher struct {
	client    *http.Client
	userAgent string
	maxBytes  int64
}

func New(timeout time.Duration, maxBytes int64, userAgent string) *Fetcher {
	return &Fetcher{
		client: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return fmt.Errorf("too many redirects")
				}
				return nil
			},
		},
		userAgent: userAgent,
		maxBytes:  maxBytes,
	}
}

func (f *Fetcher) Fetch(ctx context.Context, u string) Response {
	resp := Response{URL: u}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		resp.Err = err
		return resp
	}
	req.Header.Set("User-Agent", f.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

	r, err := f.client.Do(req)
	if err != nil {
		resp.Err = err
		return resp
	}
	defer r.Body.Close()

	resp.FinalURL = r.Request.URL.String()
	resp.StatusCode = r.StatusCode

	reader := io.Reader(r.Body)
	if f.maxBytes > 0 {
		reader = io.LimitReader(r.Body, f.maxBytes)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		resp.Err = err
		return resp
	}
	resp.Body = body
	return resp
}
