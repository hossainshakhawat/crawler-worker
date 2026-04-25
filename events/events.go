package events

import "time"

type DiscoveredURL struct {
	URL        string    `json:"url"`
	Depth      int       `json:"depth"`
	SourceURL  string    `json:"source_url"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

type CrawledPage struct {
	URL        string    `json:"url"`
	FinalURL   string    `json:"final_url"`
	Depth      int       `json:"depth"`
	HTTPStatus int       `json:"http_status"`
	Body       []byte    `json:"body"` // gzip-compressed HTML
	CrawledAt  time.Time `json:"crawled_at"`
}
