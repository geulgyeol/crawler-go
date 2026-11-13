package crawler

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"crawler/internal/config"
)

type RobotsRules struct {
	DisallowPaths []string
}

type RobotsCacheEntry struct {
	Rules       map[string]*RobotsRules
	LastUpdated time.Time
	Exists      bool
	Mutex       sync.Mutex
}

type Client struct {
	cfg            *config.Config
	httpClient     *http.Client
	robotsMap      map[string]*RobotsCacheEntry
	robotsMapMutex sync.Mutex
	lastTimes      map[string]time.Time
	lastTimesMutex sync.Mutex
}

func NewClient(cfg *config.Config) *Client {
	tr := &http.Transport{
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
		DisableKeepAlives: true,
		TLSNextProto:      make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
	}
	httpClient := &http.Client{
		Transport: tr,
		Timeout:   time.Duration(cfg.ResponseTimeoutSeconds) * time.Second,
	}

	return &Client{
		cfg:        cfg,
		httpClient: httpClient,
		robotsMap:  make(map[string]*RobotsCacheEntry),
		lastTimes:  make(map[string]time.Time),
	}
}

func (c *Client) Delay(milliseconds int, threadKey string) {
	c.lastTimesMutex.Lock()
	last, exists := c.lastTimes[threadKey]
	if !exists {
		last = time.Now()
		c.lastTimes[threadKey] = last
	}
	c.lastTimesMutex.Unlock()

	target := last.Add(time.Duration(milliseconds) * time.Millisecond)
	now := time.Now()
	if target.After(now) {
		time.Sleep(target.Sub(now))
	}

	c.lastTimesMutex.Lock()
	c.lastTimes[threadKey] = time.Now()
	c.lastTimesMutex.Unlock()
}

func (c *Client) DoRequest(method, reqURL string, headers map[string]string, body []byte) ([]byte, int, error) {
	req, err := http.NewRequest(method, reqURL, bytes.NewBuffer(body))
	if err != nil {
		return nil, 0, err
	}

	req.Header.Set("User-Agent", c.cfg.UserAgent)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}

	return respBody, resp.StatusCode, nil
}

func (c *Client) IsAllowedByRobots(fullURL string) bool {
	if fullURL == "" {
		return true
	}

	parsed, err := url.Parse(fullURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return true
	}

	domainRoot := fmt.Sprintf("%s://%s", parsed.Scheme, parsed.Host)
	path := parsed.Path
	if path == "" {
		path = "/"
	}

	c.robotsMapMutex.Lock()
	if len(c.robotsMap) >= c.cfg.MaxRobotsCacheSize {
		c.robotsMap = make(map[string]*RobotsCacheEntry)
	}
	entry, exists := c.robotsMap[domainRoot]
	if !exists {
		entry = &RobotsCacheEntry{Rules: make(map[string]*RobotsRules)}
		c.robotsMap[domainRoot] = entry
	}
	c.robotsMapMutex.Unlock()

	c.refreshRobotsCache(domainRoot, entry)

	entry.Mutex.Lock()
	defer entry.Mutex.Unlock()

	if !entry.Exists {
		return true
	}

	pathsToCheck := []string{}
	if r, ok := entry.Rules[c.cfg.CrawlerName]; ok {
		pathsToCheck = r.DisallowPaths
	} else if r, ok := entry.Rules["*"]; ok {
		pathsToCheck = r.DisallowPaths
	}

	for _, disallow := range pathsToCheck {
		if strings.HasPrefix(path, disallow) {
			return false
		}
	}

	return true
}

func (c *Client) refreshRobotsCache(domainRoot string, entry *RobotsCacheEntry) {
	entry.Mutex.Lock()
	if time.Since(entry.LastUpdated).Seconds() < float64(c.cfg.RobotsCacheDurationSeconds) {
		entry.Mutex.Unlock()
		return
	}
	entry.Mutex.Unlock()

	robotsURL := domainRoot + "/robots.txt"
	body, statusCode, err := c.DoRequest("GET", robotsURL, nil, nil)

	entry.Mutex.Lock()
	defer entry.Mutex.Unlock()

	entry.LastUpdated = time.Now()
	entry.Rules = make(map[string]*RobotsRules)

	if err != nil || statusCode >= 400 {
		entry.Exists = false
		return
	}

	entry.Exists = true
	lines := strings.Split(string(body), "\n")
	currentAgent := "*"

	for _, line := range lines {
		if idx := strings.Index(line, "#"); idx != -1 {
			line = line[:idx]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if strings.HasPrefix(strings.ToLower(line), "user-agent:") {
			currentAgent = strings.TrimSpace(line[11:])
			if _, ok := entry.Rules[currentAgent]; !ok {
				entry.Rules[currentAgent] = &RobotsRules{}
			}
		} else if strings.HasPrefix(strings.ToLower(line), "disallow:") {
			disallowedPath := strings.TrimSpace(line[9:])
			if disallowedPath != "" {
				if _, ok := entry.Rules[currentAgent]; !ok {
					entry.Rules[currentAgent] = &RobotsRules{}
				}
				entry.Rules[currentAgent].DisallowPaths = append(entry.Rules[currentAgent].DisallowPaths, disallowedPath)
			}
		}
	}
}

func (c *Client) PostHTMLContent(pageURL, blogType, htmlBody string, timestamp int64) error {
	if htmlBody == "" {
		return nil
	}

	payload := struct {
		URL       string `json:"url"`
		Body      string `json:"body"`
		Blog      string `json:"blog"`
		Timestamp int64  `json:"timestamp"`
	}{
		URL:       pageURL,
		Body:      htmlBody,
		Blog:      blogType,
		Timestamp: timestamp,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	targetURL := strings.TrimRight(c.cfg.HTMLBundlerEndpoint, "/") + "/"
	headers := map[string]string{"Content-Type": "application/json"}

	_, statusCode, err := c.DoRequest("POST", targetURL, headers, payloadBytes)
	if err != nil || statusCode >= 400 {
		return fmt.Errorf("Failed to Post to HTML Bundler (Status Code: %d): %v", statusCode, err)
	}

	return nil
}
