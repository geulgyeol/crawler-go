package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type Config struct {
	CrawlerName                string         `yaml:"crawler_name"`
	UserAgentPrefix            string         `yaml:"user_agent.prefix"`
	UserAgentSuffix            string         `yaml:"user_agent.suffix"`
	UserAgent                  string         `yaml:"-"`
	HTMLBundlerEndpoint        string         `yaml:"html_bundler_endpoint"`
	PulsarServiceURL           string         `yaml:"pulsar_service_url"`
	PulsarNamespace            string         `yaml:"pulsar_namespace"`
	MaxConcurrentRequests      int            `yaml:"max_concurrent_requests"`
	NaverTimeoutWaitingTime    int            `yaml:"naver_timeout_waiting_time"`
	ConnectingTimeoutSeconds   int            `yaml:"connecting_timeout_seconds"`
	ResponseTimeoutSeconds     int            `yaml:"response_timeout_seconds"`
	MaxMessageQueueSize        int            `yaml:"max_message_queue_size"`
	MaxBatchingMessageCount    int            `yaml:"max_batching_message_count"`
	MaxBatchingDelay           int            `yaml:"max_batching_delay"`
	RobotsCacheDurationSeconds int64          `yaml:"robots_cache_duration_seconds"`
	MaxRobotsCacheSize         int            `yaml:"max_robots_cache_size"`
	Verbose                    bool           `yaml:"verbose"`
	LogLevel                   string         `yaml:"log_level"`
	CrawlPerSecondMap          map[string]int `yaml:"crawl_per_second_map"`
}

type yamlRawConfig struct {
	CrawlerName string `yaml:"crawler_name"`
	UserAgent   struct {
		Prefix string `yaml:"prefix"`
		Suffix string `yaml:"suffix"`
	} `yaml:"user_agent"`
	HTMLBundlerEndpoint        string         `yaml:"html_bundler_endpoint"`
	PulsarServiceURL           string         `yaml:"pulsar_service_url"`
	PulsarNamespace            string         `yaml:"pulsar_namespace"`
	MaxConcurrentRequests      int            `yaml:"max_concurrent_requests"`
	NaverTimeoutWaitingTime    int            `yaml:"naver_timeout_waiting_time"`
	ConnectingTimeoutSeconds   int            `yaml:"connecting_timeout_seconds"`
	ResponseTimeoutSeconds     int            `yaml:"response_timeout_seconds"`
	MaxMessageQueueSize        int            `yaml:"max_message_queue_size"`
	MaxBatchingMessageCount    int            `yaml:"max_batching_message_count"`
	MaxBatchingDelay           int            `yaml:"max_batching_delay"`
	RobotsCacheDurationSeconds int64          `yaml:"robots_cache_duration_seconds"`
	MaxRobotsCacheSize         int            `yaml:"max_robots_cache_size"`
	Verbose                    bool           `yaml:"verbose"`
	LogLevel                   string         `yaml:"log_level"`
	CrawlPerSecondMap          map[string]int `yaml:"crawl_per_second_map"`
}

func findConfigFile(filename string) (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		candidate := filepath.Join(dir, filename)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", fmt.Errorf("%s 파일을 찾을 수 없습니다", filename)
}

func LoadConfig() (*Config, error) {
	configPath, err := findConfigFile("config.yaml")
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("config 파일 읽기 실패 (%s): %w", configPath, err)
	}

	var raw yamlRawConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("yaml 파싱 실패: %w", err)
	}

	cfg := &Config{
		CrawlerName:                raw.CrawlerName,
		UserAgent:                  raw.UserAgent.Prefix + raw.CrawlerName + raw.UserAgent.Suffix,
		HTMLBundlerEndpoint:        raw.HTMLBundlerEndpoint,
		PulsarServiceURL:           raw.PulsarServiceURL,
		PulsarNamespace:            raw.PulsarNamespace,
		MaxConcurrentRequests:      raw.MaxConcurrentRequests,
		NaverTimeoutWaitingTime:    raw.NaverTimeoutWaitingTime,
		ConnectingTimeoutSeconds:   raw.ConnectingTimeoutSeconds,
		ResponseTimeoutSeconds:     raw.ResponseTimeoutSeconds,
		MaxMessageQueueSize:        raw.MaxMessageQueueSize,
		MaxBatchingMessageCount:    raw.MaxBatchingMessageCount,
		MaxBatchingDelay:           raw.MaxBatchingDelay,
		RobotsCacheDurationSeconds: raw.RobotsCacheDurationSeconds,
		MaxRobotsCacheSize:         raw.MaxRobotsCacheSize,
		Verbose:                    raw.Verbose,
		LogLevel:                   raw.LogLevel,
		CrawlPerSecondMap:          raw.CrawlPerSecondMap,
	}

	return cfg, nil
}
