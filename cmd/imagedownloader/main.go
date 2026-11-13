package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/joho/godotenv"

	pulsarClient "github.com/apache/pulsar-client-go/pulsar"

	"crawler/internal/config"
	"crawler/internal/pulsar"
)

const maxImageSize = 20 << 20 // 20MB

func getExtension(contentType string) string {
	mimeMap := map[string]string{
		"image/jpeg":   ".jpg",
		"image/png":    ".png",
		"image/gif":    ".gif",
		"image/webp":   ".webp",
		"image/x-icon": ".ico",
	}

	for mime, ext := range mimeMap {
		if strings.Contains(contentType, mime) {
			return ext
		}
	}
	return ".bin"
}

// normalizeURL: net/url로 파싱해서 쿼리스트링(search param)과 프래그먼트를 모두 제거한다.
// http/https가 아니거나 호스트가 없으면 에러를 반환한다.
func normalizeURL(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("지원하지 않는 scheme: %q", u.Scheme)
	}
	if u.Host == "" {
		return "", errors.New("호스트가 없음")
	}

	u.RawQuery = ""
	u.ForceQuery = false // 끝에 남는 '?' 제거
	u.Fragment = ""
	u.RawFragment = ""
	return u.String(), nil
}

// toASCII: S3 사용자 메타데이터는 HTTP 헤더로 전달되므로 ASCII만 허용된다.
// 비ASCII 바이트(한글 등)는 퍼센트 인코딩한다.
func toASCII(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c > 0x7e {
			fmt.Fprintf(&b, "%%%02X", c)
		} else {
			b.WriteByte(c)
		}
	}
	return b.String()
}

// objectKey: 다운로드 전에 HeadObject를 하려면 Content-Type(확장자)을 모르는 상태에서
// 키가 정해져야 하므로, (쿼리를 제거한) URL의 sha256 해시를 키로 사용한다.
func objectKey(prefix, normalizedURL string) string {
	sum := sha256.Sum256([]byte(normalizedURL))
	return prefix + hex.EncodeToString(sum[:])
}

// objectExists: HeadObject로 존재 여부 확인. 없으면 (false, nil), 그 외 오류는 err 반환.
func objectExists(ctx context.Context, client *s3.Client, bucket, key string) (bool, error) {
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err == nil {
		return true, nil
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "404":
			return false, nil
		}
	}
	return false, err
}

type downloader struct {
	cfg        *config.Config
	s3Client   *s3.Client
	httpClient *http.Client
	bucket     string
	prefix     string
}

func (d *downloader) handle(ctx context.Context, label, rawURL string) bool {
	imgURL, err := normalizeURL(rawURL)
	if err != nil {
		log.Printf("[%s] URL 파싱 실패, 건너뜀 (%s): %v\n", label, rawURL, err)
		return true
	}

	key := objectKey(d.prefix, imgURL)

	exists, err := objectExists(ctx, d.s3Client, d.bucket, key)
	if err != nil {
		log.Printf("[%s] HeadObject 실패 (%s): %v\n", label, key, err)
		return false
	}
	if exists {
		fmt.Printf("[%s] 이미 존재, 건너뜀: %s\n", label, key)
		return true
	}

	resp, err := d.httpClient.Get(imgURL)
	if err != nil {
		return false
	}
	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return false
	}

	ct := resp.Header.Get("Content-Type")
	if getExtension(ct) == ".bin" {
		resp.Body.Close()
		return true
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxImageSize+1))
	resp.Body.Close()
	if err != nil {
		return false
	}
	if len(data) > maxImageSize {
		log.Printf("[%s] 이미지 크기 초과, 건너뜀: %s\n", label, imgURL)
		return true
	}

	_, err = d.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &d.bucket,
		Key:         &key,
		Body:        bytes.NewReader(data),
		ContentType: &ct,
		Metadata: map[string]string{
			"source-url": toASCII(rawURL),
		},
	})
	if err != nil {
		log.Printf("[%s] PutObject 실패 (%s): %v\n", label, key, err)
		return false
	}

	fmt.Printf("[%s] 업로드 성공: s3://%s/%s (%s)\n", label, d.bucket, key, ct)
	return true
}

func runLoop(ctx context.Context, label string, consumer pulsarClient.Consumer, d *downloader) {
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		payload := string(msg.Payload())
		if payload == "" {
			consumer.Ack(msg)
			continue
		}

		if !strings.HasPrefix(payload, "N") && !strings.HasPrefix(payload, "T") {
			consumer.Ack(msg)
			continue
		}
		rawURL := payload[1:]

		if d.handle(ctx, label, rawURL) {
			consumer.Ack(msg)
		} else {
			consumer.Nack(msg)
		}
	}
}

func main() {
	_ = godotenv.Load()

	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config file: %v\n", err)
	}

	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		log.Fatalf("S3_BUCKET 환경변수가 필요합니다\n")
	}
	prefix := os.Getenv("S3_PREFIX")
	endpoint := os.Getenv("S3_ENDPOINT")

	ctx := context.Background()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("auto"),
	)
	if err != nil {
		log.Fatalf("AWS 설정 로드 실패: %v\n", err)
	}
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = &endpoint
			o.UsePathStyle = true
		}
	})

	pClient, err := pulsar.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create Pulsar client: %v\n", err)
	}
	defer pClient.Close()

	consumerNaver, err := pClient.CreateConsumer("image-naver", cfg.CrawlerName+"_ImageDownloader_N")
	if err != nil {
		log.Fatalf("Failed to subscribe Naver Consumer: %v\n", err)
	}
	defer consumerNaver.Close()

	consumerTistory, err := pClient.CreateConsumer("image-tistory", cfg.CrawlerName+"_ImageDownloader_T")
	if err != nil {
		log.Fatalf("Failed to subscribe Tistory Consumer: %v\n", err)
	}
	defer consumerTistory.Close()

	d := &downloader{
		cfg:        cfg,
		s3Client:   s3Client,
		httpClient: &http.Client{Timeout: 15 * time.Second},
		bucket:     bucket,
		prefix:     prefix,
	}

	fmt.Println("ImageDownloader Started. Waiting for messages...")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		runLoop(ctx, "Naver", consumerNaver, d)
	}()
	go func() {
		defer wg.Done()
		runLoop(ctx, "Tistory", consumerTistory, d)
	}()

	wg.Wait()
}
