// R2(S3 호환) 버킷에 올라간 이미지를 확인/다운로드하는 도구.
//
// 사용법:
//
//	go run . -list                 # 프리픽스 아래 개수와 앞부분 목록 확인 (다운로드 없음)
//	go run . -n 5                  # 앞에서 5개를 ./downloaded 에 확장자 붙여 저장
//	go run . -url <원본이미지URL>   # 원본 URL로 키를 계산해 해당 이미지 하나 확인/저장
//	go run . -out ./check          # 저장 폴더 지정
//
// 환경변수: S3_BUCKET, S3_ENDPOINT, S3_PREFIX, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
)

func extFromContentType(ct string) string {
	m := map[string]string{
		"image/jpeg":   ".jpg",
		"image/png":    ".png",
		"image/gif":    ".gif",
		"image/webp":   ".webp",
		"image/x-icon": ".ico",
	}
	for mime, ext := range m {
		if strings.Contains(ct, mime) {
			return ext
		}
	}
	return ".bin"
}

// ImageDownloader와 같은 규칙: 키 = prefix + sha256(URL)
func objectKey(prefix, imgURL string) string {
	sum := sha256.Sum256([]byte(imgURL))
	return prefix + hex.EncodeToString(sum[:])
}

func main() {
	_ = godotenv.Load()

	listOnly := flag.Bool("list", false, "개수와 목록만 출력 (다운로드 안 함)")
	n := flag.Int("n", 5, "다운로드할 개수")
	imgURL := flag.String("url", "", "원본 이미지 URL (키를 계산해 하나만 확인)")
	outDir := flag.String("out", "./downloaded", "저장 폴더")
	prefixFlag := flag.String("prefix", "", "프리픽스 (비우면 S3_PREFIX 사용)")
	flag.Parse()

	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		log.Fatal("S3_BUCKET 환경변수가 필요합니다")
	}
	endpoint := os.Getenv("S3_ENDPOINT")
	prefix := *prefixFlag
	if prefix == "" {
		prefix = os.Getenv("S3_PREFIX")
	}

	ctx := context.Background()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("auto"))
	if err != nil {
		log.Fatalf("AWS 설정 로드 실패: %v", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = &endpoint
			o.UsePathStyle = true
		}
	})

	// 원본 URL 하나 확인
	if *imgURL != "" {
		key := objectKey(prefix, *imgURL)
		fmt.Printf("키: %s\n", key)
		if err := checkAndSave(ctx, client, bucket, key, *outDir); err != nil {
			log.Fatalf("실패: %v", err)
		}
		return
	}

	// 목록 조회
	total := 0
	var keys []string
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			log.Fatalf("목록 조회 실패: %v", err)
		}
		for _, obj := range page.Contents {
			total++
			if len(keys) < *n {
				keys = append(keys, aws.ToString(obj.Key))
			}
		}
	}
	fmt.Printf("s3://%s/%s* 에 객체 %d개\n", bucket, prefix, total)
	if total == 0 {
		return
	}

	if *listOnly {
		for _, k := range keys {
			fmt.Println(" -", k)
		}
		return
	}

	ok, bad := 0, 0
	for _, k := range keys {
		if err := checkAndSave(ctx, client, bucket, k, *outDir); err != nil {
			bad++
			log.Printf("  ❌ %s: %v", k, err)
			continue
		}
		ok++
	}
	fmt.Printf("\n확인 완료: 정상 %d개, 문제 %d개 (저장 폴더: %s)\n", ok, bad, *outDir)
}

// checkAndSave: 객체를 받아서 Content-Type / 실제 내용 / 크기를 검증하고 확장자를 붙여 저장한다.
func checkAndSave(ctx context.Context, client *s3.Client, bucket, key, outDir string) error {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return err
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return err
	}

	storedCT := aws.ToString(out.ContentType)
	detectedCT := http.DetectContentType(data) // 파일 내용으로 판별한 실제 타입
	ext := extFromContentType(storedCT)

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	name := filepath.Base(key) + ext
	path := filepath.Join(outDir, name)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}

	status := "✅"
	if !strings.HasPrefix(detectedCT, "image/") || len(data) == 0 {
		status = "⚠️ 이미지가 아닐 수 있음"
	}
	fmt.Printf("%s %s\n    저장된 Content-Type: %s / 실제 판별: %s / %d bytes\n    → %s\n",
		status, key, storedCT, detectedCT, len(data), path)
	return nil
}
