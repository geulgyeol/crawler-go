// R2(S3 호환) 버킷에서 특정 프리픽스 아래 객체를 전부 삭제하는 도구.
//
// 사용법:
//
//	go run . -dry-run          # 삭제 대상 개수만 확인 (실제 삭제 안 함)
//	go run . -prefix images/   # S3_PREFIX 대신 직접 지정
//	go run . -y                # 확인 질문 없이 삭제
//
// 환경변수: S3_BUCKET, S3_ENDPOINT, S3_PREFIX, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()

	prefixFlag := flag.String("prefix", "", "삭제할 프리픽스 (비우면 S3_PREFIX 사용)")
	dryRun := flag.Bool("dry-run", false, "삭제하지 않고 대상 개수만 출력")
	yes := flag.Bool("y", false, "확인 질문 생략")
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
	// 프리픽스가 비어 있으면 버킷 전체가 지워지므로 막는다.
	if strings.TrimSpace(prefix) == "" {
		log.Fatal("프리픽스가 비어 있습니다. S3_PREFIX 또는 -prefix로 지정하세요 (버킷 전체 삭제 방지)")
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

	// 1. 대상 개수 집계
	total, err := countObjects(ctx, client, bucket, prefix)
	if err != nil {
		log.Fatalf("목록 조회 실패: %v", err)
	}
	fmt.Printf("대상: s3://%s/%s* (%d개)\n", bucket, prefix, total)

	if total == 0 {
		fmt.Println("삭제할 객체가 없습니다.")
		return
	}
	if *dryRun {
		fmt.Println("dry-run: 실제 삭제는 하지 않았습니다.")
		return
	}

	// 2. 확인
	if !*yes {
		fmt.Printf("정말 삭제할까요? 되돌릴 수 없습니다. 버킷 이름(%s)을 입력하세요: ", bucket)
		line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if strings.TrimSpace(line) != bucket {
			fmt.Println("취소했습니다.")
			return
		}
	}

	// 3. 삭제
	deleted, err := deleteByPrefix(ctx, client, bucket, prefix)
	fmt.Printf("삭제 완료: %d개\n", deleted)
	if err != nil {
		log.Fatalf("삭제 중 오류: %v", err)
	}
}

func countObjects(ctx context.Context, client *s3.Client, bucket, prefix string) (int, error) {
	n := 0
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return n, err
		}
		n += len(page.Contents)
	}
	return n, nil
}

// deleteByPrefix: 프리픽스 아래 객체를 페이지(최대 1000개) 단위로 삭제하고 삭제된 개수를 반환한다.
func deleteByPrefix(ctx context.Context, client *s3.Client, bucket, prefix string) (int, error) {
	deleted := 0
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return deleted, err
		}
		if len(page.Contents) == 0 {
			continue
		}

		ids := make([]types.ObjectIdentifier, 0, len(page.Contents))
		for _, obj := range page.Contents {
			ids = append(ids, types.ObjectIdentifier{Key: obj.Key})
		}

		out, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &bucket,
			Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return deleted, err
		}
		for _, e := range out.Errors {
			log.Printf("삭제 실패: %s (%s)", aws.ToString(e.Key), aws.ToString(e.Message))
		}
		deleted += len(ids) - len(out.Errors)
		fmt.Printf("진행: %d개 삭제됨\n", deleted)
	}
	return deleted, nil
}
