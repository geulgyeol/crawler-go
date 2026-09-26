package main

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"crawler/db"
	"crawler/internal/config"
	"crawler/internal/crawler"
	"crawler/internal/pulsar"
	userreg "crawler/internal/users"

	pulsarClient "github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config file: %v\n", err)
	}

	cClient := crawler.NewClient(cfg)
	pool, err := userreg.Open(context.Background())
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()
	userQueries := db.New(pool)

	pClient, err := pulsar.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create Pulsar client: %v\n", err)
	}
	defer pClient.Close()

	userProducerNaver, err := pClient.CreateProducer("user-naver")
	if err != nil {
		log.Fatalf("Failed to create Naver User Producer: %v\n", err)
	}
	defer userProducerNaver.Close()

	userProducerTistory, err := pClient.CreateProducer("user-tistory")
	if err != nil {
		log.Fatalf("Failed to create Tistory User Producer: %v\n", err)
	}
	defer userProducerTistory.Close()

	consumerNaver, err := pClient.CreateConsumer("profile-naver", cfg.CrawlerName+"_ProfileFinder_N")
	if err != nil {
		log.Fatalf("Failed to subscribe Naver Consumer: %v\n", err)
	}
	defer consumerNaver.Close()

	consumerTistory, err := pClient.CreateConsumer("profile-tistory", cfg.CrawlerName+"_ProfileFinder_T")
	if err != nil {
		log.Fatalf("Failed to subscribe Tistory Consumer: %v\n", err)
	}
	defer consumerTistory.Close()

	fmt.Println("ProfileFinder Started. Waiting for messages...")

	ctx := context.Background()
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		runNaverLoop(ctx, cfg, cClient, userQueries, consumerNaver, userProducerNaver)
	}()
	go func() {
		defer wg.Done()
		runTistoryLoop(ctx, cfg, cClient, userQueries, consumerTistory, userProducerTistory)
	}()

	wg.Wait()
}

func runNaverLoop(ctx context.Context, cfg *config.Config, cClient *crawler.Client, users *db.Queries, consumer pulsarClient.Consumer, userProducer pulsarClient.Producer) {
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			fmt.Printf("[Naver] %s\n", err.Error())
			time.Sleep(100 * time.Millisecond)
			continue
		}

		payload := string(msg.Payload())
		if len(payload) < 2 || !strings.HasPrefix(payload, "N") {
			consumer.Ack(msg)
			continue
		}

		slashIdx := strings.Index(payload, "/")
		if slashIdx == -1 {
			consumer.Ack(msg)
			continue
		}

		profileName := payload[1:slashIdx]
		writingNumber := payload[slashIdx+1:]

		userIDs, processErr := handleNaverSympathy(ctx, cfg, cClient, users, profileName, writingNumber)
		if processErr != nil {
			// Earlier users have already been claimed in PostgreSQL. Publish
			// them before retrying the remaining users on redelivery.
			sendMessages(ctx, userProducer, userIDs)
			fmt.Printf("[Naver] User registration failed: %v\n", processErr)
			consumer.Nack(msg)
			continue
		}

		fmt.Printf("[Naver] %d Profiles found in %s\n", len(userIDs), payload)
		sendMessages(ctx, userProducer, userIDs)

		consumer.Ack(msg)
	}
}

func runTistoryLoop(ctx context.Context, cfg *config.Config, cClient *crawler.Client, users *db.Queries, consumer pulsarClient.Consumer, userProducer pulsarClient.Producer) {
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			fmt.Printf("[Tistory] %s\n", err.Error())
			time.Sleep(100 * time.Millisecond)
			continue
		}

		payload := string(msg.Payload())
		if len(payload) < 2 || !strings.HasPrefix(payload, "T") {
			consumer.Ack(msg)
			continue
		}

		slashIdx := strings.Index(payload, "/")
		if slashIdx == -1 {
			consumer.Ack(msg)
			continue
		}

		profileName := payload[1:slashIdx]
		writingNumber := payload[slashIdx+1:]

		userIDs, processErr := handleTistoryComment(ctx, cfg, cClient, users, profileName, writingNumber)
		if processErr != nil {
			sendMessages(ctx, userProducer, userIDs)
			fmt.Printf("[Tistory] User registration failed: %v\n", processErr)
			consumer.Nack(msg)
			continue
		}

		fmt.Printf("[Tistory] %d Profiles found in %s\n", len(userIDs), payload)
		sendMessages(ctx, userProducer, userIDs)

		consumer.Ack(msg)
	}
}

func sendMessages(ctx context.Context, producer pulsarClient.Producer, messages []string) {
	for _, userID := range messages {
		producer.SendAsync(ctx, &pulsarClient.ProducerMessage{
			Payload: []byte(userID),
		}, func(id pulsarClient.MessageID, msg *pulsarClient.ProducerMessage, err error) {})
	}
}

func handleNaverSympathy(ctx context.Context, cfg *config.Config, client *crawler.Client, users *db.Queries, profileName, writingNumber string) ([]string, error) {
	fmt.Printf("[Naver] Start to process \"N%s/%s\"\n", profileName, writingNumber)

	targetURL := fmt.Sprintf("https://blog.naver.com/api/blogs/%s/posts/%s/sympathy-users?itemCount=100&timeStamp=9999999999999", profileName, writingNumber)
	referer := fmt.Sprintf("https://blog.naver.com/SympathyHistoryList.naver?blogId=%s&logNo=%s", profileName, writingNumber)

	if !client.IsAllowedByRobots(targetURL) {
		return nil, nil
	}

	client.Delay(1000/cfg.CrawlPerSecondMap["ProfileFinder_N"], "ProfileFinder_N")

	headers := map[string]string{"Referer": referer}
	body, statusCode, err := client.DoRequest("GET", targetURL, headers, nil)
	if err != nil || statusCode >= 400 {
		return nil, fmt.Errorf("Request Failed (Status Code: %d): %v", statusCode, err)
	}

	re := regexp.MustCompile(`"domainIdOrBlogId":"([^"]+)"`)
	matches := re.FindAllStringSubmatch(string(body), -1)

	userIDs := make([]string, 0, len(matches))
	for _, match := range matches {
		userID := "N" + match[1]
		inserted, err := userreg.Register(ctx, users, userID)
		if err != nil {
			return userIDs, err
		}
		if inserted {
			userIDs = append(userIDs, userID)
		}
	}

	return userIDs, nil
}

func handleTistoryComment(ctx context.Context, cfg *config.Config, client *crawler.Client, users *db.Queries, profileName, writingNumber string) ([]string, error) {
	fmt.Printf("[Tistory] Start to process \"T%s/%s\"\n", profileName, writingNumber)

	targetURL := fmt.Sprintf("https://%s.tistory.com/m/api/%s/comment", profileName, writingNumber)

	if !client.IsAllowedByRobots(targetURL) {
		return nil, nil
	}

	client.Delay(1000/cfg.CrawlPerSecondMap["ProfileFinder_T"], "ProfileFinder_T")

	body, statusCode, err := client.DoRequest("GET", targetURL, nil, nil)
	if err != nil || statusCode >= 400 {
		return nil, fmt.Errorf("Request Failed (Status Code: %d): %v", statusCode, err)
	}

	re := regexp.MustCompile(`"homepage"\s*:\s*"https://([^"/]+)`)
	matches := re.FindAllStringSubmatch(string(body), -1)

	userIDs := make([]string, 0, len(matches))
	for _, match := range matches {
		fullHost := match[1]
		if !strings.Contains(fullHost, ".tistory.com") {
			continue
		}

		subDomain := strings.Split(fullHost, ".")[0]
		userID := "T" + subDomain
		inserted, err := userreg.Register(ctx, users, userID)
		if err != nil {
			return userIDs, err
		}
		if inserted {
			userIDs = append(userIDs, userID)
		}
	}

	return userIDs, nil
}
