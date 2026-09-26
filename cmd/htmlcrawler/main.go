package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"crawler/internal/config"
	"crawler/internal/crawler"
	"crawler/internal/pulsar"

	pulsarClient "github.com/apache/pulsar-client-go/pulsar"
)

type job struct {
	msg  pulsarClient.Message
	link string
}

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config file: %v\n", err)
	}

	cClient := crawler.NewClient(cfg)

	pClient, err := pulsar.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create Pulsar client: %v\n", err)
	}
	defer pClient.Close()

	consumerNaver, err := pClient.CreateConsumer("content-naver", cfg.CrawlerName+"_HTMLCrawler_N")
	if err != nil {
		log.Fatalf("Failed to subscribe Naver Consumer: %v\n", err)
	}
	defer consumerNaver.Close()

	consumerTistory, err := pClient.CreateConsumer("content-tistory", cfg.CrawlerName+"_HTMLCrawler_T")
	if err != nil {
		log.Fatalf("Failed to subscribe Tistory Consumer: %v\n", err)
	}
	defer consumerTistory.Close()

	fmt.Println("HTMLCrawler Started. Waiting for messages...")

	ctx := context.Background()

	naverWorkers := cfg.CrawlPerSecondMap["HTMLCrawler_N"]
	tistoryWorkers := cfg.CrawlPerSecondMap["HTMLCrawler_T"]

	naverQueue := make(chan job, naverWorkers*2)
	tistoryQueue := make(chan job, tistoryWorkers*2)

	processLink := func(consumer pulsarClient.Consumer, msg pulsarClient.Message, targetLink string) {
		slashIdx := strings.Index(targetLink, "/")
		if slashIdx == -1 {
			consumer.Ack(msg)
			return
		}

		profileName := targetLink[1:slashIdx]
		writingNumber := targetLink[slashIdx+1:]

		var reqURL string
		var blogType string
		switch targetLink[0] {
		case 'N':
			reqURL = fmt.Sprintf("https://blog.naver.com/PostView.nhn?blogId=%s&logNo=%s", profileName, writingNumber)
			blogType = "naver"
		case 'T':
			reqURL = fmt.Sprintf("https://%s.tistory.com/m/%s", profileName, writingNumber)
			blogType = "tistory"
		default:
			consumer.Ack(msg)
			return
		}

		if !cClient.IsAllowedByRobots(reqURL) {
			consumer.Ack(msg)
			return
		}

		bodyBytes, statusCode, err := cClient.DoRequest("GET", reqURL, nil, nil)
		if err != nil || statusCode != 200 {
			fmt.Printf("error: %v, code: %d, link: %s\n", err, statusCode, targetLink)
			consumer.Nack(msg)
			return
		}

		dbCheckLink := strings.Replace(targetLink, "/", "%20", 1)

		logPrefix := "[Naver]"
		if blogType == "tistory" {
			logPrefix = "[Tistory]"
		}

		if err := cClient.PostHTMLContent(dbCheckLink[1:], blogType, string(bodyBytes), time.Now().Unix()); err != nil {
			fmt.Printf("%s error: %v, link: %s\n", logPrefix, err, targetLink)
			consumer.Nack(msg)
			return
		}

		fmt.Printf("%s %s Crawled\n", logPrefix, targetLink)
		consumer.Ack(msg)
	}

	startWorkers := func(consumer pulsarClient.Consumer, queue chan job, count int) {
		for i := 0; i < count; i++ {
			go func() {
				for j := range queue {
					processLink(consumer, j.msg, j.link)
					time.Sleep(1 * time.Second)
				}
			}()
		}
	}
	startWorkers(consumerNaver, naverQueue, naverWorkers)
	startWorkers(consumerTistory, tistoryQueue, tistoryWorkers)

	receiveLoop := func(consumer pulsarClient.Consumer, queue chan job) {
		for {
			cm, err := consumer.Receive(ctx)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			link := string(cm.Payload())
			if link == "" {
				consumer.Ack(cm)
				continue
			}

			queue <- job{msg: cm, link: link}
		}
	}
	go receiveLoop(consumerNaver, naverQueue)
	receiveLoop(consumerTistory, tistoryQueue)
}
