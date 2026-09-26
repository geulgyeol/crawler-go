package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"crawler/db"
	"crawler/internal/config"
	"crawler/internal/pulsar"
	"crawler/internal/users"

	pulsarClient "github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config file: %v\n", err)
	}

	var userQueries *db.Queries

	pClient, err := pulsar.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create Pulsar client: %v\n", err)
	}
	defer pClient.Close()

	userProdNaver, _ := pClient.CreateProducer("user-naver")
	userProdTistory, _ := pClient.CreateProducer("user-tistory")
	profileProdNaver, _ := pClient.CreateProducer("profile-naver")
	profileProdTistory, _ := pClient.CreateProducer("profile-tistory")
	contentProdNaver, _ := pClient.CreateProducer("content-naver")
	contentProdTistory, _ := pClient.CreateProducer("content-tistory")
	imageProdNaver, _ := pClient.CreateProducer("image-naver")
	imageProdTistory, _ := pClient.CreateProducer("image-tistory")

	defer userProdNaver.Close()
	defer userProdTistory.Close()
	defer profileProdNaver.Close()
	defer profileProdTistory.Close()
	defer contentProdNaver.Close()
	defer contentProdTistory.Close()
	defer imageProdNaver.Close()
	defer imageProdTistory.Close()

	producers := map[string]map[string]pulsarClient.Producer{
		"user":    {"naver": userProdNaver, "tistory": userProdTistory},
		"profile": {"naver": profileProdNaver, "tistory": profileProdTistory},
		"content": {"naver": contentProdNaver, "tistory": contentProdTistory},
		"image":   {"naver": imageProdNaver, "tistory": imageProdTistory},
	}

	fmt.Println("Input Topic and Messages to Publish (-1 to exit)")
	fmt.Println("Topics: user, profile, content, image")
	fmt.Println("Format : [topic] [N or T][Profile Name(/Number)] ...")
	fmt.Println("ex) user Nhello")
	fmt.Println("ex) profile TWorld/1233")

	scanner := bufio.NewScanner(os.Stdin)
	ctx := context.Background()

	for {
		fmt.Print("Input > ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if parts[0] == "-1" {
			break
		}

		topic := parts[0]
		topicProducers, ok := producers[topic]
		if !ok {
			fmt.Println("Topic must be one of user, profile, content, image.")
			continue
		}

		messages := parts[1:]
		if len(messages) == 0 {
			fmt.Println("No Messages")
			continue
		}

		blogTypeOf := func(m string) (string, bool) {
			switch {
			case strings.HasPrefix(m, "N"):
				return "naver", true
			case strings.HasPrefix(m, "T"):
				return "tistory", true
			default:
				return "", false
			}
		}

		if topic == "image" {
			for _, m := range messages {
				blogType, ok := blogTypeOf(m)
				if !ok {
					fmt.Printf("%s: First Character must be N or T\n", m)
					continue
				}

				topicProducers[blogType].SendAsync(ctx, &pulsarClient.ProducerMessage{Payload: []byte(m)}, func(id pulsarClient.MessageID, msg *pulsarClient.ProducerMessage, err error) {})
			}
			fmt.Println("Sent image messages.")
		} else {
			if topic == "user" && userQueries == nil {
				pool, err := users.Open(ctx)
				if err != nil {
					fmt.Printf("Failed to connect to PostgreSQL: %v\n", err)
					continue
				}
				defer pool.Close()
				userQueries = db.New(pool)
			}
			for _, m := range messages {
				blogType, ok := blogTypeOf(m)
				if !ok {
					fmt.Printf("%s: First Character must be N or T\n", m)
					continue
				}

				if topic == "user" {
					inserted, err := users.Register(ctx, userQueries, m)
					if err != nil {
						fmt.Printf("%s: Database error: %v\n", m, err)
						continue
					}
					if !inserted {
						fmt.Printf("%s: User already registered\n", m)
						continue
					}
				}
				topicProducers[blogType].SendAsync(ctx, &pulsarClient.ProducerMessage{Payload: []byte(m)}, func(id pulsarClient.MessageID, msg *pulsarClient.ProducerMessage, err error) {})
				fmt.Printf("Sent: %s\n", m)
			}
		}
	}
}
