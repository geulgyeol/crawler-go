package pulsar

import (
	"fmt"
	"time"

	"crawler/internal/config"

	"github.com/apache/pulsar-client-go/pulsar"
)

type Client struct {
	cfg          *config.Config
	pulsarClient pulsar.Client
}

func NewClient(cfg *config.Config) (*Client, error) {
	opts := pulsar.ClientOptions{
		URL:                        cfg.PulsarServiceURL,
		TLSAllowInsecureConnection: true,
	}

	pClient, err := pulsar.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("Pulsar 클라이언트 생성 실패: %w", err)
	}

	return &Client{
		cfg:          cfg,
		pulsarClient: pClient,
	}, nil
}

func (c *Client) Close() {
	if c.pulsarClient != nil {
		c.pulsarClient.Close()
	}
}

func (c *Client) CreateProducer(topic string) (pulsar.Producer, error) {
	fullTopic := c.cfg.PulsarNamespace + topic
	producerOptions := pulsar.ProducerOptions{
		Topic:              fullTopic,
		MaxPendingMessages: c.cfg.MaxMessageQueueSize,
	}

	producer, err := c.pulsarClient.CreateProducer(producerOptions)
	if err != nil {
		return nil, fmt.Errorf("[%s] Producer 생성 실패: %w", topic, err)
	}

	return producer, nil
}

func (c *Client) CreateConsumer(topic, subscriptionName string) (pulsar.Consumer, error) {
	fullTopic := c.cfg.PulsarNamespace + topic
	consumerOptions := pulsar.ConsumerOptions{
		Topic:            fullTopic,
		SubscriptionName: subscriptionName,
		Type:             pulsar.Shared,
		DLQ: &pulsar.DLQPolicy{
			MaxDeliveries:   3,
			DeadLetterTopic: fullTopic + "-DLQ",
		},
		NackRedeliveryDelay: 1 * time.Second,
		ReceiverQueueSize:   1000,
	}

	consumer, err := c.pulsarClient.Subscribe(consumerOptions)
	if err != nil {
		return nil, fmt.Errorf("[%s] Consumer 구독 실패: %w", topic, err)
	}

	return consumer, nil
}
