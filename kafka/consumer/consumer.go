package consumer

import (
	"context"
	"errors"
	"goKFKa/services"
	"log"
	"time"

	"github.com/linkedin/goavro"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {

	// входные приколы
	Host       string
	Port       string
	Topic      string
	AvroSchema string
	GroupID    string

	// внутренние приколы
	codec  *goavro.Codec
	Reader *kafka.Reader
}

func NewConsumer(host string, port string, topic string, groupID string, avroSchema string) *Consumer {

	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		log.Fatalf("Failed to create Avro codec: %v", err)
	}

	kafkaHost := host + ":" + port
	// Настройки подключения к Kafka
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaHost},
		Topic:     topic,
		GroupID:   groupID,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
		Partition: 0,
	})

	return &Consumer{Host: host, Port: port, Topic: topic, AvroSchema: avroSchema, GroupID: groupID, codec: codec, Reader: reader}
}

func (c Consumer) StartConsumer(ctx context.Context, KafkaService services.KafkaService) error {

	defer c.Reader.Close()

	for {

		msg, err := c.Reader.ReadMessage(ctx)

		if err != nil {
			return err
		}

		native, _, err := c.codec.NativeFromBinary(msg.Value)

		if err != nil {
			return err
		}
		// Преобразуем в map для удобства работы
		record, ok := native.(map[string]interface{})
		if !ok {
			return errors.New("uncast avro")
		}

		messageCtx, _ := context.WithTimeout(ctx, 5*time.Minute)

		go KafkaService.Processing(messageCtx, record)
	}

}
