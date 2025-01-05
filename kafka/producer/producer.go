package producer

import (
	"context"
	"fmt"
	"log"

	"github.com/linkedin/goavro"
	"github.com/segmentio/kafka-go"
)

type Producer struct {

	// входные приколы
	Host       string
	Port       string
	Topic      string
	AvroSchema string

	// внутренние приколы
	codec  *goavro.Codec
	writer *kafka.Writer
}

func NewProducer(host string, port string, topic string, avroSchema string) *Producer {

	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		log.Fatalf("Failed to create Avro codec: %v", err)
	}

	writer := &kafka.Writer{
		Addr:  kafka.TCP(fmt.Sprintf("%s:%s", host, port)),
		Topic: topic,
		// Balancer: &kafka.LeastBytes{},
	}

	return &Producer{Host: host, Port: port, Topic: topic, AvroSchema: avroSchema, codec: codec, writer: writer}
}

func (p Producer) Produce(ctx context.Context, message map[string]interface{}) error {

	defer p.writer.Close()

	avroData, err := p.codec.BinaryFromNative(nil, message)
	if err != nil {
		return err
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Value: avroData,
	})
	if err != nil {
		return err
	}

	return nil
}
