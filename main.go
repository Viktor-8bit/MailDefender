package main

import (
	"context"
	"log"

	"goKFKa/Kafka/consumer"

	"goKFKa/Kafka/schemas"
	"goKFKa/services"
)

func main() {
	log.Println("service start")

	ctx := context.Background()

	var UrlConsumer = consumer.NewConsumer("localhost", "9092", "url", "url-group", schemas.Url)

	urlKafkaService := services.NewUrlKafkaService("<API-token>")

	// это запихать в какой нибудь Kafka handler
	go UrlConsumer.StartConsumer(ctx, urlKafkaService)

	select {}

}
