package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func createConsumer(brokers, topic string, groupId string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokers},
		Topic:   topic,
		GroupID: groupId,
	})
}

func onEvent(consumer *kafka.Reader) {
	for {
		msg, err := consumer.ReadMessage(context.Background())
		if err != nil {
			log.Println("Ошибка при получении:", err)
		}

		fmt.Println("Получено сообщение в топике: ", string(msg.Value))
	}
}
