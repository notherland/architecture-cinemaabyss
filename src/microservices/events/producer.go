package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

func createProducer(brokers, topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokers},
		Topic:   topic,
	})
}

func publishUserEvent(user User) error {
	data, err := json.Marshal(user) //прводит к []byte
	if err != nil {
		return err
	}

	err = userProducer.WriteMessages(context.Background(),
		kafka.Message{
			Value: data,
		},
	)
	if err != nil {
		log.Printf("Ошибка отправки события: %v", err)
		return err
	}

	log.Printf("Событие user отправлено: %d", user)
	return nil
}

func publishMovieEvent(movie Movie) error {
	data, err := json.Marshal(movie) //прводит к []byte
	if err != nil {
		return err
	}

	err = movieProducer.WriteMessages(context.Background(),
		kafka.Message{
			Value: data,
		},
	)
	if err != nil {
		log.Printf("Ошибка отправки события: %v", err)
		return err
	}

	log.Printf("Событие movie отправлено: %d", movie)
	return nil
}

func publishPaymentEvent(payment Payment) error {
	data, err := json.Marshal(payment) //прводит к []byte
	if err != nil {
		return err
	}

	err = paymentProducer.WriteMessages(context.Background(),
		kafka.Message{
			Value: data,
		},
	)
	if err != nil {
		log.Printf("Ошибка отправки события: %v", err)
		return err
	}

	log.Printf("Событие movie отправлено: %d", payment)
	return nil
}
