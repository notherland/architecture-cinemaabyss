package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type User struct {
	ID        int       `json:"user_id"`
	Username  string    `json:"username"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type Movie struct {
	ID     int    `json:"movie_id"`
	Title  string `json:"title"`
	Action string `json:"action"`
	UserId int    `json:"user_id"`
}

type Payment struct {
	ID         int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType string    `json:"method_type"`
}

var (
	movieProducer   *kafka.Writer
	userProducer    *kafka.Writer
	paymentProducer *kafka.Writer
)

var (
	movieConsumer   *kafka.Reader
	userConsumer    *kafka.Reader
	paymentConsumer *kafka.Reader
)

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka:9092"
	}

	movieProducer = createProducer(brokers, "movie-events")
	userProducer = createProducer(brokers, "user-events")
	paymentProducer = createProducer(brokers, "payment-events")
	defer paymentProducer.Close()
	defer userProducer.Close()
	defer movieProducer.Close()

	movieConsumer = createConsumer(brokers, "movie-events", "movie-events")
	userConsumer = createConsumer(brokers, "user-events", "user-events")
	paymentConsumer = createConsumer(brokers, "payment-events", "payment-events")

	go onEvent(movieConsumer)
	go onEvent(userConsumer)
	go onEvent(paymentConsumer)

	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/api/events/user", handleUserEvents)
	http.HandleFunc("/api/events/movie", handleMovieEvents)
	http.HandleFunc("/api/events/payment", handlePaymentEvents)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	log.Printf("Starting events service on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleMovieEvents(w http.ResponseWriter, r *http.Request) {
	var payload Movie
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Публикуем событие в Kafka
	if err := publishMovieEvent(payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println("Movie events published successfully: ", payload)
}

func handleUserEvents(w http.ResponseWriter, r *http.Request) {
	var payload User
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Публикуем событие в Kafka
	if err := publishUserEvent(payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println("User events published successfully: ", payload)
}

func handlePaymentEvents(w http.ResponseWriter, r *http.Request) {
	var payload Payment
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Публикуем событие в Kafka
	if err := publishPaymentEvent(payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println("Payment events published successfully: ", payload)
}
