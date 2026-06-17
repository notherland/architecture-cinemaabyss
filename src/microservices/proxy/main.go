package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	monolithURL := os.Getenv("MONOLITH_URL")
	if monolithURL == "" {
		monolithURL = "http://monolith:8080"
	}

	moviesURL := os.Getenv("MOVIES_SERVICE_URL")
	if moviesURL == "" {
		moviesURL = "http://movies-service:8081"
	}

	eventsURL := os.Getenv("EVENTS_SERVICE_URL")
	if eventsURL == "" {
		eventsURL = "http://events-service:8082"
	}

	var percent int64 = 0
	if gradual := os.Getenv("GRADUAL_MIGRATION"); gradual == "true" {
		if p, err := strconv.ParseInt(os.Getenv("MOVIES_MIGRATION_PERCENT"), 10, 0); err == nil {
			percent = p
		}
	}
	log.Println("MOVIES_MIGRATION_PERCENT = ", percent)

	monolithProxy := newProxy(monolithURL)
	moviesProxy := newProxy(moviesURL)
	eventsProxy := newProxy(eventsURL)

	http.HandleFunc("/health", handleHealth)

	//Распределяем между монолитом и новым микросервисом
	http.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
		handleMovies(w, r, monolithProxy, moviesProxy, int(percent))
	})

	http.HandleFunc("/api/events", eventsProxy.ServeHTTP)

	// Остальные запросы отправляем на монолит
	http.HandleFunc("/", monolithProxy.ServeHTTP)

	log.Printf("Starting proxy service on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleMovies(w http.ResponseWriter, r *http.Request, monolith, movies *httputil.ReverseProxy, percent int) {
	target := monolith
	if percent > 0 {
		if percent >= 100 || rand.Intn(100) < percent {
			target = movies
			log.Println("Отправлено на новый сервис")
		} else {
			log.Println("Отправлено на монолит")
		}
	}
	target.ServeHTTP(w, r)
}

func newProxy(rawURL string) *httputil.ReverseProxy {
	target, err := url.Parse(rawURL)
	if err != nil {
		log.Fatalf("Невереный url %s: %v", rawURL, err)
	}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(req *httputil.ProxyRequest) {
			req.SetURL(target)
			req.Out.Host = target.Host
		},
	}
	return proxy
}
