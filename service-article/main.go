package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASS")
	dbName := os.Getenv("DB_NAME")
	rmqUser := os.Getenv("RABBITMQ_USER")
	rmqPass := os.Getenv("RABBITMQ_PASS")
	// 1. Endpoint dummy untuk test Nginx
	http.HandleFunc("/api/submit", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Nginx Load Balancer success request to Go!")
	})

	// 2. Endpoint khusus untuk ngetest koneksi DB dan Broker dari dalam Docker
	http.HandleFunc("/api/forward", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")

		// A. Test Database (Lewat PgBouncer)
		dbURL := fmt.Sprintf("postgres://%s:%s@pgbouncer:5432/%s", dbUser, dbPass, dbName)
		connDB, err := pgx.Connect(context.Background(), dbURL)
		if err != nil {
			fmt.Fprintf(w, "fail to PostgreSQL/PgBouncer: %v\n", err)
		} else {
			fmt.Fprintln(w, "success connect PostgreSQL via PgBouncer!")
			connDB.Close(context.Background())
		}

		// B. Test Message Broker (RabbitMQ)
		rmqURL := fmt.Sprintf("amqp://%s:%s@rabbitmq:5672/", rmqUser, rmqPass)
		connRMQ, err := amqp.Dial(rmqURL)
		if err != nil {
			fmt.Fprintf(w, "fail to RabbitMQ: %v\n", err)
		} else {
			fmt.Fprintln(w, "success connect RabbitMQ!")
			connRMQ.Close()
		}
	})

	server := &http.Server{
		Addr:         ":8080",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	log.Println("running in port 8080...")
	log.Fatal(server.ListenAndServe())
}
