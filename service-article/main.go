package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

// submitRequest is the expected JSON body for POST /api/submit.
type submitRequest struct {
	Text string `json:"text"`
}

// submitResponse is returned immediately with HTTP 202 after a successful submit.
type submitResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// articleResult is the shape of a row returned by GET /api/forward.
type articleResult struct {
	ID             string  `json:"id"`
	Status         string  `json:"status"`
	RawContent     string  `json:"raw_content"`
	StemmedContent *string `json:"stemmed_content"`
	WordcloudURL   *string `json:"wordcloud_url"`
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
}

// publishPayload is what we put on the RabbitMQ exchange.
type publishPayload struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

// ---------------------------------------------------------------------------
// App — holds shared infrastructure resources
// ---------------------------------------------------------------------------

type App struct {
	db      *pgxpool.Pool
	amqpCh  *amqp.Channel
	amqpConn *amqp.Connection
}

const exchangeName = "articles_exchange"

// ---------------------------------------------------------------------------
// Startup helpers
// ---------------------------------------------------------------------------

func connectDB(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@pgbouncer:5432/%s",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASS"),
		os.Getenv("DB_NAME"),
	)

	const maxAttempts = 10
	for i := 1; i <= maxAttempts; i++ {
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			if pingErr := pool.Ping(ctx); pingErr == nil {
				log.Printf("[DB] Connected to PostgreSQL via PgBouncer (attempt %d)", i)
				return pool, nil
			} else {
				pool.Close()
				err = pingErr
			}
		}
		log.Printf("[DB] Attempt %d/%d failed: %v — retrying in 3s…", i, maxAttempts, err)
		time.Sleep(3 * time.Second)
	}
	return nil, fmt.Errorf("could not connect to PostgreSQL after %d attempts", maxAttempts)
}


func connectRabbitMQ() (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf("amqp://%s:%s@rabbitmq:5672/",
		os.Getenv("RABBITMQ_USER"),
		os.Getenv("RABBITMQ_PASS"),
	)

	const maxAttempts = 10
	var (
		conn *amqp.Connection
		err  error
	)
	for i := 1; i <= maxAttempts; i++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		log.Printf("[RMQ] Attempt %d/%d failed: %v — retrying in 3s…", i, maxAttempts, err)
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("could not connect to RabbitMQ after %d attempts: %w", maxAttempts, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("could not open AMQP channel: %w", err)
	}

	if err := ch.ExchangeDeclare(
		exchangeName, 
		"fanout",     
		true,         
		false,        
		false,        
		false,        
		nil,          
	); err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("could not declare exchange: %w", err)
	}

	// Enable publisher confirms so we know the broker acknowledged every message.
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("could not enable publisher confirms: %w", err)
	}

	log.Println("[RMQ] Connected — fanout exchange declared, publisher confirms enabled")
	return conn, ch, nil
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

// handleSubmit implements POST /api/submit
//
// 1. Decode {"text": "..."} from the request body.
// 2. INSERT a new row into the articles table (status = pending).
// 3. Respond with HTTP 202 + {"id": "...", "status": "pending"}.
// 4. Publish {"id": "...", "text": "..."} to the RabbitMQ fanout exchange.
func (a *App) handleSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Text == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid request body — 'text' field is required"})
		return
	}

	// --- 1. Persist to PostgreSQL ---
	var articleID string
	err := a.db.QueryRow(
		r.Context(),
		`INSERT INTO articles (raw_content, status) VALUES ($1, 'pending') RETURNING id`,
		req.Text,
	).Scan(&articleID)
	if err != nil {
		log.Printf("[submit] DB insert error: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to store article"})
		return
	}

	// --- 2. Respond immediately with 202 ---
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(submitResponse{
		ID:     articleID,
		Status: "pending",
	})

	// --- 3. Publish to RabbitMQ (after the response is flushed) ---
	payload, _ := json.Marshal(publishPayload{ID: articleID, Text: req.Text})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	confirms := a.amqpCh.NotifyPublish(make(chan amqp.Confirmation, 1))

	pubErr := a.amqpCh.PublishWithContext(
		ctx,
		exchangeName, 
		"",           
		true,         
		false,        
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         payload,
		},
	)
	if pubErr != nil {
		log.Printf("[submit] RMQ publish error for article %s: %v", articleID, pubErr)
		return
	}

	select {
	case confirm := <-confirms:
		if confirm.Ack {
			log.Printf("[submit] Article %s published and ACK'd by broker", articleID)
		} else {
			log.Printf("[submit] Article %s was NACK'd by broker", articleID)
		}
	case <-ctx.Done():
		log.Printf("[submit] Timed out waiting for broker ACK for article %s", articleID)
	}
}

// handleForward implements GET /api/forward
//
// Query params:
//   - ?id=<uuid>  → fetch a specific article
//   - (none)      → fetch all articles (ordered by created_at DESC, limit 50)
func (a *App) handleForward(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	id := r.URL.Query().Get("id")

	if id != "" {
		var art articleResult
		var createdAt, updatedAt time.Time

		err := a.db.QueryRow(
			r.Context(),
			`SELECT id, status::text, raw_content, stemmed_content, wordcloud_url, created_at, updated_at
			 FROM articles WHERE id = $1`,
			id,
		).Scan(
			&art.ID,
			&art.Status,
			&art.RawContent,
			&art.StemmedContent,
			&art.WordcloudURL,
			&createdAt,
			&updatedAt,
		)
		if err != nil {
			log.Printf("[forward] Article %s not found: %v", id, err)
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "article not found"})
			return
		}

		art.CreatedAt = createdAt.Format(time.RFC3339)
		art.UpdatedAt = updatedAt.Format(time.RFC3339)

		json.NewEncoder(w).Encode(art)
		return
	}

	rows, err := a.db.Query(
		r.Context(),
		`SELECT id, status::text, raw_content, stemmed_content, wordcloud_url, created_at, updated_at
		 FROM articles
		 ORDER BY created_at DESC
		 LIMIT 50`,
	)
	if err != nil {
		log.Printf("[forward] DB query error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to query articles"})
		return
	}
	defer rows.Close()

	results := make([]articleResult, 0)
	for rows.Next() {
		var art articleResult
		var createdAt, updatedAt time.Time
		if err := rows.Scan(
			&art.ID,
			&art.Status,
			&art.RawContent,
			&art.StemmedContent,
			&art.WordcloudURL,
			&createdAt,
			&updatedAt,
		); err != nil {
			log.Printf("[forward] Row scan error: %v", err)
			continue
		}
		art.CreatedAt = createdAt.Format(time.RFC3339)
		art.UpdatedAt = updatedAt.Format(time.RFC3339)
		results = append(results, art)
	}

	json.NewEncoder(w).Encode(results)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, `{"status":"ok"}`)
}



func main() {
	ctx := context.Background()

	// --- Connect to infrastructure (with retry) ---
	db, err := connectDB(ctx)
	if err != nil {
		log.Fatalf("[startup] %v", err)
	}
	defer db.Close()

	amqpConn, amqpCh, err := connectRabbitMQ()
	if err != nil {
		log.Fatalf("[startup] %v", err)
	}
	defer amqpCh.Close()
	defer amqpConn.Close()

	app := &App{
		db:       db,
		amqpCh:   amqpCh,
		amqpConn: amqpConn,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/submit", app.handleSubmit)
	mux.HandleFunc("/api/forward", app.handleForward)
	mux.HandleFunc("/health", handleHealth)

	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Println("[startup] Article service listening on :8080")
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("[server] %v", err)
	}
}
