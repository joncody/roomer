package main

import (
	"context"
	"html/template"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joncody/roomer-go-go"
	redisadapter "github.com/joncody/roomer-go-go/adapter/redis"
	"github.com/redis/go-redis/v9"
)

var index = template.Must(template.ParseFiles("examples/index.html"))

func handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := index.Execute(w, nil); err != nil {
		slog.Error("Template execution error", "err", err)
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	port := getEnv("PORT", "8080")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")

	// 1. Structured Logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	// 2. Connect Redis Adapter
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Test connection
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pingCancel()

	var adapter roomer.Adapter
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		logger.Warn("Could not connect to Redis; running in standalone single-node mode", "err", err)
	} else {
		var err error
		adapter, err = redisadapter.New(rdb,
			redisadapter.WithPrefix("roomer:demo:"),
			redisadapter.WithLogger(logger),
		)
		if err != nil {
			logger.Error("Failed to initialize Redis adapter", "err", err)
			os.Exit(1)
		}
		logger.Info("Connected to Redis cluster", "node_id", adapter.(*redisadapter.Adapter).NodeID())
	}

	// 3. Register custom event handler
	err := roomer.RegisterHandler("chat", func(c *roomer.Conn, msg *roomer.Message) error {
		logger.Info("Chat message received",
			"room", msg.Room,
			"sender", msg.Src,
			"bytes", len(msg.Payload),
		)
		c.SendToRoom(msg.Room, msg.Event, msg.Payload)
		return nil
	})
	if err != nil {
		logger.Error("Failed to register handler", "err", err)
		os.Exit(1)
	}

	// 4. Register HTTP Routes
	http.HandleFunc("/", handler)
	http.Handle("/src/", http.StripPrefix("/src/", http.FileServer(http.Dir("src"))))
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("examples/static"))))
	http.Handle("/tests/", http.StripPrefix("/tests/", http.FileServer(http.Dir("tests"))))

	// 5. Mount WebSocket handler
	opts := []roomer.Option{
		roomer.WithLogger(logger),
	}
	if adapter != nil {
		opts = append(opts, roomer.WithAdapter(adapter))
	}
	http.HandleFunc("/ws", roomer.SocketHandlerWithOptions(opts...))

	server := &http.Server{Addr: ":" + port}

	// 6. Graceful Shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan

		logger.Info("Shutting down server gracefully...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := roomer.Shutdown(shutdownCtx); err != nil {
			logger.Error("Roomer shutdown error", "err", err)
		}
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("HTTP server shutdown error", "err", err)
		}
		if rdb != nil {
			_ = rdb.Close()
		}
	}()

	logger.Info("Server running at http://localhost:" + port + "/")
	logger.Info("Run test suite at http://localhost:" + port + "/tests/")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("Server terminated unexpectedly", "err", err)
		os.Exit(1)
	}
	logger.Info("Server shutdown complete")
}
