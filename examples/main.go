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

	"github.com/joncody/roomer"
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

func main() {
	// Configure structured slog text logger for readable console output
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	// Register custom chat event handler with structured logging
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

	// 1. Serve root page
	http.HandleFunc("/", handler)

	// 2. Serve core JS library files from src/ (/src/roomer.js, /src/bytecursor.js, etc.)
	http.Handle("/src/", http.StripPrefix("/src/", http.FileServer(http.Dir("src"))))

	// 3. Serve example app static files (/static/js/index.js)
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("examples/static"))))

	// 4. Serve test suite (/tests/index.html, /tests/static/...)
	http.Handle("/tests/", http.StripPrefix("/tests/", http.FileServer(http.Dir("tests"))))

	// 5. WebSocket handler with structured logger configured
	http.HandleFunc("/ws", roomer.SocketHandlerWithOptions(
		roomer.WithLogger(logger),
	))

	server := &http.Server{Addr: ":8080"}

	// Listen for interrupt signals (Ctrl+C, SIGTERM) to demonstrate graceful shutdown
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
	}()

	logger.Info("Server running at http://localhost:8080/")
	logger.Info("Run test suite at http://localhost:8080/tests/")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("Server terminated unexpectedly", "err", err)
		os.Exit(1)
	}
	logger.Info("Server shutdown complete")
}
