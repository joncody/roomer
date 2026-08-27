package main

import (
	"html/template"
	"log/slog"
	"net/http"
	"os"

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

	logger.Info("Server running at http://localhost:8080/")
	logger.Info("Run test suite at http://localhost:8080/tests/")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		logger.Error("Server terminated unexpectedly", "err", err)
		os.Exit(1)
	}
}
