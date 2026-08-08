package main

import (
	"html/template"
	"log"
	"net/http"

	"github.com/joncody/roomer"
)

var index = template.Must(template.ParseFiles("examples/index.html"))

func handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	index.Execute(w, nil)
}

func main() {
	if err := roomer.RegisterHandler("chat", func(c *roomer.Conn, msg *roomer.Message) error {
		c.SendToRoom(msg.Room, msg.Event, msg.Payload)
		return nil
	}); err != nil {
		log.Fatal("Failed to register handler:", err)
	}

	// 1. Serve root page
	http.HandleFunc("/", handler)

	// 2. Serve core JS library files from src/ (/src/roomer.js, /src/bytecursor.js, etc.)
	http.Handle("/src/", http.StripPrefix("/src/", http.FileServer(http.Dir("src"))))

	// 3. Serve example app static files (/static/js/index.js)
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("examples/static"))))

	// 4. Serve test suite (/tests/index.html, /tests/static/...)
	http.Handle("/tests/", http.StripPrefix("/tests/", http.FileServer(http.Dir("tests"))))

	// 5. WebSocket handler
	http.HandleFunc("/ws", roomer.SocketHandler(nil))

	log.Println("Server running at http://localhost:8080/")
	log.Println("Run test suite at http://localhost:8080/tests/")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
