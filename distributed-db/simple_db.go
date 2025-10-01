package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
)

// Task represents a single to-do item
type Task struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
	Done  bool   `json:"done"`
}

// In-memory store for our tasks
var (
	tasks  = make(map[int]Task)
	nextID = 1
	mu     sync.Mutex
)

// tasksHandler handles requests to /tasks
func tasksHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		mu.Lock()
		defer mu.Unlock()
		taskList := make([]Task, 0, len(tasks))
		for _, task := range tasks {
			taskList = append(taskList, task)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(taskList)

	case "POST":
		var task Task
		if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		mu.Lock()
		defer mu.Unlock()
		task.ID = nextID
		nextID++
		tasks[task.ID] = task
		w.WriteHeader(http.StatusCreated)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(task)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func main() {
	http.HandleFunc("/tasks", tasksHandler)
	log.Println("Server starting on port 8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
```

#### **Step 4: Run the Final Test**

Now that we have a clean file, this will work.

**In Terminal A:**
```bash
go run simple_db.go
```
*The server will start, and the terminal will become busy.*

**In a new Terminal B:**
```bash
curl -X POST -H "Content-Type: application/json" -d '{"title": "This is the one"}' http://localhost:8080/tasks
curl http://localhost:8080/tasks

