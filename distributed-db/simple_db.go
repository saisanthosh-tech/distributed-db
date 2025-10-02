package main

import (
	"io"
	"log"
	"net/http"
	"sync"
)

// In-memory key-value store
var (
	store = make(map[string]string)
	mu    sync.Mutex
)

// The handler for reading and writing keys
func keyHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/keys/"):] // Get the key from the URL

	switch r.Method {
	case "GET":
		mu.Lock()
		value, ok := store[key]
		mu.Unlock()

		if !ok {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}
		io.WriteString(w, value)

	case "PUT":
		value, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusInternalServerError)
			return
		}
		mu.Lock()
		store[key] = string(value)
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func main() {
	http.HandleFunc("/keys/", keyHandler)
	log.Println("Server starting on port 8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
```

### **Step 3: The Final, Successful Test**

Now that the code is clean, this will work.

**In Terminal A (Your first terminal):**

1.  Run this command to start the server. The terminal will print `Server starting...` and then become busy. **THIS IS CORRECT. LEAVE IT RUNNING.**
    ```bash
    go run simple_db.go
    ```

**In Terminal B (A new, second terminal):**

1.  Open a **new terminal** by clicking the `+` icon.
2.  In this new terminal, first **write** the data:
    ```bash
    curl -X PUT -d "it works" http://localhost:8080/keys/final-test
    ```
3.  Now, **read** the data back:
    ```bash
    curl http://localhost:8080/keys/final-test
    

