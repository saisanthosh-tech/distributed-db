package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
)

// Simple in-memory key-value store
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
		w.Write([]byte(value))

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
	// Codespaces will automatically make this port public.
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
```

#### **Step 2: Use Two Terminals Correctly**

This is the most important part. We need two terminals. One to be the server, one to be the client.

**Terminal A (The Server):**

1.  Open your Codespaces terminal.
2.  Run this command to start the server:
    ```bash
    go run simple_db.go
    ```
3.  The terminal will print `Server starting on port 8080...`. The terminal will now be **busy**. You will **not** get a new command prompt (`$`). This is correct. The server is running. **Leave this terminal alone.**



**Terminal B (The Client):**

1.  Open a **new, second terminal** by clicking the `+` icon.
2.  In this new terminal, you will test the server that is running in Terminal A.
3.  First, **write** some data by running this command:
    ```bash
    curl -X PUT -d "it finally works" http://localhost:8080/keys/test
    ```
4.  Now, **read** the data back by running this command:
    ```bash
    curl http://localhost:8080/keys/test
    ```

### **The Result**

The final command in **Terminal B** will print:
```
it finally works
