// router.go
package main

import (
	"crypto/sha1"
	"encoding/hex"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// Shard represents a single shard in our cluster, containing the leader's address.
// In a real system, the router would dynamically discover the current leader of a shard.
type Shard struct {
	Name    string
	Address string
}

// Router holds the configuration for all shards.
type Router struct {
	shards []*Shard
}

// NewRouter creates a new router with a predefined shard configuration.
func NewRouter() *Router {
	// For this project, we hardcode the initial leaders. A production system
	// would use a service discovery mechanism (like etcd or Zookeeper)
	// for the router to find the current leader of each shard, especially after a failover.
	return &Router{
		shards: []*Shard{
			{Name: "shard-0", Address: "http://localhost:8080"}, // Leader of Shard 0
			{Name: "shard-1", Address: "http://localhost:9090"}, // Leader of Shard 1
		},
	}
}

// getShardForKey determines which shard is responsible for a given key.
// It uses a simple consistent hashing approach to distribute keys evenly.
func (r *Router) getShardForKey(key string) *Shard {
	// Use SHA1 to hash the key, providing a good distribution.
	h := sha1.New()
	h.Write([]byte(key))
	hash := hex.EncodeToString(h.Sum(nil))

	// A simple modulo of the first character of the hash determines the shard.
	// This ensures that the same key always maps to the same shard.
	shardIndex := int(hash[0]) % len(r.shards)
	return r.shards[shardIndex]
}

// handleRequest is the main handler for the router.
// It acts as a reverse proxy, forwarding requests to the appropriate shard.
func (r *Router) handleRequest(w http.ResponseWriter, req *http.Request) {
	// Extract the key from the URL path.
	key := req.URL.Path[len("/keys/"):]
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	// Find the correct shard for this key.
	shard := r.getShardForKey(key)
	log.Printf("Routing key '%s' to %s (%s)", key, shard.Name, shard.Address)

	// Parse the destination URL for the proxy.
	remote, err := url.Parse(shard.Address)
	if err != nil {
		log.Printf("Error parsing shard address: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Create a reverse proxy to forward the request.
	// This handles all the details of copying headers and the request body.
	proxy := httputil.NewSingleHostReverseProxy(remote)
	proxy.ServeHTTP(w, req)
}

func main() {
	router := NewRouter()
	http.HandleFunc("/keys/", router.handleRequest)

	log.Println("Starting router on :7070...")
	if err := http.ListenAndServe(":7070", nil); err != nil {
		log.Fatalf("Failed to start router: %v", err)
	}
}

