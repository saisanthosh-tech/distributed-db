// // main.go
// package main

// import (
// 	"bytes"
// 	"encoding/json"
// 	"flag"
// 	"io"
// 	"log"
// 	"net/http"
// 	"sort"
// 	"sync"
// 	"time"
// )

// // --- 1. Data Structures ---

// // Node represents a single server in our distributed database cluster.
// type Node struct {
// 	mu            sync.RWMutex      // A mutex to protect concurrent access to the data store.
// 	store         map[string]string // The in-memory key-value store.
// 	addr          string            // The network address of this node (e.g., ":8080").
// 	isLeader      bool              // True if this node is the leader.
// 	leaderAddr    string            // The address of the current leader node.
// 	peerAddrs     []string          // A list of addresses for all peer nodes in the cluster.
// 	lastHeartbeat time.Time         // For followers, the time the last heartbeat was received.
// }

// // KeyValue represents a single key-value pair for API requests/responses.
// type KeyValue struct {
// 	Key   string `json:"key"`
// 	Value string `json:"value"`
// }

// // ElectionMessage is sent by a candidate to request a vote.
// type ElectionMessage struct {
// 	CandidateAddr string `json:"candidate_addr"`
// }

// // --- 2. Global State & Configuration ---

// var node *Node

// // --- 3. Core Node Logic ---

// // NewNode creates and initializes a new database node.
// func NewNode(addr string, leaderAddr string, peers []string) *Node {
// 	isLeader := (addr == leaderAddr)

// 	return &Node{
// 		store:         make(map[string]string),
// 		addr:          addr,
// 		isLeader:      isLeader,
// 		leaderAddr:    leaderAddr,
// 		peerAddrs:     peers,
// 		lastHeartbeat: time.Now(), // Initialize heartbeat time
// 	}
// }

// // --- 4. HTTP Handlers ---

// // handleRequests is the main entry point for all incoming HTTP requests.
// // It acts as a router, directing requests to the appropriate handler based on the node's role.
// func handleRequests(w http.ResponseWriter, r *http.Request) {
// 	// We've added new internal endpoints for the election process.
// 	if r.URL.Path == "/heartbeat" {
// 		handleHeartbeat(w, r)
// 		return
// 	}
// 	if r.URL.Path == "/request-vote" {
// 		handleRequestVote(w, r)
// 		return
// 	}

// 	if node.isLeader {
// 		// Leader can handle reads, writes, and deletes.
// 		switch r.Method {
// 		case "GET":
// 			handleGet(w, r)
// 		case "PUT":
// 			handlePut(w, r)
// 		case "DELETE":
// 			handleDelete(w, r)
// 		default:
// 			http.Error(w, "Method not allowed for leader", http.StatusMethodNotAllowed)
// 		}
// 	} else {
// 		// Followers can only handle reads and internal replication requests.
// 		switch r.Method {
// 		case "GET":
// 			handleGet(w, r)
// 		case "POST":
// 			// The POST method is used for our internal replication endpoint.
// 			if r.URL.Path == "/replicate" {
// 				handleReplicate(w, r)
// 			} else {
// 				http.Error(w, "Not found", http.StatusNotFound)
// 			}
// 		default:
// 			// Followers should redirect write/delete requests to the leader.
// 			// If the follower doesn't know who the leader is, it returns an error.
// 			if node.leaderAddr == "" {
// 				http.Error(w, "Leader is currently not available", http.StatusServiceUnavailable)
// 				return
// 			}
// 			http.Redirect(w, r, "http://"+node.leaderAddr+r.URL.String(), http.StatusTemporaryRedirect)
// 		}
// 	}
// }

// // handleGet retrieves a value from the key-value store.
// func handleGet(w http.ResponseWriter, r *http.Request) {
// 	key := r.URL.Path[len("/keys/"):]
// 	if key == "" {
// 		http.Error(w, "Key is required", http.StatusBadRequest)
// 		return
// 	}

// 	node.mu.RLock()
// 	defer node.mu.RUnlock()

// 	value, ok := node.store[key]
// 	if !ok {
// 		http.Error(w, "Key not found", http.StatusNotFound)
// 		return
// 	}

// 	// Respond with the value
// 	w.WriteHeader(http.StatusOK)
// 	w.Write([]byte(value))
// 	log.Printf("[%s] GET key='%s', value='%s'", node.addr, key, value)
// }

// // handlePut adds or updates a key-value pair. This is a leader-only operation.
// func handlePut(w http.ResponseWriter, r *http.Request) {
// 	key := r.URL.Path[len("/keys/"):]
// 	if key == "" {
// 		http.Error(w, "Key is required", http.StatusBadRequest)
// 		return
// 	}

// 	body, err := io.ReadAll(r.Body)
// 	if err != nil {
// 		http.Error(w, "Error reading request body", http.StatusInternalServerError)
// 		return
// 	}
// 	value := string(body)

// 	// Update the local store
// 	node.mu.Lock()
// 	node.store[key] = value
// 	node.mu.Unlock()

// 	log.Printf("[%s] PUT key='%s', value='%s'", node.addr, key, value)

// 	// Replicate the write to all followers
// 	replicateToFollowers(KeyValue{Key: key, Value: value})

// 	w.WriteHeader(http.StatusOK)
// }

// // handleDelete removes a key. This is a leader-only operation.
// func handleDelete(w http.ResponseWriter, r *http.Request) {
// 	key := r.URL.Path[len("/keys/"):]
// 	if key == "" {
// 		http.Error(w, "Key is required", http.StatusBadRequest)
// 		return
// 	}

// 	// Update the local store
// 	node.mu.Lock()
// 	delete(node.store, key)
// 	node.mu.Unlock()

// 	log.Printf("[%s] DELETE key='%s'", node.addr, key)

// 	// Replicate the delete to all followers (by sending an empty value)
// 	replicateToFollowers(KeyValue{Key: key, Value: ""}) // Empty value signifies deletion

// 	w.WriteHeader(http.StatusOK)
// }

// // --- 5. Replication & Leader Election Logic ---

// // replicateToFollowers sends the key-value pair to all follower nodes.
// func replicateToFollowers(kv KeyValue) {
// 	var wg sync.WaitGroup
// 	// Replicate to all peers except self
// 	node.mu.RLock()
// 	peers := node.peerAddrs
// 	node.mu.RUnlock()

// 	for _, peerAddr := range peers {
// 		if peerAddr == node.addr {
// 			continue
// 		}
// 		wg.Add(1)
// 		go func(addr string) {
// 			defer wg.Done()
// 			sendReplicationRequest(addr, kv)
// 		}(peerAddr)
// 	}
// 	wg.Wait() // Wait for all replications to complete
// }

// // sendReplicationRequest sends a single replication request to a follower.
// func sendReplicationRequest(followerAddr string, kv KeyValue) {
// 	jsonData, err := json.Marshal(kv)
// 	if err != nil {
// 		log.Printf("[%s] Error marshalling data for replication: %v", node.addr, err)
// 		return
// 	}

// 	url := "http://" + followerAddr + "/replicate"
// 	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
// 	if err != nil {
// 		log.Printf("[%s] Error creating replication request for %s: %v", node.addr, followerAddr, err)
// 		return
// 	}
// 	req.Header.Set("Content-Type", "application/json")

// 	client := &http.Client{Timeout: 2 * time.Second} // Shorter timeout for internal calls
// 	resp, err := client.Do(req)
// 	if err != nil {
// 		log.Printf("[%s] Failed to replicate to %s: %v", node.addr, followerAddr, err)
// 		return
// 	}
// 	defer resp.Body.Close()

// 	if resp.StatusCode != http.StatusOK {
// 		log.Printf("[%s] Replication to %s failed with status: %s", node.addr, followerAddr, resp.Status)
// 	} else {
// 		log.Printf("[%s] Successfully replicated key='%s' to %s", node.addr, kv.Key, followerAddr)
// 	}
// }

// // handleReplicate is the endpoint on follower nodes that receives replication data from the leader.
// func handleReplicate(w http.ResponseWriter, r *http.Request) {
// 	var kv KeyValue
// 	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
// 		http.Error(w, "Invalid request body", http.StatusBadRequest)
// 		return
// 	}

// 	node.mu.Lock()
// 	if kv.Value == "" {
// 		// An empty value signifies a delete operation
// 		delete(node.store, kv.Key)
// 		log.Printf("[%s] Replicated DELETE for key='%s'", node.addr, kv.Key)
// 	} else {
// 		node.store[kv.Key] = kv.Value
// 		log.Printf("[%s] Replicated PUT for key='%s', value='%s'", node.addr, kv.Key, kv.Value)
// 	}
// 	node.mu.Unlock()

// 	w.WriteHeader(http.StatusOK)
// }

// // handleHealthCheck is a simple endpoint to verify a node is running.
// func handleHealthCheck(w http.ResponseWriter, r *http.Request) {
// 	w.WriteHeader(http.StatusOK)
// }

// // --- Heartbeat and Election Functions ---

// // startHeartbeats is called by the leader to periodically notify followers it is alive.
// func startHeartbeats() {
// 	// This ticker will fire every 2 seconds.
// 	ticker := time.NewTicker(2 * time.Second)
// 	defer ticker.Stop()

// 	for range ticker.C {
// 		// If this node is no longer the leader, stop sending heartbeats.
// 		if !node.isLeader {
// 			return
// 		}

// 		// Send a heartbeat to each peer.
// 		node.mu.RLock()
// 		peers := node.peerAddrs
// 		node.mu.RUnlock()
// 		for _, peerAddr := range peers {
// 			if peerAddr != node.addr {
// 				go sendHeartbeat(peerAddr)
// 			}
// 		}
// 	}
// }

// // sendHeartbeat sends a single heartbeat POST request to a peer.
// func sendHeartbeat(peerAddr string) {
// 	url := "http://" + peerAddr + "/heartbeat"
// 	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(`{"leader_addr":"`+node.addr+`"}`)))
// 	if err != nil {
// 		return
// 	}
// 	req.Header.Set("Content-Type", "application/json")

// 	client := &http.Client{Timeout: 1 * time.Second}
// 	_, err = client.Do(req)
// 	if err != nil {
// 		// Don't log failures here as they are expected if a node is down.
// 		return
// 	}
// }

// // handleHeartbeat is the endpoint on followers that receives heartbeats from the leader.
// func handleHeartbeat(w http.ResponseWriter, r *http.Request) {
// 	var payload struct {
// 		LeaderAddr string `json:"leader_addr"`
// 	}
// 	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
// 		http.Error(w, "Invalid request", http.StatusBadRequest)
// 		return
// 	}

// 	node.mu.Lock()
// 	node.leaderAddr = payload.LeaderAddr
// 	node.isLeader = (node.addr == node.leaderAddr)
// 	node.lastHeartbeat = time.Now()
// 	node.mu.Unlock()

// 	w.WriteHeader(http.StatusOK)
// }

// // monitorHeartbeat is a background process run by followers to check if the leader is still alive.
// func monitorHeartbeat() {
// 	// This ticker checks every 3 seconds.
// 	ticker := time.NewTicker(3 * time.Second)
// 	defer ticker.Stop()

// 	for range ticker.C {
// 		if node.isLeader {
// 			continue // Leaders don't need to monitor.
// 		}

// 		node.mu.RLock()
// 		last := node.lastHeartbeat
// 		node.mu.RUnlock()

// 		// If it's been more than 5 seconds since the last heartbeat, assume leader is down.
// 		if time.Since(last) > 5*time.Second {
// 			log.Printf("[%s] Leader timed out. Starting a new election.", node.addr)
// 			startElection()
// 		}
// 	}
// }

// // startElection begins the process of electing a new leader.
// func startElection() {
// 	// In our simplified model, the node with the "lowest" address (e.g., ":8081" before ":8082")
// 	// will always win the election. This avoids the complexity of a full Raft implementation
// 	// while still demonstrating the failover mechanism.

// 	// Create a sorted list of all peers that are still alive.
// 	var livePeers []string
// 	livePeers = append(livePeers, node.addr) // The current node is alive.

// 	node.mu.RLock()
// 	peers := node.peerAddrs
// 	node.mu.RUnlock()
// 	for _, peerAddr := range peers {
// 		if peerAddr == node.addr {
// 			continue
// 		}
// 		// Try to ping other nodes to see if they are alive.
// 		if isPeerAlive(peerAddr) {
// 			livePeers = append(livePeers, peerAddr)
// 		}
// 	}

// 	// Sort the addresses alphabetically. The first one in the list is the new leader.
// 	sort.Strings(livePeers)
// 	newLeaderAddr := livePeers[0]

// 	log.Printf("[%s] Election concluded. New leader is %s.", node.addr, newLeaderAddr)

// 	// Update self
// 	node.mu.Lock()
// 	node.leaderAddr = newLeaderAddr
// 	node.isLeader = (node.addr == newLeaderAddr)
// 	// THE FIX: All nodes update their peer list to only include live nodes.
// 	node.peerAddrs = livePeers
// 	node.mu.Unlock()

// 	// If this node just became the leader, it needs to start sending heartbeats.
// 	if node.isLeader {
// 		go startHeartbeats()
// 	}
// }

// // isPeerAlive checks if another node is responsive.
// func isPeerAlive(peerAddr string) bool {
// 	url := "http://" + peerAddr + "/health" // Use the new, safe health endpoint
// 	req, err := http.NewRequest("GET", url, nil)
// 	if err != nil {
// 		return false
// 	}

// 	client := &http.Client{Timeout: 1 * time.Second}
// 	resp, err := client.Do(req)
// 	if err != nil {
// 		return false
// 	}
// 	defer resp.Body.Close()
// 	return resp.StatusCode == http.StatusOK
// }

// // We add a placeholder for handleRequestVote, as a full implementation is complex.
// // Our simplified election logic doesn't require it.
// func handleRequestVote(w http.ResponseWriter, r *http.Request) {
// 	w.WriteHeader(http.StatusOK)
// }


// // --- 6. Main Function ---

// func main() {
// 	// Parse command-line flags to determine this node's role and address.
// 	addr := flag.String("addr", ":8080", "The network address for this node to listen on")
// 	leaderAddr := flag.String("leader", ":8080", "The network address of the leader node")
// 	flag.Parse()

// 	// All nodes in our cluster. In a real system, this would be discovered dynamically.
// 	peers := []string{":8080", ":8081", ":8082"}

// 	// Initialize the node
// 	node = NewNode(*addr, *leaderAddr, peers)

// 	// Set up the HTTP server
// 	http.HandleFunc("/keys/", handleRequests)
// 	http.HandleFunc("/replicate", handleReplicate)
// 	http.HandleFunc("/heartbeat", handleHeartbeat)
// 	http.HandleFunc("/request-vote", handleRequestVote)
// 	http.HandleFunc("/health", handleHealthCheck)


// 	// Start background tasks based on role.
// 	if node.isLeader {
// 		// The leader starts sending heartbeats immediately.
// 		go startHeartbeats()
// 	} else {
// 		// Followers start monitoring for heartbeats.
// 		go monitorHeartbeat()
// 	}

// 	// Start the server
// 	role := "Follower"
// 	if node.isLeader {
// 		role = "Leader"
// 	}
// 	log.Printf("Starting node as %s on %s", role, node.addr)
// 	log.Fatal(http.ListenAndServe(node.addr, nil))
// }






// main.go
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

// --- 1. Data Structures ---

// Node represents a single server in our distributed database cluster.
type Node struct {
	mu            sync.RWMutex
	store         map[string]string
	addr          string
	isLeader      bool
	leaderAddr    string
	peerAddrs     []string
	lastHeartbeat time.Time
}

// KeyValue represents a single key-value pair for API requests/responses.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// --- 2. Global State & Configuration ---

var node *Node

// This map defines the nodes in each shard. In a real system, this would come from a config file or service.
var shardConfig = map[int][]string{
	0: {":8080", ":8081", ":8082"},
	1: {":9090", ":9091", ":9092"},
}

// --- 3. Core Node Logic ---

// NewNode creates and initializes a new database node.
func NewNode(addr string, leaderAddr string, peers []string) *Node {
	return &Node{
		store:         make(map[string]string),
		addr:          addr,
		isLeader:      (addr == leaderAddr),
		leaderAddr:    leaderAddr,
		peerAddrs:     peers,
		lastHeartbeat: time.Now(),
	}
}

// --- 4. Main Function ---

func main() {
	// SHARDING UPDATE: New flags for shard configuration.
	addr := flag.String("addr", ":8080", "The network address for this node to listen on")
	shardID := flag.Int("shard", 0, "The ID of the shard this node belongs to")
	flag.Parse()

	// Determine peers and leader for this node's shard.
	peers, ok := shardConfig[*shardID]
	if !ok {
		log.Fatalf("Invalid shard ID: %d", *shardID)
	}
	// The leader is always the first node in the shard's peer list by convention.
	leaderAddr := peers[0]

	// Initialize the node.
	node = NewNode(*addr, leaderAddr, peers)

	// Setup HTTP endpoints for both external and internal APIs.
	http.HandleFunc("/keys/", handleClientRequests)
	http.HandleFunc("/replicate", handleReplicate)
	http.HandleFunc("/heartbeat", handleHeartbeat)
	http.HandleFunc("/health", handleHealthCheck)

	if node.isLeader {
		go startHeartbeats()
	} else {
		go monitorHeartbeat()
	}

	role := "Follower"
	if node.isLeader {
		role = "Leader"
	}
	log.Printf("Starting node for Shard %d as %s on %s", *shardID, role, node.addr)
	log.Fatal(http.ListenAndServe(node.addr, nil))
}

// --- 5. HTTP Handlers ---

// handleClientRequests routes requests based on the node's current role (leader/follower).
func handleClientRequests(w http.ResponseWriter, r *http.Request) {
	if node.isLeader {
		switch r.Method {
		case "GET":
			handleGet(w, r)
		case "PUT":
			handlePut(w, r)
		default:
			http.Error(w, "Method not allowed for leader", http.StatusMethodNotAllowed)
		}
	} else { // Follower logic
		if r.Method == "GET" {
			handleGet(w, r)
		} else {
			// Redirect writes/deletes to the current known leader.
			if node.leaderAddr == "" {
				http.Error(w, "Leader is currently not available", http.StatusServiceUnavailable)
				return
			}
			http.Redirect(w, r, "http://"+node.leaderAddr+r.URL.String(), http.StatusTemporaryRedirect)
		}
	}
}

// handleGet retrieves a value from the local key-value store.
func handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/keys/"):]
	node.mu.RLock()
	value, ok := node.store[key]
	node.mu.RUnlock()

	if !ok {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	w.Write([]byte(value))
	log.Printf("[%s] GET key='%s', value='%s'", node.addr, key, value)
}

// handlePut adds or updates a key-value pair (leader-only operation).
func handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/keys/"):]
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	value := string(body)

	node.mu.Lock()
	node.store[key] = value
	node.mu.Unlock()

	log.Printf("[%s] PUT key='%s', value='%s'", node.addr, key, value)
	replicateToFollowers(KeyValue{Key: key, Value: value})
	w.WriteHeader(http.StatusOK)
}

// --- 6. Replication & Leader Election Logic ---

// replicateToFollowers sends the key-value pair to all follower nodes in its shard.
func replicateToFollowers(kv KeyValue) {
	var wg sync.WaitGroup
	node.mu.RLock()
	peers := node.peerAddrs
	node.mu.RUnlock()

	for _, peerAddr := range peers {
		if peerAddr != node.addr {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				sendReplicationRequest(addr, kv)
			}(peerAddr)
		}
	}
	wg.Wait()
}

// sendReplicationRequest sends a single replication request to a follower.
func sendReplicationRequest(followerAddr string, kv KeyValue) {
	jsonData, _ := json.Marshal(kv)
	url := "http://" + followerAddr + "/replicate"
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("[%s] Failed to replicate to %s: %v", node.addr, followerAddr, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		log.Printf("[%s] Successfully replicated key='%s' to %s", node.addr, kv.Key, followerAddr)
	} else {
		log.Printf("[%s] Replication to %s failed with status: %s", node.addr, followerAddr, resp.Status)
	}
}

// handleReplicate is the endpoint on followers to receive data from the leader.
func handleReplicate(w http.ResponseWriter, r *http.Request) {
	var kv KeyValue
	json.NewDecoder(r.Body).Decode(&kv)

	node.mu.Lock()
	node.store[kv.Key] = kv.Value
	node.mu.Unlock()
	log.Printf("[%s] Replicated PUT for key='%s', value='%s'", node.addr, kv.Key, kv.Value)
	w.WriteHeader(http.StatusOK)
}

// handleHealthCheck is a simple endpoint to verify a node is running.
func handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// --- Heartbeat and Election Functions ---

// startHeartbeats is called by the leader to periodically notify followers it is alive.
func startHeartbeats() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if !node.isLeader {
			return
		}
		node.mu.RLock()
		peers := node.peerAddrs
		node.mu.RUnlock()
		for _, peerAddr := range peers {
			if peerAddr != node.addr {
				go sendHeartbeat(peerAddr)
			}
		}
	}
}

// sendHeartbeat sends a single heartbeat POST request to a peer.
func sendHeartbeat(peerAddr string) {
	url := "http://" + peerAddr + "/heartbeat"
	payload, _ := json.Marshal(map[string]string{"leader_addr": node.addr})
	http.Post(url, "application/json", bytes.NewBuffer(payload))
}

// handleHeartbeat is the endpoint on followers to receive heartbeats from the leader.
func handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var payload struct{ LeaderAddr string `json:"leader_addr"` }
	json.NewDecoder(r.Body).Decode(&payload)

	node.mu.Lock()
	node.leaderAddr = payload.LeaderAddr
	node.lastHeartbeat = time.Now()
	node.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

// monitorHeartbeat is a background process run by followers to check if the leader is alive.
func monitorHeartbeat() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if node.isLeader {
			continue
		}
		node.mu.RLock()
		last := node.lastHeartbeat
		node.mu.RUnlock()

		if time.Since(last) > 5*time.Second {
			log.Printf("[%s] Leader timed out. Starting a new election.", node.addr)
			startElection()
		}
	}
}

// startElection begins the process of electing a new leader.
func startElection() {
	var livePeers []string
	livePeers = append(livePeers, node.addr)

	node.mu.RLock()
	peers := node.peerAddrs
	node.mu.RUnlock()
	for _, peerAddr := range peers {
		if peerAddr != node.addr && isPeerAlive(peerAddr) {
			livePeers = append(livePeers, peerAddr)
		}
	}

	sort.Strings(livePeers)
	newLeaderAddr := livePeers[0]
	log.Printf("[%s] Election concluded. New leader is %s.", node.addr, newLeaderAddr)

	node.mu.Lock()
	node.leaderAddr = newLeaderAddr
	node.isLeader = (node.addr == newLeaderAddr)
	node.peerAddrs = livePeers // Update peer list to only include live nodes.
	node.mu.Unlock()

	if node.isLeader {
		go startHeartbeats()
	}
}

// isPeerAlive checks if another node is responsive.
func isPeerAlive(peerAddr string) bool {
	resp, err := http.Get("http://" + peerAddr + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

