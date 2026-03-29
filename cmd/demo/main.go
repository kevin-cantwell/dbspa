// Command demo runs an interactive browser demo for FoldDB.
package main

import (
	"bufio"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sync"
)

//go:embed static
var staticFiles embed.FS

var (
	// Active query session
	mu         sync.Mutex
	activeCmd  *exec.Cmd
	activeDone chan struct{} // signals generator to stop
	activeW    io.WriteCloser
)

func main() {
	// Build folddb binary if it doesn't exist
	folddbPath := "/tmp/folddb"
	if _, err := os.Stat(folddbPath); os.IsNotExist(err) {
		log.Println("Building folddb binary...")
		cmd := exec.Command("go", "build", "-o", folddbPath, "./cmd/folddb")
		cmd.Dir = findModuleRoot()
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Fatalf("Failed to build folddb: %v", err)
		}
		log.Println("folddb binary built successfully")
	}

	mux := http.NewServeMux()

	// Serve static files (strip the "static" prefix from embedded FS)
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		log.Fatal(err)
	}
	mux.Handle("/", http.FileServer(http.FS(sub)))

	// API: list streams
	mux.HandleFunc("/api/streams", handleStreams)

	// API: run query
	mux.HandleFunc("/api/run", handleRun)

	// API: stop query
	mux.HandleFunc("/api/stop", handleStop)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	addr := ":" + port
	log.Printf("Demo server starting on http://localhost%s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func findModuleRoot() string {
	// Walk up from the binary's expected location to find go.mod
	// In dev, we're likely running from the module root or cmd/demo
	dirs := []string{".", "../..", "/Users/kevin/dev/neql"}
	for _, d := range dirs {
		if _, err := os.Stat(d + "/go.mod"); err == nil {
			return d
		}
	}
	return "."
}

type streamInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Format      string `json:"format"`
	Rate        string `json:"rate"`
}

func handleStreams(w http.ResponseWriter, r *http.Request) {
	streams := []streamInfo{
		{Name: "ecommerce_orders", Description: "Debezium CDC events for an orders table", Format: "debezium", Rate: "~5 events/sec"},
		{Name: "api_metrics", Description: "Simulated API request logs", Format: "json", Rate: "~10 events/sec"},
		{Name: "clickstream", Description: "User click/interaction events", Format: "json", Rate: "~8 events/sec"},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(streams)
}

type runRequest struct {
	Stream string `json:"stream"`
	SQL    string `json:"sql"`
}

func handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	gen := NewGenerator(req.Stream)
	if gen == nil {
		http.Error(w, "unknown stream: "+req.Stream, http.StatusBadRequest)
		return
	}

	// Stop any existing query
	stopActive()

	// Set up pipe: generator -> folddb stdin
	pr, pw := io.Pipe()
	done := make(chan struct{})

	// Start generator
	go gen.Run(pw, done)

	// Build folddb command
	folddbPath := "/tmp/folddb"
	cmd := exec.Command(folddbPath, req.SQL)
	cmd.Stdin = pr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		http.Error(w, "failed to create stdout pipe: "+err.Error(), http.StatusInternalServerError)
		return
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		http.Error(w, "failed to create stderr pipe: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := cmd.Start(); err != nil {
		http.Error(w, "failed to start folddb: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Store active session
	mu.Lock()
	activeCmd = cmd
	activeDone = done
	activeW = pw
	mu.Unlock()

	// Log stderr in background
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			log.Printf("[folddb stderr] %s", scanner.Text())
		}
	}()

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Stream stdout as SSE
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	// Detect client disconnect
	ctx := r.Context()

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			stopActive()
			return
		default:
		}

		line := scanner.Text()
		fmt.Fprintf(w, "data: %s\n\n", line)
		flusher.Flush()
	}

	// Send done event
	fmt.Fprintf(w, "event: done\ndata: stream ended\n\n")
	flusher.Flush()

	// Clean up
	cmd.Wait()
}

func handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	stopActive()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "stopped"})
}

func stopActive() {
	mu.Lock()
	defer mu.Unlock()

	if activeDone != nil {
		select {
		case <-activeDone:
		default:
			close(activeDone)
		}
		activeDone = nil
	}

	if activeW != nil {
		activeW.Close()
		activeW = nil
	}

	if activeCmd != nil && activeCmd.Process != nil {
		activeCmd.Process.Kill()
		activeCmd.Wait()
		activeCmd = nil
	}
}
