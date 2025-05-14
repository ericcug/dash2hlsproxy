package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"dash2hlsproxy/internal/config" // Assuming Go module name is dash2hlsproxy
	"dash2hlsproxy/internal/handler"
)

func main() {
	// Define command-line flags
	defaultConfigFile := "channels.json"
	defaultListenAddr := ":8080"

	configFile := flag.String("config", defaultConfigFile, "Path to the configuration file (can be overridden by D2H_CHANNELS_JSON_PATH env var)")
	listenAddrFlag := flag.String("listen", defaultListenAddr, "Address and port to listen on (e.g., :8080 or 127.0.0.1:8080; can be overridden by D2H_LISTEN_ADDR env var)")
	flag.Parse()

	// Determine listen address: environment variable > command-line flag > default
	listenAddr := os.Getenv("D2H_LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = *listenAddrFlag
	}

	// Attempt to load the configuration
	// config.LoadConfig will use D2H_CHANNELS_JSON_PATH env var if set, otherwise *configFile
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Error loading configuration from %s: %v", *configFile, err)
		os.Exit(1)
	}

	// Log some information from the loaded configuration
	log.Printf("Successfully loaded configuration: %s (ID: %s)", cfg.Name, cfg.ID)
	log.Printf("Found %d channels:", len(cfg.Channels))

	for i, channel := range cfg.Channels {
		log.Printf("  Channel %d:", i+1)
		log.Printf("    Name: %s", channel.Name)
		log.Printf("    ID: %s", channel.ID)
		log.Printf("    Manifest URL: %s", channel.Manifest)
		log.Printf("    UserAgent: %s", channel.UserAgent)
		if channel.Key != "" {
			log.Printf("    Key defined: Yes (Length: %d bytes after parsing)", len(channel.ParsedKey))
		} else {
			log.Printf("    Key defined: No")
		}
		// Example of accessing a channel via the map
		if mappedChannel, ok := cfg.ChannelMap[channel.ID]; ok {
			log.Printf("    (Verified lookup for channel ID '%s' in ChannelMap)", mappedChannel.ID)
		}
	}

	// --- Setup HTTP server ---
	log.Println("Configuration loaded. Setting up HTTP server...")

	appCtx := &handler.AppContext{
		Config:   cfg,
		MPDCache: make(map[string]*handler.CachedMPD),
		// CacheLock is zero-value initialized (sync.RWMutex)
	}

	// Initialize MPD cache entries (or do it lazily in GetMPD)
	// For now, GetMPD will create entries on demand.

	router := handler.SetupRouter(appCtx)

	server := &http.Server{
		Addr:    listenAddr, // Use the determined listenAddr
		Handler: router,
	}

	log.Printf("Starting server on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Could not listen on %s: %v\n", server.Addr, err)
	}
	log.Println("Server stopped")
}
