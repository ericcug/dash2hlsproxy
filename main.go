package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time" // Added for HTTPClient configuration

	"dash2hlsproxy/internal/config" // Assuming Go module name is dash2hlsproxy
	"dash2hlsproxy/internal/handler"
	"dash2hlsproxy/internal/mpd_manager"
)

func main() {
	// Define command-line flags
	defaultChannelsFile := "channels.json"
	defaultListenAddr := ":8080"

	channelsFile := flag.String("c", defaultChannelsFile, "Path to the configuration file (can be overridden by CHANNELS_JSON env var)")
	listenAddrFlag := flag.String("l", defaultListenAddr, "Address and port to listen on (e.g., :8080 or 127.0.0.1:8080; can be overridden by LISTEN_ADDR env var)")
	flag.Parse()

	// Determine listen address: environment variable > command-line flag > default
	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = *listenAddrFlag
	}

	// Attempt to load the configuration
	// config.LoadConfig will use D2H_CHANNELS_JSON_PATH env var if set, otherwise *configFile
	cfg, err := config.LoadConfig(*channelsFile)
	if err != nil {
		log.Fatalf("Error loading configuration from %s: %v", *channelsFile, err)
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

	mpdMngr := mpd_manager.NewMPDManager(cfg)

	// Create a shared HTTP client with optimized transport
	sharedHTTPClient := &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10, // Adjust based on expected number of upstream servers
			IdleConnTimeout:     90 * time.Second,
			// Other settings like TLSHandshakeTimeout, ExpectContinueTimeout can be added if needed
		},
	}

	appCtx := &handler.AppContext{
		Config:     cfg,
		MPDManager: mpdMngr,
		HTTPClient: sharedHTTPClient, // Initialize the shared client
	}

	// Initialize MPD cache entries (or do it lazily in GetMPD)
	// For now, GetMPD will create entries on demand via MPDManager.

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
