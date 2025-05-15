package config

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
)

// ChannelConfig holds the configuration for a single channel.
type ChannelConfig struct {
	Name      string `json:"Name"`
	ID        string `json:"Id"`
	Manifest  string `json:"Manifest"`
	Key       string `json:"Key"` // Key hex string
	UserAgent string `json:"UserAgent"`

	// ParsedKey stores raw key bytes.
	// This is populated after loading the config.
	ParsedKey []byte `json:"-"`
}

// AppConfig holds the overall application configuration, including all channels.
type AppConfig struct {
	Name     string          `json:"Name"`
	ID       string          `json:"Id"`
	Channels []ChannelConfig `json:"Channels"`

	// ChannelMap provides quick lookup of channels by their ID.
	// This is populated after loading the config.
	ChannelMap map[string]*ChannelConfig `json:"-"`
}

// LoadConfig reads the configuration file from the given path,
// parses it, and preprocesses some fields (like keys and channel map).
// The filePath can be overridden by the D2H_CHANNELS_JSON_PATH environment variable.
func LoadConfig(defaultFilePath string) (*AppConfig, error) {
	filePath := os.Getenv("CHANNELS_JSON")
	if filePath == "" {
		filePath = defaultFilePath
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read channels file %s: %w", filePath, err)
	}

	var cfg AppConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal channels JSON from %s: %w", filePath, err)
	}

	cfg.ChannelMap = make(map[string]*ChannelConfig)
	for i := range cfg.Channels {
		ch := &cfg.Channels[i] // Get a pointer to the channel in the slice

		if ch.Key != "" {
			keyBytes, err := hex.DecodeString(ch.Key)
			if err != nil {
				return nil, fmt.Errorf("failed to decode key hex '%s' for channel %s: %w", ch.Key, ch.ID, err)
			}
			// AES-128 keys are 16 bytes. Add validation if necessary.
			// if len(keyBytes) != 16 {
			// 	return nil, fmt.Errorf("decoded key for channel %s is not 16 bytes long", ch.ID)
			// }
			ch.ParsedKey = keyBytes
		}

		if _, exists := cfg.ChannelMap[ch.ID]; exists {
			return nil, fmt.Errorf("duplicate channel ID '%s' found in configuration", ch.ID)
		}
		cfg.ChannelMap[ch.ID] = ch
	}

	return &cfg, nil
}
