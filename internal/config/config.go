package config

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
)

// ChannelConfig 保存单个频道的配置。
type ChannelConfig struct {
	Name     string `json:"Name"`
	ID       string `json:"Id"`
	Manifest string `json:"Manifest"`
	// 密钥的十六进制字符串
	Key       string `json:"Key"`
	UserAgent string `json:"UserAgent"`

	// ParsedKey 存储原始密钥字节。
	// 这是在加载配置后填充的。
	ParsedKey []byte `json:"-"`
}

// AppConfig 保存整个应用程序的配置，包括所有频道。
type AppConfig struct {
	Name     string          `json:"Name"`
	ID       string          `json:"Id"`
	Channels []ChannelConfig `json:"Channels"`

	// ChannelMap 提供按频道 ID 快速查找频道的功能。
	// 这是在加载配置后填充的。
	ChannelMap map[string]*ChannelConfig `json:"-"`
}

// LoadConfig 从给定路径读取配置文件，
// 解析它，并预处理一些字段（如密钥和频道映射）。
func LoadConfig(filePath string) (*AppConfig, error) {
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
		// 获取切片中频道的指针
		ch := &cfg.Channels[i]

		if ch.Key != "" {
			keyBytes, err := hex.DecodeString(ch.Key)
			if err != nil {
				return nil, fmt.Errorf("failed to decode key hex '%s' for channel %s: %w", ch.Key, ch.ID, err)
			}
			// AES-128 密钥是 16 字节。如有必要，请添加验证。
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
