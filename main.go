package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"dash2hlsproxy/internal/config"
	"dash2hlsproxy/internal/handler"
	"dash2hlsproxy/internal/mpd_manager"
)

func main() {
	// 定义命令行标志
	channelsFileFlag := flag.String("c", "channels.json", "Path to the configuration file (env: CHANNELS_JSON_PATH)")
	listenAddrFlag := flag.String("L", ":8080", "Address and port to listen on (env: LISTEN_ADDR)")
	logLevelFlag := flag.String("l", "info", "Set the log level (debug, info, warn, error) (env: LOG_LEVEL)")
	flag.Parse()

	// --- 统一配置加载 ---
	// 优先级: 环境变量 > 命令行标志 > 默认值
	channelsFile := getStringParameter("CHANNELS_JSON_PATH", *channelsFileFlag, "channels.json")
	listenAddr := getStringParameter("LISTEN_ADDR", *listenAddrFlag, ":8080")
	logLevel := getStringParameter("LOG_LEVEL", *logLevelFlag, "info")

	// --- 设置日志记录器 ---
	var plogLevel slog.Level
	switch strings.ToLower(logLevel) {
	case "debug":
		plogLevel = slog.LevelDebug
	case "info":
		plogLevel = slog.LevelInfo
	case "warn":
		plogLevel = slog.LevelWarn
	case "error":
		plogLevel = slog.LevelError
	default:
		plogLevel = slog.LevelInfo
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: plogLevel,
	})).With("service", "dash2hlsproxy")
	// 设置应用程序的默认日志记录器
	slog.SetDefault(logger)

	// 尝试加载配置
	cfg, err := config.LoadConfig(channelsFile)
	if err != nil {
		logger.Error("Error loading configuration", "path", channelsFile, "error", err)
		os.Exit(1)
	}

	// 记录加载配置中的一些信息
	logger.Info("Successfully loaded configuration", "name", cfg.Name, "id", cfg.ID)
	logger.Info("Channel details", "count", len(cfg.Channels))

	for i, channel := range cfg.Channels {
		hasKey := channel.Key != ""
		logger.Info("Channel found",
			"index", i+1,
			"name", channel.Name,
			"id", channel.ID,
			"manifest_url", channel.Manifest,
			"user_agent", channel.UserAgent,
			"has_key", hasKey,
		)
	}

	// --- 设置 HTTP 服务器 ---
	logger.Info("Configuration loaded. Setting up HTTP server...")

	mpdMngr := mpd_manager.NewMPDManager(cfg, logger.With("module", "mpd_manager"))

	// 创建一个带有优化传输的共享 HTTP 客户端
	sharedHTTPClient := &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns: 100,
			// 根据预期的上游服务器数量进行调整
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
			// 如果需要，可以添加其他设置，如 TLSHandshakeTimeout、ExpectContinueTimeout
		},
	}

	appCtx := &handler.AppContext{
		Config:     cfg,
		MPDManager: mpdMngr,
		// 初始化共享客户端
		HTTPClient: sharedHTTPClient,
		Logger:     logger.With("module", "handler"),
	}

	// 初始化 MPD 缓存条目（或在 GetMPD 中延迟执行）
	// 目前，GetMPD 将通过 MPDManager 按需创建条目。

	router := handler.SetupRouter(appCtx)

	server := &http.Server{
		// 使用确定的 listenAddr
		Addr:    listenAddr,
		Handler: router,
	}

	logger.Info("Starting server", "address", server.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("Could not start server", "address", server.Addr, "error", err)
		os.Exit(1)
	}
	logger.Info("Server stopped")
}

// getStringParameter 从环境变量、命令行标志或默认值中获取字符串参数。
// 优先级顺序：环境变量 > 命令行标志 > 默认值。
func getStringParameter(envName string, flagValue string, defaultValue string) string {
	// 1. 检查环境变量
	if value, exists := os.LookupEnv(envName); exists {
		return value
	}
	// 2. 检查是否设置了命令行标志（与默认值不同）
	// 注意：这假设 flag 库的默认值与此处的 defaultValue 匹配。
	// 一个更健壮的方法是检查 flag 是否被用户显式设置。
	// 但对于这个应用来说，这种方法足够了。
	if flagValue != defaultValue {
		return flagValue
	}
	// 3. 返回默认值
	return defaultValue
}
