package handler

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"dash2hlsproxy/internal/config"
	"dash2hlsproxy/internal/mpd_manager"
)

// AppContext 保存处理程序的依赖项，例如应用程序配置。
type AppContext struct {
	Config     *config.AppConfig
	MPDManager *mpd_manager.MPDManager
	// 添加共享 HTTP 客户端
	HTTPClient *http.Client
	Logger     *slog.Logger
}

func SetupRouter(appCtx *AppContext) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/hls/", appCtx.hlsRouter)
	return mux
}

func (appCtx *AppContext) hlsRouter(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/hls/")
	path = strings.TrimSuffix(path, "/")
	parts := strings.Split(path, "/")

	if len(parts) == 0 || parts[0] == "" {
		appCtx.Logger.Error("Channel ID is missing in HLS path", "path", r.URL.Path)
		http.Error(w, "Channel ID is missing", http.StatusBadRequest)
		return
	}
	channelID := parts[0]

	channelCfg, ok := appCtx.Config.ChannelMap[channelID]
	if !ok {
		appCtx.Logger.Error("Channel ID not found", "channel_id", channelID, "path", r.URL.Path)
		http.NotFound(w, r)
		return
	}

	numSubParts := len(parts) - 1

	switch numSubParts {
	case 0:
		appCtx.masterPlaylistHandler(w, r, channelCfg)
	case 1:
		if parts[1] == "key" {
			appCtx.keyServerHandler(w, r, channelCfg)
		} else {
			appCtx.Logger.Warn("Invalid HLS path structure (2 parts)", "path", path)
			http.Error(w, "Invalid HLS path structure", http.StatusBadRequest)
		}
	case 3:
		streamType := parts[1]
		qualityOrLang := parts[2]
		fileName := parts[3]

		if strings.HasSuffix(fileName, ".m3u8") {
			appCtx.mediaPlaylistHandler(w, r, channelCfg, streamType, qualityOrLang)
		} else {
			appCtx.segmentProxyHandler(w, r, channelCfg, streamType, qualityOrLang, fileName)
		}
	default:
		appCtx.Logger.Warn("Unhandled HLS path structure", "path", path, "parts", len(parts))
		http.Error(w, "Invalid HLS path structure or unsupported endpoint", http.StatusBadRequest)
	}
}

func (appCtx *AppContext) masterPlaylistHandler(w http.ResponseWriter, r *http.Request, channelCfg *config.ChannelConfig) {
	// 确保 MPD 数据和缓存的播放列表是最新的
	_, _, err := appCtx.MPDManager.GetMPD(r.Context(), channelCfg)
	if err != nil {
		appCtx.Logger.Error("Failed to get MPD for master playlist", "channel_name", channelCfg.Name, "channel_id", channelCfg.ID, "error", err)
		http.Error(w, "Failed to process MPD", http.StatusInternalServerError)
		return
	}

	cachedEntry, exists := appCtx.MPDManager.GetCachedEntry(channelCfg.ID)
	if !exists || cachedEntry.MasterPlaylist == "" {
		appCtx.Logger.Error("Master playlist not found in cache", "channel_name", channelCfg.Name, "channel_id", channelCfg.ID)
		http.Error(w, "Master playlist not available", http.StatusInternalServerError)
		return
	}

	// --- 新增逻辑：等待所有媒体播放列表的分片都准备就绪 ---
	var requiredSegments []string
	cachedEntry.Mux.RLock()
	for key, playlistStr := range cachedEntry.MediaPlaylists {
		for _, line := range strings.Split(playlistStr, "\n") {
			if strings.HasPrefix(line, "/hls/") {
				requiredSegments = append(requiredSegments, line)
			}
		}
		appCtx.Logger.Debug("Collected segments from media playlist", "key", key)
	}
	cachedEntry.Mux.RUnlock()

	if len(requiredSegments) > 0 {
		// 设置一个足够长的超时时间，以等待所有分片缓存
		timeout := 20 * time.Second
		err := appCtx.MPDManager.WaitForSegments(r.Context(), channelCfg.ID, requiredSegments, timeout)
		if err != nil {
			appCtx.Logger.Error("Error waiting for all segments to be cached for master playlist",
				"channel_id", channelCfg.ID,
				"error", err)
			http.Error(w, "Failed to cache all necessary segments in time", http.StatusServiceUnavailable)
			return
		}
		appCtx.Logger.Info("All required segments are cached. Proceeding to serve master playlist.", "channel_id", channelCfg.ID)
	}
	// --- 结束新增逻辑 ---

	cachedEntry.Mux.Lock()
	cachedEntry.LastAccessedAt = time.Now()
	cachedEntry.Mux.Unlock()

	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	cachedEntry.Mux.RLock()
	playlistContent := cachedEntry.MasterPlaylist
	cachedEntry.Mux.RUnlock()

	fmt.Fprint(w, playlistContent)

	appCtx.Logger.Info("Successfully served master playlist",
		"channel_name", channelCfg.Name,
		"channel_id", channelCfg.ID,
		"playlist_length", len(playlistContent))
}

func (appCtx *AppContext) mediaPlaylistHandler(w http.ResponseWriter, r *http.Request, channelCfg *config.ChannelConfig, streamType string, qualityOrLang string) {
	// 确保 MPD 数据和缓存的播放列表是最新的
	_, _, err := appCtx.MPDManager.GetMPD(r.Context(), channelCfg)
	if err != nil {
		appCtx.Logger.Error("Failed to get MPD for media playlist", "channel_name", channelCfg.Name, "channel_id", channelCfg.ID, "error", err)
		http.Error(w, "Failed to process MPD", http.StatusInternalServerError)
		return
	}

	cachedEntry, exists := appCtx.MPDManager.GetCachedEntry(channelCfg.ID)
	if !exists {
		appCtx.Logger.Error("Cached entry not found for media playlist", "channel_name", channelCfg.Name, "channel_id", channelCfg.ID)
		http.Error(w, "Media playlist data not available", http.StatusInternalServerError)
		return
	}

	cachedEntry.Mux.Lock()
	cachedEntry.LastAccessedAt = time.Now()
	cachedEntry.Mux.Unlock()

	mediaPlaylistKey := fmt.Sprintf("%s/%s", streamType, qualityOrLang)

	cachedEntry.Mux.RLock()
	playlistStr, ok := cachedEntry.MediaPlaylists[mediaPlaylistKey]
	cachedEntry.Mux.RUnlock()

	if !ok || playlistStr == "" {
		appCtx.Logger.Error("Media playlist not found in cache for key", "key", mediaPlaylistKey)
		http.Error(w, "Requested media playlist not available", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	fmt.Fprint(w, playlistStr)
}

func (appCtx *AppContext) segmentProxyHandler(w http.ResponseWriter, r *http.Request, channelCfg *config.ChannelConfig, streamType string, qualityOrLang string, segmentName string) {
	cachedEntry, exists := appCtx.MPDManager.GetCachedEntry(channelCfg.ID)
	if !exists || cachedEntry.SegmentCache == nil {
		appCtx.Logger.Error("Cached entry or SegmentCache not found for segment proxy", "channel_id", channelCfg.ID)
		http.Error(w, "Channel data not available", http.StatusInternalServerError)
		return
	}

	segmentKey := fmt.Sprintf("/hls/%s/%s/%s/%s", channelCfg.ID, streamType, qualityOrLang, segmentName)

	// 在新设计中，如果分片不在缓存中，这是一个严重错误，因为 mediaPlaylistHandler 应该已经确保了它的存在。
	cachedSeg, segmentExists := cachedEntry.SegmentCache.Get(segmentKey)
	if !segmentExists {
		appCtx.Logger.Error("FATAL: Segment not found in cache. This should not happen in the new design.",
			"key", segmentKey,
			"channel_id", channelCfg.ID)
		w.Header().Set("X-Cache-Status", "MISS")
		http.Error(w, "Segment not available", http.StatusInternalServerError)
		return
	}

	appCtx.Logger.Debug("Serving segment from cache", "key", segmentKey)

	// For init segments, set a long cache time as they are static.
	if strings.Contains(segmentName, "init.m4s") {
		w.Header().Set("Cache-Control", "public, max-age=86400") // Cache for 24 hours
	}

	w.Header().Set("Content-Type", cachedSeg.ContentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(cachedSeg.Data)))
	w.Header().Set("X-Cache-Status", "HIT")
	w.WriteHeader(http.StatusOK)
	_, err := w.Write(cachedSeg.Data)
	if err != nil {
		if strings.Contains(err.Error(), "broken pipe") {
			appCtx.Logger.Info("Client disconnected while serving cached segment", "key", segmentKey, "error", err)
		} else {
			appCtx.Logger.Error("Error writing cached segment to client", "key", segmentKey, "error", err)
		}
	}
}

// keyServerHandler 为特定频道提供解密密钥。
func (appCtx *AppContext) keyServerHandler(w http.ResponseWriter, r *http.Request, channelCfg *config.ChannelConfig) {
	if len(channelCfg.ParsedKey) == 0 {
		appCtx.Logger.Warn("No key configured for channel", "channel_name", channelCfg.Name, "channel_id", channelCfg.ID)
		http.NotFound(w, r)
		return
	}

	appCtx.Logger.Info("Serving key for channel", "channel_name", channelCfg.Name, "channel_id", channelCfg.ID, "key_length", len(channelCfg.ParsedKey))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(channelCfg.ParsedKey)))
	_, err := w.Write(channelCfg.ParsedKey)
	if err != nil {
		appCtx.Logger.Error("Error writing key for channel", "channel_name", channelCfg.Name, "channel_id", channelCfg.ID, "error", err)
	}
}
