package handler

import (
	"fmt"
	"io"
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
	_, _, err := appCtx.MPDManager.GetMPD(channelCfg)
	if err != nil {
		appCtx.Logger.Error("Failed to get MPD for master playlist", "channel_name", channelCfg.Name, "channel_id", channelCfg.ID, "error", err)
		http.Error(w, "Failed to process MPD", http.StatusInternalServerError)
		return
	}

	cachedEntry, exists := appCtx.MPDManager.GetCachedEntry(channelCfg.ID)
	if !exists || cachedEntry.MasterPlaylist == "" {
		appCtx.Logger.Error("Master playlist not found in cache", "channel_name", channelCfg.Name, "channel_id", channelCfg.ID)
		// 回退或错误：重新生成或报错。目前，如果未预生成则报错。
		// 如果期望它始终存在，这表明预生成逻辑存在严重问题。
		http.Error(w, "Master playlist not available", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	// 锁定以读取 MasterPlaylist
	cachedEntry.Mux.RLock()
	fmt.Fprint(w, cachedEntry.MasterPlaylist)
	cachedEntry.Mux.RUnlock()
}

const NumLiveSegments = 5

func (appCtx *AppContext) mediaPlaylistHandler(w http.ResponseWriter, r *http.Request, channelCfg *config.ChannelConfig, streamType string, qualityOrLang string) {
	// 确保 MPD 数据和缓存的播放列表是最新的
	_, _, err := appCtx.MPDManager.GetMPD(channelCfg)
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

	mediaPlaylistKey := fmt.Sprintf("%s/%s", streamType, qualityOrLang)

	cachedEntry.Mux.RLock()
	playlistStr, ok := cachedEntry.MediaPlaylists[mediaPlaylistKey]
	cachedEntry.Mux.RUnlock()

	if !ok || playlistStr == "" {
		appCtx.Logger.Error("Media playlist not found in cache for key",
			"key", mediaPlaylistKey,
			"stream_type", streamType,
			"quality_or_lang", qualityOrLang,
			"channel_name", channelCfg.Name,
			"channel_id", channelCfg.ID)
		http.Error(w, "Requested media playlist not available", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	fmt.Fprint(w, playlistStr)
}

func (appCtx *AppContext) segmentProxyHandler(w http.ResponseWriter, r *http.Request, channelCfg *config.ChannelConfig, streamType string, qualityOrLang string, segmentName string) {
	// 确保 MPD 是最新的，这对于获取最新的 SegmentCache 至关重要
	_, _, err := appCtx.MPDManager.GetMPD(channelCfg)
	if err != nil {
		appCtx.Logger.Error("Failed to get MPD for segment proxy", "channel_name", channelCfg.Name, "channel_id", channelCfg.ID, "error", err)
		http.Error(w, "Failed to process MPD for segment proxy", http.StatusInternalServerError)
		return
	}

	cachedEntry, exists := appCtx.MPDManager.GetCachedEntry(channelCfg.ID)
	if !exists || cachedEntry.SegmentCache == nil {
		appCtx.Logger.Error("Cached entry or SegmentCache not found for segment proxy", "channel_name", channelCfg.Name, "channel_id", channelCfg.ID)
		http.Error(w, "MPD data not available", http.StatusInternalServerError)
		return
	}

	// 构造用于 HLS URL 的分段路径，它将作为缓存键
	segmentKey := fmt.Sprintf("/hls/%s/%s/%s/%s", channelCfg.ID, streamType, qualityOrLang, segmentName)

	// 1. 检查缓存
	cachedEntry.SegmentCache.RLock()
	cachedSeg, segmentExists := cachedEntry.SegmentCache.Segments[segmentKey]
	cachedEntry.SegmentCache.RUnlock()

	if segmentExists {
		appCtx.Logger.Debug("Serving segment from cache", "key", segmentKey)
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
		return
	}

	// 2. 缓存未命中：从上游代理并回填缓存
	appCtx.Logger.Debug("Segment not in cache, proxying from upstream", "key", segmentKey)
	w.Header().Set("X-Cache-Status", "MISS")

	// --- 从此处开始是原始代理逻辑的改编版 ---

	cachedEntry.Mux.RLock()
	streamMap, streamOk := cachedEntry.PrecomputedData[streamType]
	if !streamOk {
		cachedEntry.Mux.RUnlock()
		appCtx.Logger.Warn("Precomputed data for streamType not found", "stream_type", streamType, "channel_name", channelCfg.Name, "channel_id", channelCfg.ID)
		http.NotFound(w, r)
		return
	}
	precomputed, qualityOk := streamMap[qualityOrLang]
	cachedEntry.Mux.RUnlock()

	if !qualityOk {
		appCtx.Logger.Warn("Precomputed data for quality/lang not found", "stream_type", streamType, "quality_or_lang", qualityOrLang, "channel_name", channelCfg.Name, "channel_id", channelCfg.ID)
		http.NotFound(w, r)
		return
	}

	resolvedSegmentBaseURL := precomputed.ResolvedBaseURL
	segTemplate := precomputed.SegmentTemplate
	representationID := precomputed.RepresentationID

	if segTemplate == nil {
		appCtx.Logger.Error("SegmentTemplate missing in precomputed data", "stream_type", streamType, "quality_or_lang", qualityOrLang, "channel_name", channelCfg.Name, "channel_id", channelCfg.ID)
		http.Error(w, "Invalid MPD data (no SegmentTemplate)", http.StatusInternalServerError)
		return
	}

	var relativeSegmentPath string
	hlsSegmentIdentifier := strings.TrimSuffix(segmentName, ".m4s")
	mpdPathSegmentIdentifier := hlsSegmentIdentifier

	if mpdPathSegmentIdentifier == "init" {
		if segTemplate.Initialization == "" {
			http.Error(w, "MPD has no Initialization segment", http.StatusNotFound)
			return
		}
		relativeSegmentPath = strings.ReplaceAll(segTemplate.Initialization, "$RepresentationID$", representationID)
	} else {
		if segTemplate.Media == "" {
			http.Error(w, "MPD has no Media segment template", http.StatusInternalServerError)
			return
		}
		tempPath := strings.ReplaceAll(segTemplate.Media, "$RepresentationID$", representationID)
		if strings.Contains(tempPath, "$Time$") {
			relativeSegmentPath = strings.ReplaceAll(tempPath, "$Time$", mpdPathSegmentIdentifier)
		} else if strings.Contains(tempPath, "$Number$") {
			relativeSegmentPath = strings.ReplaceAll(tempPath, "$Number$", mpdPathSegmentIdentifier)
		} else {
			relativeSegmentPath = tempPath
		}
	}

	if relativeSegmentPath == "" {
		http.Error(w, "Could not determine upstream segment path", http.StatusInternalServerError)
		return
	}

	upstreamURLStr := resolvedSegmentBaseURL + relativeSegmentPath

	// --- 获取、代理和回填 ---
	const maxRetries = 3
	var upstreamResp *http.Response
	for i := 0; i < maxRetries; i++ {
		httpClientReq, reqErr := http.NewRequest("GET", upstreamURLStr, nil)
		if reqErr != nil {
			http.Error(w, "Failed to create upstream request", http.StatusInternalServerError)
			return
		}
		if channelCfg.UserAgent != "" {
			httpClientReq.Header.Set("User-Agent", channelCfg.UserAgent)
		}

		upstreamResp, err = appCtx.HTTPClient.Do(httpClientReq)
		if err == nil && upstreamResp.StatusCode == http.StatusOK {
			break // 成功
		}
		if upstreamResp != nil {
			upstreamResp.Body.Close()
		}
		if i == maxRetries-1 {
			http.Error(w, "Failed to fetch upstream segment", http.StatusBadGateway)
			return
		}
	}
	defer upstreamResp.Body.Close()

	// 读取主体以进行缓存和代理
	bodyBytes, readErr := io.ReadAll(upstreamResp.Body)
	if readErr != nil {
		http.Error(w, "Failed to read upstream response", http.StatusInternalServerError)
		return
	}

	// 回填缓存
	contentType := upstreamResp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	cachedEntry.SegmentCache.Lock()
	cachedEntry.SegmentCache.Segments[segmentKey] = &mpd_manager.CachedSegment{
		Data:        bodyBytes,
		ContentType: contentType,
		FetchedAt:   time.Now(),
	}
	cachedEntry.SegmentCache.Unlock()
	appCtx.Logger.Debug("Segment cached after miss", "key", segmentKey)

	// 将响应代理回客户端
	for key, values := range upstreamResp.Header {
		for _, value := range values {
			// 仅代理必要的头信息
			if key == "Content-Length" || key == "ETag" || key == "Last-Modified" || key == "Cache-Control" || key == "Expires" || key == "Date" {
				w.Header().Add(key, value)
			}
		}
	}
	w.Header().Set("Content-Type", contentType)
	// Content-Length 将由 http.ResponseWriter 自动设置

	w.WriteHeader(http.StatusOK)
	_, copyErr := w.Write(bodyBytes)
	if copyErr != nil {
		if strings.Contains(copyErr.Error(), "broken pipe") {
			appCtx.Logger.Info("Client disconnected while proxying segment", "key", segmentKey, "error", copyErr)
		} else {
			appCtx.Logger.Error("Error copying segment data to client", "key", segmentKey, "error", copyErr)
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
