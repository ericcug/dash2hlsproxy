package handler

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

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
	// 确保 MPD 是最新的。
	_, _, err := appCtx.MPDManager.GetMPD(channelCfg)
	if err != nil {
		appCtx.Logger.Error("Failed to get MPD for segment proxy", "channel_name", channelCfg.Name, "channel_id", channelCfg.ID, "error", err)
		http.Error(w, "Failed to process MPD for segment proxy", http.StatusInternalServerError)
		return
	}

	cachedEntry, exists := appCtx.MPDManager.GetCachedEntry(channelCfg.ID)
	if !exists {
		appCtx.Logger.Error("Cached entry not found for segment proxy", "channel_name", channelCfg.Name, "channel_id", channelCfg.ID)
		http.Error(w, "MPD data not available", http.StatusInternalServerError)
		return
	}

	cachedEntry.Mux.RLock()
	streamMap, streamOk := cachedEntry.PrecomputedData[streamType]
	if !streamOk {
		cachedEntry.Mux.RUnlock()
		appCtx.Logger.Warn("Precomputed data for streamType not found", "stream_type", streamType, "channel_name", channelCfg.Name, "channel_id", channelCfg.ID)
		http.NotFound(w, r)
		return
	}
	precomputed, qualityOk := streamMap[qualityOrLang]
	// 尽早解锁，因为我们已经复制了必要的数据或确定未找到
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
		http.Error(w, "Invalid MPD data (no SegmentTemplate in precomputed data)", http.StatusInternalServerError)
		return
	}

	var relativeSegmentPath string
	hlsSegmentIdentifier := ""

	hlsSegmentIdentifier = strings.TrimSuffix(segmentName, ".m4s")

	// 现在直接使用 hlsSegmentIdentifier
	mpdPathSegmentIdentifier := hlsSegmentIdentifier

	if mpdPathSegmentIdentifier == "init" {
		if segTemplate.Initialization == "" {
			appCtx.Logger.Error("HLS requested 'init.m4s' but MPD has no Initialization", "stream_type", streamType, "quality_or_lang", qualityOrLang)
			http.Error(w, "MPD has no Initialization segment", http.StatusNotFound)
			return
		}
		relativeSegmentPath = strings.ReplaceAll(segTemplate.Initialization, "$RepresentationID$", representationID)
	} else {
		if segTemplate.Media == "" {
			appCtx.Logger.Error("MPD has no Media template", "stream_type", streamType, "quality_or_lang", qualityOrLang)
			http.Error(w, "MPD has no Media segment template", http.StatusInternalServerError)
			return
		}
		tempPath := strings.ReplaceAll(segTemplate.Media, "$RepresentationID$", representationID)
		if strings.Contains(tempPath, "$Time$") {
			relativeSegmentPath = strings.ReplaceAll(tempPath, "$Time$", mpdPathSegmentIdentifier)
		} else if strings.Contains(tempPath, "$Number$") {
			relativeSegmentPath = strings.ReplaceAll(tempPath, "$Number$", mpdPathSegmentIdentifier)
		} else {
			appCtx.Logger.Warn("MPD Media template does not contain $Time$ or $Number$. Using template directly.", "stream_type", streamType, "quality_or_lang", qualityOrLang, "template", tempPath)
			// 或作为错误处理
			relativeSegmentPath = tempPath
		}
	}

	if relativeSegmentPath == "" {
		appCtx.Logger.Error("Failed to determine relativeSegmentPath for HLS segment", "segment_name", segmentName)
		http.Error(w, "Could not determine upstream segment path", http.StatusInternalServerError)
		return
	}

	upstreamURLStr := resolvedSegmentBaseURL + relativeSegmentPath

	httpClientReq, err := http.NewRequest("GET", upstreamURLStr, nil)
	if err != nil {
		appCtx.Logger.Error("Error creating request for upstream segment", "url", upstreamURLStr, "error", err)
		http.Error(w, "Failed to create upstream request", http.StatusInternalServerError)
		return
	}
	if channelCfg.UserAgent != "" {
		httpClientReq.Header.Set("User-Agent", channelCfg.UserAgent)
	}

	upstreamResp, err := appCtx.HTTPClient.Do(httpClientReq)
	if err != nil {
		appCtx.Logger.Error("Error fetching upstream segment", "url", upstreamURLStr, "error", err)
		http.Error(w, "Failed to fetch upstream segment", http.StatusBadGateway)
		return
	}
	defer upstreamResp.Body.Close()

	if upstreamResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(io.LimitReader(upstreamResp.Body, 1024))
		appCtx.Logger.Error("Upstream segment fetch failed", "url", upstreamURLStr, "status", upstreamResp.Status, "body", string(bodyBytes))
		http.Error(w, fmt.Sprintf("Failed to fetch upstream segment: %s", upstreamResp.Status), upstreamResp.StatusCode)
		return
	}

	// 对未转换的段进行标准代理
	for key, values := range upstreamResp.Header {
		for _, value := range values {
			if key == "Content-Type" || key == "Content-Length" || key == "ETag" || key == "Last-Modified" || key == "Cache-Control" || key == "Expires" || key == "Date" {
				w.Header().Add(key, value)
			}
		}
	}
	if w.Header().Get("Content-Type") == "" {
		appCtx.Logger.Warn("Upstream response missing Content-Type. Defaulting to application/octet-stream.", "url", upstreamURLStr)
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, upstreamResp.Body)
	if err != nil {
		appCtx.Logger.Error("Error copying segment data to client", "url", upstreamURLStr, "error", err)
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
