package handler

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"dash2hlsproxy/internal/config"
	"dash2hlsproxy/internal/mpd_manager"
)

// AppContext holds dependencies for handlers, like the app configuration.
type AppContext struct {
	Config     *config.AppConfig
	MPDManager *mpd_manager.MPDManager
	HTTPClient *http.Client // Added shared HTTP client
	// MPDCache and CacheLock are now part of MPDManager
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
		log.Printf("Error: Channel ID is missing in HLS path: %s", r.URL.Path)
		http.Error(w, "Channel ID is missing", http.StatusBadRequest)
		return
	}
	channelID := parts[0]

	channelCfg, ok := appCtx.Config.ChannelMap[channelID]
	if !ok {
		log.Printf("Error: Channel ID '%s' not found. Path: %s", channelID, r.URL.Path)
		http.NotFound(w, r)
		return
	}

	// Accessing LastAccessedAt is now handled within MPDManager.GetMPD or a dedicated method if needed for hlsRouter specifically.
	// For now, GetMPD updates it. If direct update is needed here without fetching, MPDManager would need a new method.
	// Let's assume GetMPD's update is sufficient for now.
	// If not, we might need: appCtx.MPDManager.UpdateAccessTime(channelID)

	// log.Printf("HLS request for channel: %s (%s), Path: /hls/%s", channelCfg.Name, channelID, path) // Removed success log
	numSubParts := len(parts) - 1

	switch numSubParts {
	case 0:
		appCtx.masterPlaylistHandler(w, r, channelCfg)
	case 1:
		if parts[1] == "key" {
			appCtx.keyServerHandler(w, r, channelCfg)
		} else {
			log.Printf("Invalid HLS path structure (2 parts): /hls/%s", path)
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
		log.Printf("Unhandled HLS path structure: /hls/%s (parts: %d)", path, len(parts))
		http.Error(w, "Invalid HLS path structure or unsupported endpoint", http.StatusBadRequest)
	}
}

func (appCtx *AppContext) masterPlaylistHandler(w http.ResponseWriter, r *http.Request, channelCfg *config.ChannelConfig) {
	// log.Printf("Serving MASTER playlist for channel %s (%s)", channelCfg.Name, channelCfg.ID) // Removed success log

	// Ensure MPD data and cached playlists are up-to-date
	_, _, err := appCtx.MPDManager.GetMPD(channelCfg)
	if err != nil {
		log.Printf("Error ensuring MPD is up-to-date for master playlist %s (%s): %v", channelCfg.Name, channelCfg.ID, err)
		http.Error(w, "Failed to process MPD", http.StatusInternalServerError)
		return
	}

	cachedEntry, exists := appCtx.MPDManager.GetCachedEntry(channelCfg.ID)
	if !exists || cachedEntry.MasterPlaylist == "" {
		log.Printf("Error: Master playlist not found in cache for channel %s (%s)", channelCfg.Name, channelCfg.ID)
		// Fallback or error: Regenerate or error out. For now, error out if not pre-generated.
		// This indicates an issue with the pre-generation logic if it's expected to always be there.
		http.Error(w, "Master playlist not available", http.StatusInternalServerError)
		return
	}

	// log.Printf("Successfully retrieved cached master playlist for %s (%s)", channelCfg.Name, channelCfg.ID) // Removed success log

	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	cachedEntry.Mux.RLock() // Lock for reading MasterPlaylist
	fmt.Fprint(w, cachedEntry.MasterPlaylist)
	cachedEntry.Mux.RUnlock()
}

const NumLiveSegments = 5

func (appCtx *AppContext) mediaPlaylistHandler(w http.ResponseWriter, r *http.Request, channelCfg *config.ChannelConfig, streamType string, qualityOrLang string) {
	// log.Printf("MediaPlaylistHandler: START - Request for channel %s (%s): type=%s, quality/lang=%s, URL=%s", // Removed success log
	//	channelCfg.Name, channelCfg.ID, streamType, qualityOrLang, r.URL.String())
	// defer log.Printf("MediaPlaylistHandler: END - Request for channel %s (%s): type=%s, quality/lang=%s, URL=%s", // Removed success log
	//	channelCfg.Name, channelCfg.ID, streamType, qualityOrLang, r.URL.String())

	// Ensure MPD data and cached playlists are up-to-date
	_, _, err := appCtx.MPDManager.GetMPD(channelCfg)
	if err != nil {
		log.Printf("Error ensuring MPD is up-to-date for media playlist %s (%s): %v", channelCfg.Name, channelCfg.ID, err)
		http.Error(w, "Failed to process MPD", http.StatusInternalServerError)
		return
	}

	cachedEntry, exists := appCtx.MPDManager.GetCachedEntry(channelCfg.ID)
	if !exists {
		log.Printf("Error: Cached entry not found for channel %s (%s) when requesting media playlist", channelCfg.Name, channelCfg.ID)
		http.Error(w, "Media playlist data not available", http.StatusInternalServerError)
		return
	}

	// Construct the key used for storing this specific media playlist
	// This key must match how it's stored in mpd_manager's generateMediaPlaylists
	// Assuming the placeholder generateMediaPlaylists uses "media.m3u8" or similar simple key for now.
	// A more robust key would be e.g., fmt.Sprintf("%s/%s", streamType, qualityOrLang)
	// For the current placeholder in mpd_manager.go, it's "media.m3u8"
	// This needs to be aligned with the actual implementation of generateMediaPlaylists.
	// Let's assume a generic key for now, or that the map only contains one relevant playlist if simple.
	// For a multi-representation setup, the key must be specific.
	// The placeholder `generateMediaPlaylists` uses "media.m3u8" as a key.
	// This is too simplistic for multiple qualities/languages.
	// Let's assume the key is `fmt.Sprintf("%s/%s/playlist.m3u8", streamType, qualityOrLang)` or similar,
	// or more simply, the placeholder `generateMediaPlaylists` would need to be smarter or the handler
	// would iterate if keys are not exact.
	// Given the current mpd_manager placeholder returns `playlists["media.m3u8"] = ...`, this will only work for one media playlist.
	// This part highlights that the playlist generation and keying strategy needs to be robust.

	// For now, let's assume the key is derived from streamType and qualityOrLang,
	// and the `generateMediaPlaylists` in `mpd_manager` populates the map accordingly.
	// The key should consistently represent the specific media playlist.
	mediaPlaylistKey := fmt.Sprintf("%s/%s", streamType, qualityOrLang)

	cachedEntry.Mux.RLock()
	playlistStr, ok := cachedEntry.MediaPlaylists[mediaPlaylistKey]
	cachedEntry.Mux.RUnlock()

	if !ok || playlistStr == "" {
		log.Printf("Error: Media playlist for key '%s' (type=%s, quality/lang=%s) not found in cache for channel %s (%s)",
			mediaPlaylistKey, streamType, qualityOrLang, channelCfg.Name, channelCfg.ID)
		// This could happen if the specific quality/language was not generated or if the key is wrong.
		http.Error(w, "Requested media playlist not available", http.StatusNotFound)
		return
	}

	// log.Printf("Successfully retrieved cached media playlist for key '%s', channel %s (%s)", mediaPlaylistKey, channelCfg.Name, channelCfg.ID) // Removed success log

	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	fmt.Fprint(w, playlistStr)
}

func (appCtx *AppContext) segmentProxyHandler(w http.ResponseWriter, r *http.Request, channelCfg *config.ChannelConfig, streamType string, qualityOrLang string, segmentName string) {
	// log.Printf("SegmentProxyHandler: START - Request for channel %s (%s): type=%s, quality/lang=%s, segment=%s, URL=%s", // Removed success log
	//	channelCfg.Name, channelCfg.ID, streamType, qualityOrLang, segmentName, r.URL.String())
	// defer log.Printf("SegmentProxyHandler: END - Request for channel %s (%s): type=%s, quality/lang=%s, segment=%s, URL=%s", // Removed success log
	//	channelCfg.Name, channelCfg.ID, streamType, qualityOrLang, segmentName, r.URL.String())

	// Ensure MPD is up-to-date. GetMPD also handles precomputation and playlist generation.
	// We don't directly use the returned mpdData here for segment lookup anymore,
	// but calling it ensures the cachedEntry used below is fresh.
	_, _, err := appCtx.MPDManager.GetMPD(channelCfg)
	if err != nil {
		log.Printf("SegmentProxy: Error ensuring MPD is up-to-date for channel %s (%s): %v", channelCfg.Name, channelCfg.ID, err)
		http.Error(w, "Failed to process MPD for segment proxy", http.StatusInternalServerError)
		return
	}

	cachedEntry, exists := appCtx.MPDManager.GetCachedEntry(channelCfg.ID)
	if !exists {
		log.Printf("SegmentProxy: Cached entry not found for channel %s (%s) after GetMPD call. This should not happen.", channelCfg.Name, channelCfg.ID)
		http.Error(w, "MPD data not available", http.StatusInternalServerError)
		return
	}

	cachedEntry.Mux.RLock() // Lock for reading PrecomputedData
	streamMap, streamOk := cachedEntry.PrecomputedData[streamType]
	if !streamOk {
		cachedEntry.Mux.RUnlock()
		log.Printf("SegmentProxy: Precomputed data for streamType '%s' not found. Channel %s (%s)", streamType, channelCfg.Name, channelCfg.ID)
		http.NotFound(w, r)
		return
	}

	precomputed, qualityOk := streamMap[qualityOrLang]
	if !qualityOk {
		cachedEntry.Mux.RUnlock()
		log.Printf("SegmentProxy: Precomputed data for streamType '%s', quality/lang '%s' not found. Channel %s (%s)", streamType, qualityOrLang, channelCfg.Name, channelCfg.ID)
		http.NotFound(w, r)
		return
	}
	// Make copies of necessary fields from precomputed data while under lock
	resolvedSegmentBaseURL := precomputed.ResolvedBaseURL
	segTemplate := precomputed.SegmentTemplate // This is a pointer, but its content is static per MPD version
	representationID := precomputed.RepresentationID
	// targetAS and targetRep could also be copied if needed for other logic, but not for URL construction here.
	cachedEntry.Mux.RUnlock()

	if segTemplate == nil { // Should have been caught during precomputation, but double check
		log.Printf("SegmentProxy: SegmentTemplate missing in precomputed data for %s/%s in channel %s (%s)",
			streamType, qualityOrLang, channelCfg.Name, channelCfg.ID)
		http.Error(w, "Invalid MPD data (no SegmentTemplate in precomputed data)", http.StatusInternalServerError)
		return
	}

	var relativeSegmentPath string
	hlsSegmentIdentifier := strings.TrimSuffix(segmentName, ".m4s")

	if hlsSegmentIdentifier == "init" {
		if segTemplate.Initialization == "" {
			log.Printf("SegmentProxyHandler: HLS requested 'init.m4s' but MPD (precomputed) has no Initialization segment for %s/%s, channel %s (%s)",
				streamType, qualityOrLang, channelCfg.Name, channelCfg.ID)
			http.Error(w, "MPD has no Initialization segment", http.StatusNotFound)
			return
		}
		relativeSegmentPath = strings.ReplaceAll(segTemplate.Initialization, "$RepresentationID$", representationID)
	} else {
		if segTemplate.Media == "" {
			log.Printf("SegmentProxyHandler: HLS requested media segment '%s' but MPD (precomputed) has no Media template for %s/%s, channel %s (%s)",
				segmentName, streamType, qualityOrLang, channelCfg.Name, channelCfg.ID)
			http.Error(w, "MPD has no Media segment template", http.StatusNotFound)
			return
		}
		tempPath := strings.ReplaceAll(segTemplate.Media, "$RepresentationID$", representationID)

		if strings.Contains(tempPath, "$Time$") {
			relativeSegmentPath = strings.ReplaceAll(tempPath, "$Time$", hlsSegmentIdentifier)
		} else if strings.Contains(tempPath, "$Number$") {
			relativeSegmentPath = strings.ReplaceAll(tempPath, "$Number$", hlsSegmentIdentifier)
		} else {
			log.Printf("SegmentProxyHandler: Warning - MPD Media template '%s' (precomputed) for %s/%s does not contain $Time$ or $Number$. Using HLS identifier '%s' directly. Path: '%s'",
				segTemplate.Media, streamType, qualityOrLang, hlsSegmentIdentifier, tempPath)
			relativeSegmentPath = tempPath
		}
	}

	if relativeSegmentPath == "" {
		log.Printf("SegmentProxyHandler: Failed to determine relativeSegmentPath for HLS segment '%s', channel %s (%s) using precomputed data", segmentName, channelCfg.Name, channelCfg.ID)
		http.Error(w, "Could not determine upstream segment path", http.StatusInternalServerError)
		return
	}

	upstreamURLStr := resolvedSegmentBaseURL + relativeSegmentPath
	// log.Printf("SegmentProxyHandler: ---> Key URL Components (precomputed) for channel %s (%s): ResolvedSegmentBaseURL='%s', RelativeSegmentPath='%s', ConstructedUpstreamURLToFetch='%s'",
	//	channelCfg.Name, channelCfg.ID, resolvedSegmentBaseURL, relativeSegmentPath, upstreamURLStr)
	// log.Printf("SegmentProxyHandler: ---> Key URL Components for channel %s (%s): FinalMPDURL='%s', ResolvedSegmentBaseURL (before adding trailing slash)='%s', RelativeSegmentPath (from MPD template)='%s', ConstructedUpstreamURLToFetch='%s'", // Removed detailed success log
	//	channelCfg.Name, channelCfg.ID, finalMPDURLStr, strings.TrimSuffix(resolvedSegmentBaseURL, "/"), relativeSegmentPath, upstreamURLStr)

	// Use the shared HTTP client from AppContext
	req, err := http.NewRequest("GET", upstreamURLStr, nil)
	if err != nil {
		log.Printf("SegmentProxyHandler: Error creating request for upstream segment %s: %v", upstreamURLStr, err)
		http.Error(w, "Failed to create upstream request", http.StatusInternalServerError)
		return
	}
	if channelCfg.UserAgent != "" {
		req.Header.Set("User-Agent", channelCfg.UserAgent)
	}
	// log.Printf("SegmentProxyHandler: Requesting upstream segment: %s with User-Agent: '%s'", upstreamURLStr, req.Header.Get("User-Agent")) // Removed success log

	resp, err := appCtx.HTTPClient.Do(req) // Use shared client
	if err != nil {
		log.Printf("SegmentProxyHandler: Error fetching upstream segment %s: %v", upstreamURLStr, err)
		http.Error(w, "Failed to fetch upstream segment", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		w.WriteHeader(resp.StatusCode)
		// Try to copy a small part of the body if it's an error, for debugging
		if resp.ContentLength > 0 && resp.ContentLength < 1024 {
			bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			w.Write(bodyBytes)
		} else if resp.ContentLength == 0 {
			fmt.Fprintf(w, "Error fetching upstream segment: %s (empty body)", resp.Status)
		} else {
			// For larger or unknown content length, just write status
			fmt.Fprintf(w, "Error fetching upstream segment: %s", resp.Status)
		}
		return
	}

	// Copy relevant headers from upstream to client
	for key, values := range resp.Header {
		for _, value := range values {
			// Only copy a curated list of headers. Avoids issues with Hop-by-hop headers etc.
			if key == "Content-Type" || key == "Content-Length" || key == "ETag" || key == "Last-Modified" || key == "Cache-Control" || key == "Expires" || key == "Date" {
				w.Header().Add(key, value)
			}
		}
	}
	// Ensure Content-Type is set, default to octet-stream if not provided by upstream.
	if w.Header().Get("Content-Type") == "" {
		log.Printf("SegmentProxyHandler: Upstream response for %s missing Content-Type. Defaulting to application/octet-stream.", upstreamURLStr)
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	w.WriteHeader(http.StatusOK)
	copiedBytes, err := io.Copy(w, resp.Body)
	if err != nil {
		log.Printf("SegmentProxyHandler: Error copying segment data for %s to client: %v. Copied %d bytes.", upstreamURLStr, err, copiedBytes)
	} else {
		// log.Printf("SegmentProxyHandler: Successfully copied %d bytes for segment %s to client.", copiedBytes, upstreamURLStr) // Removed success log
	}
}

// keyServerHandler serves decryption keys for a specific channel.
func (appCtx *AppContext) keyServerHandler(w http.ResponseWriter, r *http.Request, channelCfg *config.ChannelConfig) {
	// log.Printf("KeyServerHandler: START - Request for channel %s (%s), URL=%s", channelCfg.Name, channelCfg.ID, r.URL.String()) // Removed success log
	// defer log.Printf("KeyServerHandler: END - Request for channel %s (%s), URL=%s", channelCfg.Name, channelCfg.ID, r.URL.String()) // Removed success log

	if len(channelCfg.ParsedKey) == 0 {
		log.Printf("KeyServerHandler: No key configured for channel %s (%s)", channelCfg.Name, channelCfg.ID)
		http.NotFound(w, r)
		return
	}

	// log.Printf("KeyServerHandler: Serving key for channel %s (%s). Key length: %d bytes. ParsedKey (first 4 bytes if available): %x", // Removed success log
	//	channelCfg.Name, channelCfg.ID, len(channelCfg.ParsedKey), channelCfg.ParsedKey[:min(4, len(channelCfg.ParsedKey))])
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(channelCfg.ParsedKey)))
	_, err := w.Write(channelCfg.ParsedKey)
	if err != nil {
		log.Printf("KeyServerHandler: Error writing key for channel %s (%s): %v", channelCfg.Name, channelCfg.ID, err)
	}
}

/*func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}*/
