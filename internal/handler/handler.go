package handler

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"bytes"
	"dash2hlsproxy/internal/config"
	"dash2hlsproxy/internal/mpd_manager"
	"errors" // For custom error

	"github.com/abema/go-mp4" // Replaced Eyevinn/mp4ff
	"github.com/asticode/go-astisub"
)

var errMdatFoundAndProcessed = errors.New("mdat box found and processed, stop iteration")

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
	// Ensure MPD is up-to-date.
	_, _, err := appCtx.MPDManager.GetMPD(channelCfg)
	if err != nil {
		log.Printf("SegmentProxy: Error ensuring MPD is up-to-date for channel %s (%s): %v", channelCfg.Name, channelCfg.ID, err)
		http.Error(w, "Failed to process MPD for segment proxy", http.StatusInternalServerError)
		return
	}

	cachedEntry, exists := appCtx.MPDManager.GetCachedEntry(channelCfg.ID)
	if !exists {
		log.Printf("SegmentProxy: Cached entry not found for channel %s (%s) after GetMPD call.", channelCfg.Name, channelCfg.ID)
		http.Error(w, "MPD data not available", http.StatusInternalServerError)
		return
	}

	cachedEntry.Mux.RLock()
	streamMap, streamOk := cachedEntry.PrecomputedData[streamType]
	if !streamOk {
		cachedEntry.Mux.RUnlock()
		log.Printf("SegmentProxy: Precomputed data for streamType '%s' not found. Channel %s (%s)", streamType, channelCfg.Name, channelCfg.ID)
		http.NotFound(w, r)
		return
	}
	precomputed, qualityOk := streamMap[qualityOrLang]
	cachedEntry.Mux.RUnlock() // Unlock earlier as we copied necessary data or determined not found

	if !qualityOk {
		log.Printf("SegmentProxy: Precomputed data for streamType '%s', quality/lang '%s' not found. Channel %s (%s)", streamType, qualityOrLang, channelCfg.Name, channelCfg.ID)
		http.NotFound(w, r)
		return
	}

	resolvedSegmentBaseURL := precomputed.ResolvedBaseURL
	segTemplate := precomputed.SegmentTemplate
	representationID := precomputed.RepresentationID

	if segTemplate == nil {
		log.Printf("SegmentProxy: SegmentTemplate missing in precomputed data for %s/%s in channel %s (%s)",
			streamType, qualityOrLang, channelCfg.Name, channelCfg.ID)
		http.Error(w, "Invalid MPD data (no SegmentTemplate in precomputed data)", http.StatusInternalServerError)
		return
	}

	var relativeSegmentPath string
	hlsSegmentIdentifier := ""
	isVTTRequest := false
	originalSegmentNameForMPD := segmentName // Keep original for MPD path construction if it was .vtt

	if streamType == "subtitles" && strings.HasSuffix(segmentName, ".vtt") {
		isVTTRequest = true
		hlsSegmentIdentifier = strings.TrimSuffix(segmentName, ".vtt")
		// Construct the original .m4s segment name for MPD path resolution
		originalSegmentNameForMPD = hlsSegmentIdentifier + ".m4s"
	} else {
		hlsSegmentIdentifier = strings.TrimSuffix(segmentName, ".m4s")
	}

	// Determine relative path using originalSegmentNameForMPD for MPD template substitution
	// if it was a .vtt request, because MPD templates expect .m4s (or whatever is in the template)
	mpdPathSegmentIdentifier := strings.TrimSuffix(originalSegmentNameForMPD, ".m4s") // Ensure we use the m4s identifier for MPD templates

	if mpdPathSegmentIdentifier == "init" {
		if isVTTRequest { // VTT init requests are not standard
			log.Printf("SegmentProxyHandler: Received init segment request for WebVTT ('%s'). Returning 404.", segmentName)
			http.NotFound(w, r)
			return
		}
		if segTemplate.Initialization == "" {
			log.Printf("SegmentProxyHandler: HLS requested 'init.m4s' but MPD has no Initialization for %s/%s", streamType, qualityOrLang)
			http.Error(w, "MPD has no Initialization segment", http.StatusNotFound)
			return
		}
		relativeSegmentPath = strings.ReplaceAll(segTemplate.Initialization, "$RepresentationID$", representationID)
	} else {
		if segTemplate.Media == "" {
			log.Printf("SegmentProxyHandler: MPD has no Media template for %s/%s", streamType, qualityOrLang)
			http.Error(w, "MPD has no Media segment template", http.StatusNotFound)
			return
		}
		tempPath := strings.ReplaceAll(segTemplate.Media, "$RepresentationID$", representationID)
		if strings.Contains(tempPath, "$Time$") {
			relativeSegmentPath = strings.ReplaceAll(tempPath, "$Time$", mpdPathSegmentIdentifier)
		} else if strings.Contains(tempPath, "$Number$") {
			relativeSegmentPath = strings.ReplaceAll(tempPath, "$Number$", mpdPathSegmentIdentifier)
		} else {
			log.Printf("SegmentProxyHandler: Warning - MPD Media template for %s/%s does not contain $Time$ or $Number$. Using '%s' directly.",
				streamType, qualityOrLang, mpdPathSegmentIdentifier)
			relativeSegmentPath = tempPath // Or handle as an error
		}
	}

	if relativeSegmentPath == "" {
		log.Printf("SegmentProxyHandler: Failed to determine relativeSegmentPath for HLS segment '%s'", segmentName)
		http.Error(w, "Could not determine upstream segment path", http.StatusInternalServerError)
		return
	}

	// Log components of the upstream URL - REMOVED as per user request
	/*
		log.Printf("SegmentProxyHandler: Building upstream URL. ResolvedBaseURL: '%s', RelativeSegmentPath: '%s', SegmentTemplate Media: '%s', SegmentTemplate Init: '%s', RepresentationID: '%s', HLS SegmentName: '%s'",
			resolvedSegmentBaseURL,
			relativeSegmentPath,
			safeString(segTemplate.Media),
			safeString(segTemplate.Initialization),
			representationID,
			segmentName)
	*/

	upstreamURLStr := resolvedSegmentBaseURL + relativeSegmentPath

	httpClientReq, err := http.NewRequest("GET", upstreamURLStr, nil)
	if err != nil {
		log.Printf("SegmentProxyHandler: Error creating request for upstream segment %s: %v", upstreamURLStr, err)
		http.Error(w, "Failed to create upstream request", http.StatusInternalServerError)
		return
	}
	if channelCfg.UserAgent != "" {
		httpClientReq.Header.Set("User-Agent", channelCfg.UserAgent)
	}

	upstreamResp, err := appCtx.HTTPClient.Do(httpClientReq)
	if err != nil {
		log.Printf("SegmentProxyHandler: Error fetching upstream segment %s: %v", upstreamURLStr, err)
		http.Error(w, "Failed to fetch upstream segment", http.StatusBadGateway)
		return
	}
	defer upstreamResp.Body.Close()

	if upstreamResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(io.LimitReader(upstreamResp.Body, 1024))
		log.Printf("SegmentProxyHandler: Upstream segment fetch for %s failed with status %s. Body: %s", upstreamURLStr, upstreamResp.Status, string(bodyBytes))
		http.Error(w, fmt.Sprintf("Failed to fetch upstream segment: %s", upstreamResp.Status), upstreamResp.StatusCode)
		return
	}

	if isVTTRequest && precomputed.AdaptationSet != nil && mpd_manager.IsSTPPTrack(precomputed.AdaptationSet) {
		stppData, err := io.ReadAll(upstreamResp.Body)
		if err != nil {
			log.Printf("SegmentProxyHandler: Error reading STPP segment body from %s: %v", upstreamURLStr, err)
			http.Error(w, "Failed to read STPP segment data", http.StatusInternalServerError)
			return
		}
		upstreamResp.Body.Close() // Close original body as we've read it

		var ttmlPayload []byte = stppData       // Default to full data if MP4 parsing fails
		ttmlReader := bytes.NewReader(stppData) // Initialize ttmlReader with stppData, will be updated if mdat is found

		stppDataReader := bytes.NewReader(stppData)
		var mdatFound bool
		var mdatErr error

		// Use abema/go-mp4 to find and extract MDAT payload
		_, errReadBox := mp4.ReadBoxStructure(stppDataReader, func(h *mp4.ReadHandle) (interface{}, error) {
			if h.BoxInfo.Type.String() == "mdat" {
				box, _, err := h.ReadPayload()
				if err != nil {
					mdatErr = fmt.Errorf("reading mdat payload: %w", err)
					return nil, errMdatFoundAndProcessed // Stop on error, but signal it was due to finding mdat
				}
				if mdat, ok := box.(*mp4.Mdat); ok {
					// Create a copy of the data, as the underlying buffer of Mdat might be from a larger mapped file or buffer.
					// For stppData from io.ReadAll, this might not be strictly necessary but is safer.
					ttmlPayload = make([]byte, len(mdat.Data))
					copy(ttmlPayload, mdat.Data)
					ttmlReader = bytes.NewReader(ttmlPayload)
					mdatFound = true
					// log.Printf("SegmentProxyHandler: Extracted TTML from MDAT box for %s using abema/go-mp4", segmentName) // REMOVED as per user request
				} else {
					mdatErr = fmt.Errorf("mdat payload is not of type *mp4.Mdat, actual type: %T", box)
				}
				return nil, errMdatFoundAndProcessed // Stop after processing the first mdat
			}
			// We are looking for a top-level mdat, typically after a moof in an fMP4 segment.
			// We don't need to expand children of other boxes for this specific task.
			// However, ReadBoxStructure by default does a depth-first traversal if h.Expand() is called.
			// To only check top-level boxes in a simple fMP4 segment (like moof, mdat):
			if len(h.Path) > 1 { // Path includes the current box, so > 1 means it's a child
				return nil, nil // Don't expand children
			}
			return h.Expand() // Expand top-level boxes
		})

		if errReadBox != nil && errReadBox != errMdatFoundAndProcessed {
			log.Printf("SegmentProxyHandler: Error reading STPP MP4 structure for %s with abema/go-mp4: %v", segmentName, errReadBox)
			// Fallback: ttmlPayload and ttmlReader remain as original stppData
		} else if mdatErr != nil {
			log.Printf("SegmentProxyHandler: Error processing MDAT for %s with abema/go-mp4: %v", segmentName, mdatErr)
			// Fallback
		} else if !mdatFound {
			log.Printf("SegmentProxyHandler: MDAT box not found in STPP segment for %s using abema/go-mp4. Proceeding with raw data.", segmentName)
			// Fallback
		}

		// Log details about the extracted TTML payload before parsing - REMOVED as per user request
		/*
			ttmlPayloadLength := len(ttmlPayload)
			// Log the full TTML payload as requested by the user for debugging astisub
			log.Printf("SegmentProxyHandler: Extracted TTML payload for %s. Length: %d bytes. Full Content:\n%s", segmentName, ttmlPayloadLength, string(ttmlPayload))
		*/

		subs, err := astisub.ReadFromTTML(ttmlReader)
		if err != nil {
			log.Printf("SegmentProxyHandler: Error parsing TTML data for segment %s (URL: %s): %v", segmentName, upstreamURLStr, err)
			dataSnippet := string(ttmlPayload)
			if len(dataSnippet) > 200 {
				dataSnippet = dataSnippet[:200]
			}
			log.Printf("SegmentProxyHandler: TTML data snippet: %s", dataSnippet)
			http.Error(w, "Failed to parse TTML data from STPP segment", http.StatusInternalServerError)
			return
		}

		var webvttBuffer bytes.Buffer
		if len(subs.Items) == 0 {
			log.Printf("SegmentProxyHandler: No subtitle items found in TTML for segment %s after parsing (URL: %s). Sending empty WebVTT.", segmentName, upstreamURLStr)
			// Send an empty but valid WebVTT response
			emptyWebVTT := "WEBVTT\n\n"
			w.Header().Set("Content-Type", "text/vtt")
			w.Header().Set("Content-Length", strconv.Itoa(len(emptyWebVTT))) // Correctly set Content-Length
			w.WriteHeader(http.StatusOK)
			_, writeErr := w.Write([]byte(emptyWebVTT))
			if writeErr != nil {
				log.Printf("SegmentProxyHandler: Error writing empty WebVTT data for %s to client: %v", segmentName, writeErr)
			}
			return // Handled empty subtitles
		}

		if err := subs.WriteToWebVTT(&webvttBuffer); err != nil {
			// This path should ideally not be hit if subs.Items is empty due to the check above,
			// but astisub might have other reasons to fail WriteToWebVTT.
			log.Printf("SegmentProxyHandler: Error converting TTML to WebVTT for segment %s (URL: %s): %v. Parsed %d items.", segmentName, upstreamURLStr, err, len(subs.Items))
			http.Error(w, "Failed to convert TTML to WebVTT", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/vtt")
		w.Header().Set("Content-Length", strconv.Itoa(webvttBuffer.Len()))
		w.WriteHeader(http.StatusOK)
		_, err = w.Write(webvttBuffer.Bytes())
		if err != nil {
			log.Printf("SegmentProxyHandler: Error writing WebVTT data for %s to client: %v", segmentName, err)
		}
		return // Conversion done
	}

	// Standard proxying for non-converted segments
	for key, values := range upstreamResp.Header {
		for _, value := range values {
			if key == "Content-Type" || key == "Content-Length" || key == "ETag" || key == "Last-Modified" || key == "Cache-Control" || key == "Expires" || key == "Date" {
				w.Header().Add(key, value)
			}
		}
	}
	if w.Header().Get("Content-Type") == "" {
		log.Printf("SegmentProxyHandler: Upstream response for %s missing Content-Type. Defaulting to application/octet-stream.", upstreamURLStr)
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, upstreamResp.Body)
	if err != nil {
		log.Printf("SegmentProxyHandler: Error copying segment data for %s to client: %v", upstreamURLStr, err)
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

// Helper function to safely get string value from SegmentTemplate fields
func safeString(s string) string {
	if s == "" {
		return "<empty_or_nil>"
	}
	return s
}
