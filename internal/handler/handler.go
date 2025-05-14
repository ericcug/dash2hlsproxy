package handler

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"dash2hlsproxy/internal/config"
	"dash2hlsproxy/internal/mpd"
)

// CachedMPD holds a parsed MPD, its fetch time, and the final URL it was fetched from.
type CachedMPD struct {
	Data             *mpd.MPD
	FetchedAt        time.Time
	FinalMPDURL      string
	Mux              sync.RWMutex
	LastAccessedAt   time.Time
	stopAutoUpdateCh chan struct{} // Channel to signal the auto-updater to stop
}

// AppContext holds dependencies for handlers, like the app configuration.
type AppContext struct {
	Config    *config.AppConfig
	MPDCache  map[string]*CachedMPD
	CacheLock sync.RWMutex
}

// GetMPD retrieves a parsed MPD for a channel, using a cache.
// It returns the final URL from which the MPD was fetched, the parsed MPD, and an error.
func (appCtx *AppContext) GetMPD(channelCfg *config.ChannelConfig) (string, *mpd.MPD, error) {
	log.Printf("GetMPD: Called for channel %s (%s)", channelCfg.Name, channelCfg.ID)
	appCtx.CacheLock.RLock()
	cachedEntry, exists := appCtx.MPDCache[channelCfg.ID]
	appCtx.CacheLock.RUnlock()

	if exists {
		cachedEntry.Mux.RLock()
		if entryData := cachedEntry.Data; entryData != nil {
			log.Printf("GetMPD: Using cached MPD for channel %s (%s), finalURL: %s, FetchedAt: %s", channelCfg.Name, channelCfg.ID, cachedEntry.FinalMPDURL, cachedEntry.FetchedAt.Format(time.RFC3339))
			dataCopy := *entryData
			finalURLCopy := cachedEntry.FinalMPDURL
			cachedEntry.LastAccessedAt = time.Now()
			cachedEntry.Mux.RUnlock()
			return finalURLCopy, &dataCopy, nil
		}
		cachedEntry.Mux.RUnlock()
	}

	log.Printf("GetMPD: Fetching new MPD for channel %s (%s) from manifest: %s", channelCfg.Name, channelCfg.ID, channelCfg.Manifest)

	appCtx.CacheLock.Lock()
	entry, ok := appCtx.MPDCache[channelCfg.ID]
	if !ok {
		entry = &CachedMPD{
			stopAutoUpdateCh: make(chan struct{}),
		}
		appCtx.MPDCache[channelCfg.ID] = entry
	}
	appCtx.CacheLock.Unlock()

	entry.Mux.Lock()
	defer entry.Mux.Unlock()

	if entry.Data != nil {
		log.Printf("GetMPD: MPD for channel %s (%s) was updated by another goroutine or already cached, using it. FinalURL: %s, FetchedAt: %s", channelCfg.Name, channelCfg.ID, entry.FinalMPDURL, entry.FetchedAt.Format(time.RFC3339))
		dataCopy := *entry.Data
		finalURLCopy := entry.FinalMPDURL
		entry.LastAccessedAt = time.Now()
		return finalURLCopy, &dataCopy, nil
	}

	urlToFetch := channelCfg.Manifest
	if entry.FinalMPDURL != "" {
		log.Printf("GetMPD: Attempting to refresh MPD from cached FinalMPDURL: %s for channel %s (%s)", entry.FinalMPDURL, channelCfg.Name, channelCfg.ID)
		urlToFetch = entry.FinalMPDURL
	} else {
		log.Printf("GetMPD: No cached FinalMPDURL, fetching from initial manifest URL: %s for channel %s (%s)", channelCfg.Manifest, channelCfg.Name, channelCfg.ID)
	}

	newFinalURL, newMPDData, err := mpd.FetchAndParseMPD(urlToFetch, channelCfg.UserAgent)
	if err != nil {
		return "", nil, fmt.Errorf("GetMPD: error fetching from %s for channel %s (%s): %w", urlToFetch, channelCfg.Name, channelCfg.ID, err)
	}

	log.Printf("GetMPD: Successfully fetched new MPD for channel %s (%s). New FinalMPDURL: %s. MPD Type: %s", channelCfg.Name, channelCfg.ID, newFinalURL, newMPDData.Type)
	entry.Data = newMPDData
	entry.FetchedAt = time.Now()
	entry.LastAccessedAt = time.Now()
	entry.FinalMPDURL = newFinalURL

	if newMPDData.Type != "static" {
		minUpdatePeriod, err := newMPDData.GetMinimumUpdatePeriod()
		if err == nil && minUpdatePeriod > 0 {
			log.Printf("GetMPD: Channel %s (%s) is dynamic with MinimumUpdatePeriod %s. Starting auto-updater.", channelCfg.Name, channelCfg.ID, minUpdatePeriod)
			go appCtx.autoUpdateMPD(channelCfg, entry, minUpdatePeriod)
		} else if err != nil {
			log.Printf("GetMPD: Channel %s (%s) is dynamic but error getting MinimumUpdatePeriod: %v. No auto-update.", channelCfg.Name, channelCfg.ID, err)
		} else {
			log.Printf("GetMPD: Channel %s (%s) is dynamic but MinimumUpdatePeriod is zero or not set. No auto-update.", channelCfg.Name, channelCfg.ID)
		}
	}

	dataCopy := *newMPDData
	log.Printf("GetMPD: Returning new MPD for channel %s (%s). FinalMPDURL: %s", channelCfg.Name, channelCfg.ID, newFinalURL)
	return newFinalURL, &dataCopy, nil
}

func (appCtx *AppContext) autoUpdateMPD(channelCfg *config.ChannelConfig, cachedEntry *CachedMPD, initialMinUpdatePeriod time.Duration) {
	ticker := time.NewTicker(initialMinUpdatePeriod)
	defer ticker.Stop()

	log.Printf("AutoUpdater started for channel %s (%s). Update interval: %s", channelCfg.Name, channelCfg.ID, initialMinUpdatePeriod)

	for {
		select {
		case <-ticker.C:
			cachedEntry.Mux.RLock()
			lastAccessed := cachedEntry.LastAccessedAt
			currentMinUpdatePeriodDuration, err := cachedEntry.Data.GetMinimumUpdatePeriod()
			if err != nil {
				log.Printf("AutoUpdater [%s]: Error getting current MinimumUpdatePeriod from cached MPD: %v. Stopping.", channelCfg.ID, err)
				cachedEntry.Mux.RUnlock()
				return
			}
			cachedEntry.Mux.RUnlock()

			inactivityTimeout := 2 * initialMinUpdatePeriod
			if inactivityTimeout <= 0 {
				inactivityTimeout = 10 * time.Minute
			}

			if time.Since(lastAccessed) > inactivityTimeout {
				log.Printf("AutoUpdater [%s]: Channel not accessed for over %s. Stopping auto-update.", channelCfg.ID, inactivityTimeout)
				return
			}

			log.Printf("AutoUpdater [%s]: Time to refresh MPD.", channelCfg.ID)
			urlToFetch := ""
			cachedEntry.Mux.RLock()
			if cachedEntry.FinalMPDURL != "" {
				urlToFetch = cachedEntry.FinalMPDURL
			} else {
				urlToFetch = channelCfg.Manifest
			}
			cachedEntry.Mux.RUnlock()

			if urlToFetch == "" {
				log.Printf("AutoUpdater [%s]: No URL to fetch MPD from. Skipping update.", channelCfg.ID)
				continue
			}

			log.Printf("AutoUpdater [%s]: Fetching from %s", channelCfg.ID, urlToFetch)
			newFinalURL, newMPDData, err := mpd.FetchAndParseMPD(urlToFetch, channelCfg.UserAgent)
			if err != nil {
				log.Printf("AutoUpdater [%s]: Error fetching MPD: %v. Will retry on next tick.", channelCfg.ID, err)
				continue
			}

			cachedEntry.Mux.Lock()
			log.Printf("AutoUpdater [%s]: Successfully fetched new MPD. Updating cache. New FinalMPDURL: %s", channelCfg.ID, newFinalURL)
			entryDataChanged := (cachedEntry.Data == nil || newMPDData.PublishTime != cachedEntry.Data.PublishTime)
			cachedEntry.Data = newMPDData
			cachedEntry.FetchedAt = time.Now()
			cachedEntry.FinalMPDURL = newFinalURL

			newMinUpdatePeriod, newMupErr := newMPDData.GetMinimumUpdatePeriod()
			if newMupErr == nil && newMinUpdatePeriod > 0 && newMinUpdatePeriod != currentMinUpdatePeriodDuration {
				log.Printf("AutoUpdater [%s]: MinimumUpdatePeriod changed from %s to %s. Adjusting ticker.",
					channelCfg.ID, currentMinUpdatePeriodDuration, newMinUpdatePeriod)
				ticker.Reset(newMinUpdatePeriod)
			} else if newMupErr != nil {
				log.Printf("AutoUpdater [%s]: Error parsing new MinimumUpdatePeriod: %v. Keeping current ticker interval.", channelCfg.ID, newMupErr)
			}
			cachedEntry.Mux.Unlock()

			if entryDataChanged {
				log.Printf("AutoUpdater [%s]: MPD data has been updated.", channelCfg.ID)
			} else {
				log.Printf("AutoUpdater [%s]: MPD data fetched, but no significant change detected (based on PublishTime).", channelCfg.ID)
			}

		case <-cachedEntry.stopAutoUpdateCh:
			log.Printf("AutoUpdater [%s]: Received stop signal. Shutting down.", channelCfg.ID)
			return
		}
	}
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

	appCtx.CacheLock.RLock()
	cachedEntry, exists := appCtx.MPDCache[channelID]
	appCtx.CacheLock.RUnlock()
	if exists {
		cachedEntry.Mux.Lock()
		cachedEntry.LastAccessedAt = time.Now()
		cachedEntry.Mux.Unlock()
	}

	log.Printf("HLS request for channel: %s (%s), Path: /hls/%s", channelCfg.Name, channelID, path)
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
			appCtx.mediaPlaylistHandler(w, r, channelCfg, streamType, qualityOrLang, fileName)
		} else {
			appCtx.segmentProxyHandler(w, r, channelCfg, streamType, qualityOrLang, fileName)
		}
	default:
		log.Printf("Unhandled HLS path structure: /hls/%s (parts: %d)", path, len(parts))
		http.Error(w, "Invalid HLS path structure or unsupported endpoint", http.StatusBadRequest)
	}
}

func (appCtx *AppContext) masterPlaylistHandler(w http.ResponseWriter, r *http.Request, channelCfg *config.ChannelConfig) {
	log.Printf("Serving MASTER playlist for channel %s (%s)", channelCfg.Name, channelCfg.ID)

	_, mpdData, err := appCtx.GetMPD(channelCfg)
	if err != nil {
		log.Printf("Error getting MPD for master playlist %s (%s): %v", channelCfg.Name, channelCfg.ID, err)
		http.Error(w, "Failed to process MPD", http.StatusInternalServerError)
		return
	}
	log.Printf("Successfully fetched and parsed MPD for %s (%s): Type=%s", channelCfg.Name, channelCfg.ID, mpdData.Type)

	var playlist bytes.Buffer
	playlist.WriteString("#EXTM3U\n")
	playlist.WriteString("#EXT-X-VERSION:7\n")

	audioGroupID := "audio_grp"
	subtitleGroupID := "subs_grp"
	audioStreamIndex := 0
	for _, period := range mpdData.Periods {
		for _, as := range period.AdaptationSets {
			if as.ContentType == "audio" {
				if len(as.Representations) > 0 {
					lang := as.Lang
					if lang == "" {
						lang = fmt.Sprintf("audio%d", audioStreamIndex)
					}
					name := lang
					if as.Lang != "" {
						name = fmt.Sprintf("Audio %s", as.Lang)
					}
					isDefault := "NO"
					if audioStreamIndex == 0 {
						isDefault = "YES"
					}
					mediaPlaylistPath := fmt.Sprintf("/hls/%s/audio/%s/playlist.m3u8", channelCfg.ID, lang)
					playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"%s\",NAME=\"%s\",LANGUAGE=\"%s\",AUTOSELECT=YES,DEFAULT=%s,URI=\"%s\"\n",
						audioGroupID, name, lang, isDefault, mediaPlaylistPath))
					audioStreamIndex++
				}
			}
		}
	}
	subtitleStreamIndex := 0
	for _, period := range mpdData.Periods {
		for _, as := range period.AdaptationSets {
			if as.ContentType == "text" {
				if len(as.Representations) > 0 {
					lang := as.Lang
					if lang == "" {
						lang = fmt.Sprintf("sub%d", subtitleStreamIndex)
					}
					name := fmt.Sprintf("Subtitles %s", lang)
					isDefault := "NO"
					mediaPlaylistPath := fmt.Sprintf("/hls/%s/subtitles/%s/playlist.m3u8", channelCfg.ID, lang)
					playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"%s\",NAME=\"%s\",LANGUAGE=\"%s\",AUTOSELECT=YES,DEFAULT=%s,URI=\"%s\"\n",
						subtitleGroupID, name, lang, isDefault, mediaPlaylistPath))
					subtitleStreamIndex++
				}
			}
		}
	}
	for _, period := range mpdData.Periods {
		for _, as := range period.AdaptationSets {
			if as.ContentType == "video" {
				for _, rep := range as.Representations {
					var streamInf strings.Builder
					streamInf.WriteString("#EXT-X-STREAM-INF:PROGRAM-ID=1")
					streamInf.WriteString(fmt.Sprintf(",BANDWIDTH=%d", rep.Bandwidth))
					if rep.Width > 0 && rep.Height > 0 {
						streamInf.WriteString(fmt.Sprintf(",RESOLUTION=%dx%d", rep.Width, rep.Height))
					}
					if rep.Codecs != "" {
						streamInf.WriteString(fmt.Sprintf(",CODECS=\"%s\"", rep.Codecs))
					}
					if audioStreamIndex > 0 {
						streamInf.WriteString(fmt.Sprintf(",AUDIO=\"%s\"", audioGroupID))
					}
					if subtitleStreamIndex > 0 {
						streamInf.WriteString(fmt.Sprintf(",SUBTITLES=\"%s\"", subtitleGroupID))
					}
					playlist.WriteString(streamInf.String() + "\n")
					videoMediaPlaylistPath := fmt.Sprintf("/hls/%s/video/%s/playlist.m3u8", channelCfg.ID, rep.ID)
					playlist.WriteString(videoMediaPlaylistPath + "\n")
				}
			}
		}
	}
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	fmt.Fprint(w, playlist.String())
}

const NumLiveSegments = 5

func (appCtx *AppContext) mediaPlaylistHandler(w http.ResponseWriter, r *http.Request, channelCfg *config.ChannelConfig, streamType string, qualityOrLang string, playlistFile string) {
	log.Printf("MediaPlaylistHandler: START - Request for channel %s (%s): type=%s, quality/lang=%s, file=%s, URL=%s",
		channelCfg.Name, channelCfg.ID, streamType, qualityOrLang, playlistFile, r.URL.String())
	defer log.Printf("MediaPlaylistHandler: END - Request for channel %s (%s): type=%s, quality/lang=%s, file=%s, URL=%s",
		channelCfg.Name, channelCfg.ID, streamType, qualityOrLang, playlistFile, r.URL.String())

	finalMPDURLStr, mpdData, err := appCtx.GetMPD(channelCfg)
	if err != nil {
		log.Printf("Error getting MPD for media playlist %s (%s): %v", channelCfg.Name, channelCfg.ID, err)
		http.Error(w, "Failed to process MPD", http.StatusInternalServerError)
		return
	}
	finalMPDURL, _ := url.Parse(finalMPDURLStr)

	var targetAS *mpd.AdaptationSet
	var targetRep *mpd.Representation
	var resolvedSegmentBaseURL string

	found := false
	for i := range mpdData.Periods {
		p := &mpdData.Periods[i]
		currentPeriodBase := finalMPDURL

		if len(p.BaseURLs) > 0 && p.BaseURLs[0] != "" {
			periodLevelBase, _ := url.Parse(p.BaseURLs[0])
			currentPeriodBase = currentPeriodBase.ResolveReference(periodLevelBase)
		}

		for j := range p.AdaptationSets {
			as := &p.AdaptationSets[j]
			correctStreamType := false
			switch streamType {
			case "video":
				correctStreamType = as.ContentType == "video"
			case "audio":
				correctStreamType = as.ContentType == "audio"
			case "subtitles":
				correctStreamType = as.ContentType == "text"
			}

			if correctStreamType {
				if streamType == "video" {
					for k := range as.Representations {
						rep := &as.Representations[k]
						if rep.ID == qualityOrLang {
							targetAS = as
							targetRep = rep
							found = true
							break
						}
					}
				} else {
					if as.Lang == qualityOrLang {
						targetAS = as
						if len(as.Representations) > 0 {
							targetRep = &as.Representations[0]
						}
						found = true
						break
					}
				}
			}
			if found {
				asBase := currentPeriodBase
				if as.BaseURL != "" {
					asLevelBaseURL, _ := url.Parse(as.BaseURL)
					asBase = currentPeriodBase.ResolveReference(asLevelBaseURL)
				}
				resolvedSegmentBaseURL = asBase.String()
				if resolvedSegmentBaseURL != "" && !strings.HasSuffix(resolvedSegmentBaseURL, "/") {
					resolvedSegmentBaseURL += "/"
				}
				break
			}
		}
		if found {
			break
		}
	}

	if !found || targetAS == nil || targetRep == nil {
		log.Printf("MediaPlaylist: Could not find matching AdaptationSet/Representation for type=%s, quality/lang=%s in channel %s (%s)",
			streamType, qualityOrLang, channelCfg.Name, channelCfg.ID)
		http.NotFound(w, r)
		return
	}

	segTemplate := targetRep.SegmentTemplate
	if segTemplate == nil {
		segTemplate = targetAS.SegmentTemplate
	}

	if segTemplate == nil || segTemplate.SegmentTimeline == nil || len(segTemplate.SegmentTimeline.Segments) == 0 {
		log.Printf("MediaPlaylist: SegmentTemplate or SegmentTimeline missing/empty for %s/%s in channel %s (%s)",
			streamType, qualityOrLang, channelCfg.Name, channelCfg.ID)
		http.Error(w, "Invalid MPD data (no segments defined)", http.StatusInternalServerError)
		return
	}

	var playlist bytes.Buffer
	playlist.WriteString("#EXTM3U\n")
	playlist.WriteString("#EXT-X-VERSION:7\n")

	maxSegDurSeconds := 0.0
	timescale := uint64(1)
	if segTemplate.Timescale != nil {
		timescale = *segTemplate.Timescale
	}
	if timescale == 0 {
		timescale = 1
	}

	var allSegments []struct {
		StartTime, Duration uint64
		URL                 string
	}
	currentStartTime := uint64(0)
	if len(segTemplate.SegmentTimeline.Segments) > 0 && segTemplate.SegmentTimeline.Segments[0].T != nil {
		currentStartTime = *segTemplate.SegmentTimeline.Segments[0].T
	}

	for _, s := range segTemplate.SegmentTimeline.Segments {
		if s.T != nil {
			currentStartTime = *s.T
		}
		repeatCount := 0
		if s.R != nil {
			repeatCount = *s.R
		}
		for i := 0; i <= repeatCount; i++ {
			segDurationSeconds := float64(s.D) / float64(timescale)
			if segDurationSeconds > maxSegDurSeconds {
				maxSegDurSeconds = segDurationSeconds
			}

			mediaURL := strings.ReplaceAll(segTemplate.Media, "$RepresentationID$", targetRep.ID)
			mediaURL = strings.ReplaceAll(mediaURL, "$Time$", strconv.FormatUint(currentStartTime, 10))
			segmentFilename := mediaURL
			if strings.Contains(segmentFilename, "/") {
				segmentFilename = segmentFilename[strings.LastIndex(segmentFilename, "/")+1:]
			}
			allSegments = append(allSegments, struct {
				StartTime, Duration uint64
				URL                 string
			}{
				StartTime: currentStartTime, Duration: s.D, URL: segmentFilename,
			})
			currentStartTime += s.D
		}
	}

	if len(allSegments) == 0 {
		log.Printf("MediaPlaylist: No segments generated for %s/%s in channel %s", streamType, qualityOrLang, channelCfg.ID)
		http.Error(w, "No segments available", http.StatusInternalServerError)
		return
	}

	targetDuration := int(maxSegDurSeconds + 0.5)
	if targetDuration == 0 {
		targetDuration = 10
	}
	playlist.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", targetDuration))

	startIndex := 0
	if len(allSegments) > NumLiveSegments {
		startIndex = len(allSegments) - NumLiveSegments
	}
	playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", startIndex))

	segmentURLBasePath := fmt.Sprintf("/hls/%s/%s/%s/", channelCfg.ID, streamType, qualityOrLang)

	// Add EXT-X-MAP for FMP4 initialization segment
	if segTemplate.Initialization != "" {
		initMediaURL := strings.ReplaceAll(segTemplate.Initialization, "$RepresentationID$", targetRep.ID)
		// Assuming $Time$ and $Number$ are not typically in init segment patterns for MPD to HLS
		// If they were, they would need to be handled or stripped if not applicable.

		initializationSegmentFilename := initMediaURL
		// If initMediaURL could be a full path, extract filename
		if strings.Contains(initializationSegmentFilename, "/") {
			initializationSegmentFilename = initializationSegmentFilename[strings.LastIndex(initializationSegmentFilename, "/")+1:]
		}

		// Construct the full URI for the #EXT-X-MAP tag, relative to the playlist's base path
		// segmentURLBasePath is already defined as e.g., /hls/channelID/video/quality/
		mapURI := segmentURLBasePath + initializationSegmentFilename
		playlist.WriteString(fmt.Sprintf("#EXT-X-MAP:URI=\"%s\"\n", mapURI))
		log.Printf("MediaPlaylist: Added EXT-X-MAP with URI: %s for %s/%s in channel %s", mapURI, streamType, qualityOrLang, channelCfg.ID)
	} else {
		log.Printf("MediaPlaylist: No Initialization pattern found in SegmentTemplate for %s/%s in channel %s. EXT-X-MAP not added.", streamType, qualityOrLang, channelCfg.ID)
	}

	if len(channelCfg.ParsedKey) > 0 {
		keyURI := fmt.Sprintf("/hls/%s/key", channelCfg.ID)
		log.Printf("MediaPlaylist: Adding EXT-X-KEY tag. URI: %s (IV omitted for CMAF)", keyURI)
		playlist.WriteString(fmt.Sprintf("#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"%s\",KEYFORMAT=\"identity\"\n", keyURI))
	} else {
		log.Printf("MediaPlaylist: No key defined for channel %s. Not adding EXT-X-KEY tag.", channelCfg.ID)
	}

	for i := startIndex; i < len(allSegments); i++ {
		seg := allSegments[i]
		segDurationSeconds := float64(seg.Duration) / float64(timescale)
		playlist.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", segDurationSeconds))
		playlist.WriteString(segmentURLBasePath + seg.URL + "\n")
	}

	if mpdData.Type == "static" {
		playlist.WriteString("#EXT-X-ENDLIST\n")
	}

	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	fmt.Fprint(w, playlist.String())
}

func (appCtx *AppContext) segmentProxyHandler(w http.ResponseWriter, r *http.Request, channelCfg *config.ChannelConfig, streamType string, qualityOrLang string, segmentName string) {
	log.Printf("SegmentProxyHandler: START - Request for channel %s (%s): type=%s, quality/lang=%s, segment=%s, URL=%s",
		channelCfg.Name, channelCfg.ID, streamType, qualityOrLang, segmentName, r.URL.String())
	defer log.Printf("SegmentProxyHandler: END - Request for channel %s (%s): type=%s, quality/lang=%s, segment=%s, URL=%s",
		channelCfg.Name, channelCfg.ID, streamType, qualityOrLang, segmentName, r.URL.String())

	finalMPDURLStr, mpdData, err := appCtx.GetMPD(channelCfg)
	if err != nil {
		log.Printf("SegmentProxy: Error getting MPD for channel %s (%s): %v", channelCfg.Name, channelCfg.ID, err)
		http.Error(w, "Failed to process MPD for segment proxy", http.StatusInternalServerError)
		return
	}
	log.Printf("SegmentProxy: Received finalMPDURLStr from GetMPD: '%s' for channel %s (%s)", finalMPDURLStr, channelCfg.Name, channelCfg.ID)

	finalMPDURL, errParseMPDURL := url.Parse(finalMPDURLStr)
	if errParseMPDURL != nil {
		log.Printf("SegmentProxy: Error parsing finalMPDURLStr '%s' for channel %s (%s): %v", finalMPDURLStr, channelCfg.Name, channelCfg.ID, errParseMPDURL)
		http.Error(w, "Failed to parse MPD URL", http.StatusInternalServerError)
		return
	}
	if finalMPDURL.Scheme == "" || finalMPDURL.Host == "" {
		log.Printf("SegmentProxy: Warning - finalMPDURL '%s' (from '%s') is missing scheme or host. Scheme: '%s', Host: '%s'. Channel: %s (%s)",
			finalMPDURL.String(), finalMPDURLStr, finalMPDURL.Scheme, finalMPDURL.Host, channelCfg.Name, channelCfg.ID)
	}

	var targetAS *mpd.AdaptationSet
	var targetRep *mpd.Representation
	var resolvedSegmentBaseURL string

	var periodLoopDone = false
	for i := range mpdData.Periods {
		if periodLoopDone {
			break
		}
		p := &mpdData.Periods[i]
		currentPeriodBase := finalMPDURL
		log.Printf("SegmentProxy: Processing Period %d. Initial currentPeriodBase: '%s'. Channel: %s (%s)", i, currentPeriodBase.String(), channelCfg.Name, channelCfg.ID)

		if len(p.BaseURLs) > 0 && p.BaseURLs[0] != "" {
			log.Printf("SegmentProxy: Period %d has BaseURL[0]: '%s'.", i, p.BaseURLs[0])
			periodLevelBase, errParsePBase := url.Parse(p.BaseURLs[0])
			if errParsePBase != nil {
				log.Printf("SegmentProxy: Error parsing Period BaseURL '%s': %v. Using parent base '%s'.", p.BaseURLs[0], errParsePBase, currentPeriodBase.String())
			} else {
				resolvedPBase := currentPeriodBase.ResolveReference(periodLevelBase)
				log.Printf("SegmentProxy: Period BaseURL: original='%s', resolved against parent ('%s') -> '%s'.", p.BaseURLs[0], currentPeriodBase.String(), resolvedPBase.String())
				currentPeriodBase = resolvedPBase
			}
		} else {
			log.Printf("SegmentProxy: Period %d has no BaseURL. currentPeriodBase remains '%s'.", i, currentPeriodBase.String())
		}

		for j := range p.AdaptationSets {
			as := &p.AdaptationSets[j]
			isMatch := false
			switch streamType {
			case "video":
				if as.ContentType == "video" {
					for k := range as.Representations {
						rep := &as.Representations[k]
						if rep.ID == qualityOrLang {
							targetAS = as
							targetRep = rep
							isMatch = true
							break
						}
					}
				}
			case "audio":
				if as.ContentType == "audio" && as.Lang == qualityOrLang {
					targetAS = as
					if len(as.Representations) > 0 {
						targetRep = &as.Representations[0]
					}
					isMatch = true
				}
			case "subtitles":
				if as.ContentType == "text" && as.Lang == qualityOrLang {
					targetAS = as
					if len(as.Representations) > 0 {
						targetRep = &as.Representations[0]
					}
					isMatch = true
				}
			}

			if isMatch {
				log.Printf("SegmentProxy: Matched AS ID '%s' (Type: %s, Lang: '%s'). Original AS.BaseURL: '%s'. PeriodBase for this AS: '%s'",
					as.ID, as.ContentType, as.Lang, as.BaseURL, currentPeriodBase.String())

				asBase := currentPeriodBase

				if as.BaseURL != "" {
					parsedASSpecificBase, errParse := url.Parse(as.BaseURL)
					if errParse != nil {
						log.Printf("SegmentProxy: Error parsing AS.BaseURL ('%s'): %v. Using Period base '%s'.", as.BaseURL, errParse, asBase.String())
					} else {
						resolved := asBase.ResolveReference(parsedASSpecificBase)
						if resolved != nil {
							log.Printf("SegmentProxy: AS.BaseURL ('%s') resolved with Period base ('%s') -> '%s'.", as.BaseURL, asBase.String(), resolved.String())
							asBase = resolved
						} else {
							log.Printf("SegmentProxy: Warning - AS.BaseURL ('%s') resolved to nil with Period base ('%s'). Using Period base.", as.BaseURL, asBase.String())
						}
					}
				} else {
					log.Printf("SegmentProxy: No BaseURL in matched AS (ID: '%s'). Using Period base '%s'.", as.ID, asBase.String())
				}

				if asBase != nil {
					resolvedSegmentBaseURL = asBase.String()
				} else {
					resolvedSegmentBaseURL = ""
					log.Printf("SegmentProxy: CRITICAL - asBase is nil for AS (ID: '%s') after processing its BaseURL. resolvedSegmentBaseURL set to empty.", as.ID)
				}

				log.Printf("SegmentProxy: For matched AS (ID: '%s'), resolvedSegmentBaseURL is now: '%s'", as.ID, resolvedSegmentBaseURL)

				if resolvedSegmentBaseURL != "" && !strings.HasSuffix(resolvedSegmentBaseURL, "/") {
					resolvedSegmentBaseURL += "/"
				} else if resolvedSegmentBaseURL == "" {
					log.Printf("SegmentProxy: Warning - resolvedSegmentBaseURL is empty for AS (ID: '%s') after attempting to add trailing slash.", as.ID)
				}

				periodLoopDone = true
				break
			}
		}
	}

	if targetAS == nil || targetRep == nil {
		log.Printf("SegmentProxy: Could not find matching AdaptationSet/Representation for type=%s, quality/lang=%s in channel %s (%s)",
			streamType, qualityOrLang, channelCfg.Name, channelCfg.ID)
		http.NotFound(w, r)
		return
	}

	segTemplate := targetRep.SegmentTemplate
	if segTemplate == nil {
		segTemplate = targetAS.SegmentTemplate
	}

	if segTemplate == nil {
		log.Printf("SegmentProxy: SegmentTemplate missing for %s/%s in channel %s (%s)",
			streamType, qualityOrLang, channelCfg.Name, channelCfg.ID)
		http.Error(w, "Invalid MPD data (no SegmentTemplate for segment proxy)", http.StatusInternalServerError)
		return
	}

	var relativeSegmentPath string
	if strings.Contains(segTemplate.Media, "$RepresentationID$/") {
		relativeSegmentPath = targetRep.ID + "/" + segmentName
	} else if strings.Contains(segTemplate.Media, "$RepresentationID$") {
		pathPrefix := strings.ReplaceAll(segTemplate.Media, "$RepresentationID$", targetRep.ID)
		if strings.Contains(pathPrefix, "$Time$") {
			pathPrefix = strings.Split(pathPrefix, "$Time$")[0]
		} else if strings.Contains(pathPrefix, "$Number$") {
			pathPrefix = strings.Split(pathPrefix, "$Number$")[0]
		}

		if strings.HasSuffix(pathPrefix, "/") {
			relativeSegmentPath = pathPrefix + segmentName
		} else {
			relativeSegmentPath = pathPrefix + segmentName
		}
	} else {
		relativeSegmentPath = segmentName
	}

	upstreamURLStr := resolvedSegmentBaseURL + relativeSegmentPath
	log.Printf("SegmentProxyHandler: ---> Key URL Components for channel %s (%s): FinalMPDURL='%s', ResolvedSegmentBaseURL (before adding trailing slash)='%s', RelativeSegmentPath='%s', ConstructedUpstreamURLToFetch='%s'",
		channelCfg.Name, channelCfg.ID, finalMPDURLStr, strings.TrimSuffix(resolvedSegmentBaseURL, "/"), relativeSegmentPath, upstreamURLStr)

	httpClient := &http.Client{Timeout: 20 * time.Second}
	req, err := http.NewRequest("GET", upstreamURLStr, nil)
	if err != nil {
		log.Printf("SegmentProxyHandler: Error creating request for upstream segment %s: %v", upstreamURLStr, err)
		http.Error(w, "Failed to create upstream request", http.StatusInternalServerError)
		return
	}
	if channelCfg.UserAgent != "" {
		req.Header.Set("User-Agent", channelCfg.UserAgent)
	}
	log.Printf("SegmentProxyHandler: Requesting upstream segment: %s with User-Agent: '%s'", upstreamURLStr, req.Header.Get("User-Agent"))

	resp, err := httpClient.Do(req)
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
		log.Printf("SegmentProxyHandler: Successfully copied %d bytes for segment %s to client.", copiedBytes, upstreamURLStr)
	}
}

// keyServerHandler serves decryption keys for a specific channel.
func (appCtx *AppContext) keyServerHandler(w http.ResponseWriter, r *http.Request, channelCfg *config.ChannelConfig) {
	log.Printf("KeyServerHandler: START - Request for channel %s (%s), URL=%s", channelCfg.Name, channelCfg.ID, r.URL.String())
	defer log.Printf("KeyServerHandler: END - Request for channel %s (%s), URL=%s", channelCfg.Name, channelCfg.ID, r.URL.String())

	if len(channelCfg.ParsedKey) == 0 {
		log.Printf("KeyServerHandler: No key configured for channel %s (%s)", channelCfg.Name, channelCfg.ID)
		http.NotFound(w, r)
		return
	}

	log.Printf("KeyServerHandler: Serving key for channel %s (%s). Key length: %d bytes. ParsedKey (first 4 bytes if available): %x",
		channelCfg.Name, channelCfg.ID, len(channelCfg.ParsedKey), channelCfg.ParsedKey[:min(4, len(channelCfg.ParsedKey))])
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(channelCfg.ParsedKey)))
	_, err := w.Write(channelCfg.ParsedKey)
	if err != nil {
		log.Printf("KeyServerHandler: Error writing key for channel %s (%s): %v", channelCfg.Name, channelCfg.ID, err)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
