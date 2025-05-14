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
	Data                 *mpd.MPD
	FetchedAt            time.Time
	FinalMPDURL          string
	Mux                  sync.RWMutex
	LastAccessedAt       time.Time
	stopAutoUpdateCh     chan struct{} // Channel to signal the auto-updater to stop
	HLSBaseMediaSequence uint64        // Our own media sequence number for HLS playlists
	LastMPDPublishTime   time.Time     // Last seen MPD publish time to track actual updates
	InitialBaseURL       string        // BaseURL from the first successful fetch for this channel
	InitialBaseURLIsSet  bool          // True if InitialBaseURL has been populated
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
			stopAutoUpdateCh:     make(chan struct{}),
			HLSBaseMediaSequence: 0, // Initial sequence
		}
		appCtx.MPDCache[channelCfg.ID] = entry
	}
	appCtx.CacheLock.Unlock()

	entry.Mux.Lock()
	defer entry.Mux.Unlock()

	// If data exists and was fetched recently by another goroutine, use it.
	if entry.Data != nil {
		log.Printf("GetMPD: MPD for channel %s (%s) was updated by another goroutine or already cached, using it. FinalURL: %s, FetchedAt: %s", channelCfg.Name, channelCfg.ID, entry.FinalMPDURL, entry.FetchedAt.Format(time.RFC3339))
		dataCopy := *entry.Data
		finalURLCopy := entry.FinalMPDURL
		entry.LastAccessedAt = time.Now()
		return finalURLCopy, &dataCopy, nil
	}

	// Determine the URL to fetch from (cached final URL or initial manifest URL)
	urlToFetch := channelCfg.Manifest
	if entry.FinalMPDURL != "" {
		log.Printf("GetMPD: Attempting to refresh MPD from cached FinalMPDURL: %s for channel %s (%s)", entry.FinalMPDURL, channelCfg.Name, channelCfg.ID)
		urlToFetch = entry.FinalMPDURL
	} else {
		log.Printf("GetMPD: No cached FinalMPDURL, fetching from initial manifest URL: %s for channel %s (%s)", channelCfg.Manifest, channelCfg.Name, channelCfg.ID)
	}

	// Fetch MPD with BaseURL consistency check and retries
	// Stored InitialBaseURL and InitialBaseURLIsSet are read while 'entry' is locked.
	newFinalURL, newMPDData, fetchedBaseURL, err := fetchMPDWithBaseURLRetry(
		urlToFetch,
		channelCfg.UserAgent,
		channelCfg.ID,
		channelCfg.Name,
		entry.InitialBaseURL,
		entry.InitialBaseURLIsSet,
		5,             // maxRetries
		2*time.Second, // retryDelay
	)

	if err != nil {
		// entry.Mux.Unlock() is deferred
		return "", nil, fmt.Errorf("GetMPD: error fetching/retrying MPD from %s for channel %s (%s): %w", urlToFetch, channelCfg.Name, channelCfg.ID, err)
	}
	// newMPDData can be nil if all attempts failed in a specific way in fetchMPDWithBaseURLRetry,
	// though the helper tries to return last good/attempted data.
	if newMPDData == nil {
		return "", nil, fmt.Errorf("GetMPD: failed to obtain MPD data after retries for channel %s (%s) from %s", channelCfg.Name, channelCfg.ID, urlToFetch)
	}

	// If InitialBaseURL wasn't set, and we have MPD data, set it now.
	if !entry.InitialBaseURLIsSet {
		entry.InitialBaseURL = fetchedBaseURL // actualBaseURL from the first successful fetch
		entry.InitialBaseURLIsSet = true
		log.Printf("GetMPD: Initial BaseURL for channel %s (%s) set to: '%s'", channelCfg.ID, channelCfg.Name, entry.InitialBaseURL)
	}

	log.Printf("GetMPD: Successfully processed MPD for channel %s (%s). FinalMPDURL: %s. MPD Type: %s, PublishTime: %s, Actual BaseURL: '%s'",
		channelCfg.Name, channelCfg.ID, newFinalURL, newMPDData.Type, newMPDData.PublishTime, fetchedBaseURL)

	entry.Data = newMPDData
	entry.FetchedAt = time.Now()
	entry.LastAccessedAt = time.Now()
	entry.FinalMPDURL = newFinalURL

	// Update HLSBaseMediaSequence and LastMPDPublishTime if this is the first fetch or publish time changed
	newPublishTime, ptErr := time.Parse(time.RFC3339, newMPDData.PublishTime)
	if ptErr != nil {
		// Try parsing without fractional seconds if RFC3339 fails (e.g. "2025-05-14T16:14:56Z")
		newPublishTime, ptErr = time.Parse("2006-01-02T15:04:05Z", newMPDData.PublishTime)
	}

	if ptErr == nil {
		if entry.LastMPDPublishTime.IsZero() || newPublishTime.After(entry.LastMPDPublishTime) {
			if !entry.LastMPDPublishTime.IsZero() { // Don't increment for the very first fetch
				entry.HLSBaseMediaSequence++
				log.Printf("GetMPD: PublishTime changed for %s. Old: %s, New: %s. Incremented HLSBaseMediaSequence to %d.", channelCfg.ID, entry.LastMPDPublishTime.Format(time.RFC3339), newPublishTime.Format(time.RFC3339), entry.HLSBaseMediaSequence)
			} else {
				log.Printf("GetMPD: First fetch for %s. HLSBaseMediaSequence is %d.", channelCfg.ID, entry.HLSBaseMediaSequence)
			}
			entry.LastMPDPublishTime = newPublishTime
		}
	} else {
		log.Printf("GetMPD: Warning - could not parse PublishTime '%s' for channel %s: %v", newMPDData.PublishTime, channelCfg.ID, ptErr)
	}

	if newMPDData.Type != "static" {
		minUpdatePeriod, errMUP := newMPDData.GetMinimumUpdatePeriod()
		if errMUP == nil && minUpdatePeriod > 0 {
			log.Printf("GetMPD: Channel %s (%s) is dynamic with MinimumUpdatePeriod %s. Starting auto-updater.", channelCfg.Name, channelCfg.ID, minUpdatePeriod)
			go appCtx.autoUpdateMPD(channelCfg, entry, minUpdatePeriod)
		} else if errMUP != nil {
			log.Printf("GetMPD: Channel %s (%s) is dynamic but error getting MinimumUpdatePeriod: %v. No auto-update.", channelCfg.Name, channelCfg.ID, errMUP)
		} else {
			log.Printf("GetMPD: Channel %s (%s) is dynamic but MinimumUpdatePeriod is zero or not set (%s). No auto-update.", channelCfg.Name, channelCfg.ID, minUpdatePeriod)
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
			var currentMinUpdatePeriodDuration time.Duration
			var lastAccessed time.Time
			var currentFinalMPDURL string
			var currentPublishTime time.Time
			var knownInitialBaseURL string // For BaseURL consistency check
			var isInitialBaseURLSet bool   // For BaseURL consistency check

			cachedEntry.Mux.RLock()
			if cachedEntry.Data == nil { // Should not happen if auto-updater is running
				log.Printf("AutoUpdater [%s]: Cached MPD data is nil. Stopping.", channelCfg.ID)
				cachedEntry.Mux.RUnlock()
				return
			}
			var mupErr error
			currentMinUpdatePeriodDuration, mupErr = cachedEntry.Data.GetMinimumUpdatePeriod()
			if mupErr != nil {
				log.Printf("AutoUpdater [%s]: Error getting current MinimumUpdatePeriod from cached MPD: %v. Stopping.", channelCfg.ID, mupErr)
				cachedEntry.Mux.RUnlock()
				return
			}
			lastAccessed = cachedEntry.LastAccessedAt
			currentFinalMPDURL = cachedEntry.FinalMPDURL
			currentPublishTime = cachedEntry.LastMPDPublishTime // Use our stored LastMPDPublishTime
			knownInitialBaseURL = cachedEntry.InitialBaseURL
			isInitialBaseURLSet = cachedEntry.InitialBaseURLIsSet
			cachedEntry.Mux.RUnlock()

			inactivityTimeout := 2 * initialMinUpdatePeriod // Use initialMinUpdatePeriod for consistent timeout
			if inactivityTimeout <= 0 {
				inactivityTimeout = 10 * time.Minute // Default fallback
			}

			if time.Since(lastAccessed) > inactivityTimeout {
				log.Printf("AutoUpdater [%s]: Channel not accessed for over %s (last_accessed: %s, initial_mup: %s). Stopping auto-update.",
					channelCfg.ID, inactivityTimeout, lastAccessed.Format(time.RFC3339), initialMinUpdatePeriod)
				return
			}

			urlToFetch := currentFinalMPDURL
			if urlToFetch == "" { // Fallback to initial manifest if final URL somehow became empty
				urlToFetch = channelCfg.Manifest
				log.Printf("AutoUpdater [%s]: FinalMPDURL was empty, falling back to initial manifest URL: %s", channelCfg.ID, urlToFetch)
			}
			if urlToFetch == "" {
				log.Printf("AutoUpdater [%s]: No URL to fetch MPD from. Skipping update.", channelCfg.ID)
				continue
			}

			log.Printf("AutoUpdater [%s]: Time to refresh MPD. Will fetch from %s", channelCfg.ID, urlToFetch)

			// Read InitialBaseURL settings before fetching. These are stable after first GetMPD.
			// No need to hold lock for these specifically if fetchMPDWithBaseURLRetry is robust.
			// currentFinalMPDURL (as urlToFetch), cachedEntry.InitialBaseURL, cachedEntry.InitialBaseURLIsSet
			// are passed to the helper.

			newFinalURL, newMPDData, fetchedBaseURL, err := fetchMPDWithBaseURLRetry(
				urlToFetch,
				channelCfg.UserAgent,
				channelCfg.ID,
				channelCfg.Name,
				knownInitialBaseURL, // This was read from cachedEntry under RLock earlier
				isInitialBaseURLSet, // This was read from cachedEntry under RLock earlier
				5,                   // maxRetries
				2*time.Second,       // retryDelay
			)

			if err != nil {
				log.Printf("AutoUpdater [%s]: Error fetching/retrying MPD from %s: %v. Will retry on next tick.", channelCfg.ID, urlToFetch, err)
				continue
			}
			if newMPDData == nil {
				log.Printf("AutoUpdater [%s]: Failed to obtain MPD data after retries from %s. Will retry on next tick.", channelCfg.ID, urlToFetch)
				continue
			}

			cachedEntry.Mux.Lock()
			log.Printf("AutoUpdater [%s]: Successfully processed MPD. Updating cache. New FinalMPDURL: %s, New PublishTime: %s, Actual BaseURL: '%s'",
				channelCfg.ID, newFinalURL, newMPDData.PublishTime, fetchedBaseURL)

			entryDataChanged := false
			newPt, ptErr := time.Parse(time.RFC3339, newMPDData.PublishTime)
			if ptErr != nil {
				newPt, ptErr = time.Parse("2006-01-02T15:04:05Z", newMPDData.PublishTime)
			}

			if ptErr == nil {
				if newPt.After(currentPublishTime) {
					entryDataChanged = true
					cachedEntry.HLSBaseMediaSequence++
					cachedEntry.LastMPDPublishTime = newPt
					log.Printf("AutoUpdater [%s]: PublishTime changed. Old: %s, New: %s. Incremented HLSBaseMediaSequence to %d.",
						channelCfg.ID, currentPublishTime.Format(time.RFC3339), newPt.Format(time.RFC3339), cachedEntry.HLSBaseMediaSequence)
				}
			} else {
				log.Printf("AutoUpdater [%s]: Warning - could not parse new PublishTime '%s': %v", newMPDData.PublishTime, ptErr)
			}

			cachedEntry.Data = newMPDData
			cachedEntry.FetchedAt = time.Now() // Update fetch time regardless of publish time change
			cachedEntry.FinalMPDURL = newFinalURL

			newMinUpdatePeriod, newMupErr := newMPDData.GetMinimumUpdatePeriod()
			if newMupErr == nil && newMinUpdatePeriod > 0 && newMinUpdatePeriod != currentMinUpdatePeriodDuration {
				log.Printf("AutoUpdater [%s]: MinimumUpdatePeriod changed from %s to %s. Adjusting ticker.",
					channelCfg.ID, currentMinUpdatePeriodDuration, newMinUpdatePeriod)
				ticker.Reset(newMinUpdatePeriod)
				initialMinUpdatePeriod = newMinUpdatePeriod // Update for inactivity timeout calculation
			} else if newMupErr != nil {
				log.Printf("AutoUpdater [%s]: Error parsing new MinimumUpdatePeriod '%s': %v. Keeping current ticker interval %s.",
					channelCfg.ID, newMPDData.MinimumUpdatePeriod, newMupErr, currentMinUpdatePeriodDuration)
			}
			cachedEntry.Mux.Unlock()

			if entryDataChanged {
				log.Printf("AutoUpdater [%s]: MPD data and HLS sequence have been updated.", channelCfg.ID)
			} else {
				log.Printf("AutoUpdater [%s]: MPD data fetched, but no publish time change detected (Old: %s, New: %s). HLS sequence not incremented.",
					channelCfg.ID, currentPublishTime.Format(time.RFC3339), newMPDData.PublishTime)
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
					codecsString := ""
					if len(as.Representations) > 0 && as.Representations[0].Codecs != "" {
						codecsString = fmt.Sprintf(",CODECS=\"%s\"", as.Representations[0].Codecs)
					}
					mediaPlaylistPath := fmt.Sprintf("/hls/%s/audio/%s/playlist.m3u8", channelCfg.ID, lang)
					playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"%s\",NAME=\"%s\",LANGUAGE=\"%s\",AUTOSELECT=YES,DEFAULT=%s%s,URI=\"%s\"\n",
						audioGroupID, name, lang, isDefault, codecsString, mediaPlaylistPath))
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
					// Typically, subtitles DEFAULT=NO unless explicitly requested or only one track.
					// For simplicity, keeping it NO. Add CHARACTERISTICS="public.accessibility.transcribes-spoken-dialog" if applicable.
					codecsString := ""
					if len(as.Representations) > 0 && as.Representations[0].Codecs != "" {
						// Subtitle codecs might be like "stpp", "wvtt" etc.
						codecsString = fmt.Sprintf(",CODECS=\"%s\"", as.Representations[0].Codecs)
					}
					mediaPlaylistPath := fmt.Sprintf("/hls/%s/subtitles/%s/playlist.m3u8", channelCfg.ID, lang)
					playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"%s\",NAME=\"%s\",LANGUAGE=\"%s\",AUTOSELECT=YES,DEFAULT=%s%s,URI=\"%s\"\n",
						subtitleGroupID, name, lang, isDefault, codecsString, mediaPlaylistPath))
					subtitleStreamIndex++
				}
			}
		}
	}
	for _, period := range mpdData.Periods {
		for _, as := range period.AdaptationSets {
			if as.ContentType == "video" {
				if len(as.Representations) == 0 {
					continue
				}

				var highestBandwidthRep *mpd.Representation
				maxBandwidth := uint(0)

				for i := range as.Representations {
					rep := &as.Representations[i] // Use pointer to avoid copying
					if rep.Bandwidth > maxBandwidth {
						maxBandwidth = rep.Bandwidth
						highestBandwidthRep = rep
					}
				}

				if highestBandwidthRep != nil {
					rep := highestBandwidthRep // Use the determined highest bandwidth representation
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
					// Use rep.ID for the path, assuming it's unique and suitable
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

			// For HLS playlist, use the timestamp (or number) directly as the segment identifier, with .m4s suffix.
			// The actual path construction from MPD template will be handled by segmentProxyHandler.
			// This assumes $Time$ is the relevant placeholder. If $Number$ is used, this needs adjustment or detection.
			// For simplicity, we'll assume $Time$ is used and its value is currentStartTime.
			segmentIdentifierForHLS := strconv.FormatUint(currentStartTime, 10)
			segmentURLForHLS := segmentIdentifierForHLS + ".m4s"

			allSegments = append(allSegments, struct {
				StartTime, Duration uint64
				URL                 string
			}{
				StartTime: currentStartTime, Duration: s.D, URL: segmentURLForHLS,
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

	// Get the CachedMPD entry to access our managed HLSBaseMediaSequence
	appCtx.CacheLock.RLock()
	cachedEntryForHLS, existsInCache := appCtx.MPDCache[channelCfg.ID]
	appCtx.CacheLock.RUnlock()

	var currentHLSSequence uint64 = 0 // Default if not found
	if existsInCache {
		cachedEntryForHLS.Mux.RLock()
		currentHLSSequence = cachedEntryForHLS.HLSBaseMediaSequence
		cachedEntryForHLS.Mux.RUnlock()
		log.Printf("MediaPlaylistHandler: Using HLSBaseMediaSequence %d for channel %s", currentHLSSequence, channelCfg.ID)
	} else {
		// This case should ideally not happen if GetMPD has been called for this channel
		// and the entry should always exist if the auto-updater is running or has run.
		log.Printf("MediaPlaylistHandler: Warning - CachedMPD entry not found for channel %s. Using default HLS sequence 0.", channelCfg.ID)
	}
	playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", currentHLSSequence))

	// Determine startIndex for segment windowing from the MPD's timeline.
	// This no longer directly sets EXT-X-MEDIA-SEQUENCE but is used for segment selection.
	startIndex := 0
	if len(allSegments) > NumLiveSegments {
		startIndex = len(allSegments) - NumLiveSegments
	}

	segmentURLBasePath := fmt.Sprintf("/hls/%s/%s/%s/", channelCfg.ID, streamType, qualityOrLang)

	// Add EXT-X-MAP for FMP4 initialization segment
	if segTemplate.Initialization != "" {
		// For HLS, use a fixed name like "init.m4s" for the initialization segment.
		// The segmentProxyHandler will map "init.m4s" back to the actual path from segTemplate.Initialization.
		hlsInitSegmentName := "init.m4s"
		mapURI := segmentURLBasePath + hlsInitSegmentName
		playlist.WriteString(fmt.Sprintf("#EXT-X-MAP:URI=\"%s\"\n", mapURI))
		log.Printf("MediaPlaylist: Added EXT-X-MAP with URI: %s (maps to MPD init segment) for %s/%s in channel %s", mapURI, streamType, qualityOrLang, channelCfg.ID)
	} else {
		log.Printf("MediaPlaylist: No Initialization pattern found in SegmentTemplate for %s/%s in channel %s. EXT-X-MAP not added.", streamType, qualityOrLang, channelCfg.ID)
	}

	if len(channelCfg.ParsedKey) > 0 && streamType != "subtitles" {
		keyURI := fmt.Sprintf("/hls/%s/key", channelCfg.ID)
		log.Printf("MediaPlaylist: Adding EXT-X-KEY tag for streamType '%s'. URI: %s (IV omitted for CMAF)", streamType, keyURI)
		playlist.WriteString(fmt.Sprintf("#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"%s\",KEYFORMAT=\"identity\"\n", keyURI))
	} else {
		if streamType == "subtitles" {
			log.Printf("MediaPlaylist: Not adding EXT-X-KEY tag for subtitles streamType '%s', channel %s.", streamType, channelCfg.ID)
		} else {
			log.Printf("MediaPlaylist: No key defined for channel %s or streamType is '%s'. Not adding EXT-X-KEY tag.", channelCfg.ID, streamType)
		}
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
	hlsSegmentIdentifier := strings.TrimSuffix(segmentName, ".m4s")

	if hlsSegmentIdentifier == "init" {
		if segTemplate.Initialization == "" {
			log.Printf("SegmentProxyHandler: HLS requested 'init.m4s' but MPD has no Initialization segment for %s/%s, channel %s (%s)",
				streamType, qualityOrLang, channelCfg.Name, channelCfg.ID)
			http.Error(w, "MPD has no Initialization segment", http.StatusNotFound)
			return
		}
		// Construct the actual path for the initialization segment from MPD template
		relativeSegmentPath = strings.ReplaceAll(segTemplate.Initialization, "$RepresentationID$", targetRep.ID)
		// $Time$ and $Number$ are not typically expected in Initialization templates by this logic
		log.Printf("SegmentProxyHandler: Mapped HLS 'init.m4s' to MPD Initialization path: '%s'", relativeSegmentPath)
	} else {
		// Assume hlsSegmentIdentifier is the $Time$ or $Number$ value
		if segTemplate.Media == "" {
			log.Printf("SegmentProxyHandler: HLS requested media segment '%s' but MPD has no Media template for %s/%s, channel %s (%s)",
				segmentName, streamType, qualityOrLang, channelCfg.Name, channelCfg.ID)
			http.Error(w, "MPD has no Media segment template", http.StatusNotFound)
			return
		}
		// Construct the actual path for the media segment from MPD template
		tempPath := strings.ReplaceAll(segTemplate.Media, "$RepresentationID$", targetRep.ID)

		// Check for $Time$ or $Number$ and substitute. Prioritize $Time$.
		if strings.Contains(tempPath, "$Time$") {
			relativeSegmentPath = strings.ReplaceAll(tempPath, "$Time$", hlsSegmentIdentifier)
		} else if strings.Contains(tempPath, "$Number$") {
			// If $Number$ is used, hlsSegmentIdentifier should correspond to it.
			// This assumes segment numbers in MPD are 1-based if $Number$ is used,
			// and HLS media sequence might need adjustment if they are 0-based.
			// For now, direct substitution.
			relativeSegmentPath = strings.ReplaceAll(tempPath, "$Number$", hlsSegmentIdentifier)
		} else {
			// If no $Time$ or $Number$, this implies the template might be static or use other placeholders not supported.
			// Or, the hlsSegmentIdentifier itself is the filename (without extension).
			// This case is ambiguous with the current HLS naming strategy.
			// For now, assume if no $Time$/$Number$, the template itself is the relative path (after $RepresentationID$ subst).
			// This might be incorrect if the template was e.g. "segment.mp4" and hlsSegmentIdentifier was "segment"
			log.Printf("SegmentProxyHandler: Warning - MPD Media template '%s' for %s/%s does not contain $Time$ or $Number$. Using HLS identifier '%s' directly in path reconstruction might be problematic. Current relativeSegmentPath: '%s'",
				segTemplate.Media, streamType, qualityOrLang, hlsSegmentIdentifier, tempPath)
			// A possible fallback: if the template is just a filename, use it.
			// If hlsSegmentIdentifier was derived from this filename (without ext), we need to append original ext.
			// This part is complex if we don't enforce that hlsSegmentIdentifier IS the time/number.
			// Given mediaPlaylistHandler now uses timestamp as identifier, this 'else' should be rare for $Time$ based templates.
			// If it happens, it implies a misconfiguration or an MPD structure not fitting the $Time$/$Number$ model well.
			// For now, we will assume that if $Time$ or $Number$ is not in the template,
			// the `tempPath` (after $RepresentationID$ substitution) is the direct path.
			// This is unlikely to be correct if the template was just e.g. "media.ts".
			// A safer bet if no $Time$/$Number$ is to assume the template is literal after $RepID$
			relativeSegmentPath = tempPath
			log.Printf("SegmentProxyHandler: Using path '%s' after $RepresentationID$ substitution as no $Time$/$Number$ found in template '%s'", relativeSegmentPath, segTemplate.Media)
		}
		log.Printf("SegmentProxyHandler: Mapped HLS segment '%s' (identifier: '%s') to MPD Media path: '%s'", segmentName, hlsSegmentIdentifier, relativeSegmentPath)
	}

	if relativeSegmentPath == "" {
		log.Printf("SegmentProxyHandler: Failed to determine relativeSegmentPath for HLS segment '%s', channel %s (%s)", segmentName, channelCfg.Name, channelCfg.ID)
		http.Error(w, "Could not determine upstream segment path", http.StatusInternalServerError)
		return
	}

	upstreamURLStr := resolvedSegmentBaseURL + relativeSegmentPath
	log.Printf("SegmentProxyHandler: ---> Key URL Components for channel %s (%s): FinalMPDURL='%s', ResolvedSegmentBaseURL (before adding trailing slash)='%s', RelativeSegmentPath (from MPD template)='%s', ConstructedUpstreamURLToFetch='%s'",
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

// fetchMPDWithBaseURLRetry fetches an MPD and retries if the BaseURL does not match a known initial BaseURL.
// - initialFetchURL: The URL to fetch the MPD from.
// - userAgent: The User-Agent string to use for the HTTP request.
// - channelID, channelName: For logging purposes.
// - knownInitialBaseURL: The expected BaseURL (MPD.BaseURLs[0]).
// - isInitialBaseURLSet: True if knownInitialBaseURL is authoritatively set. If false, the fetched BaseURL is considered the initial one.
// - maxRetries: Number of retries if BaseURL mismatches.
// - retryDelay: Duration to wait between retries.
// It returns the final URL from which the MPD was fetched (after redirects), the parsed MPD data,
// the actual BaseURL found in the fetched MPD, and an error if fetching fails persistently.
func fetchMPDWithBaseURLRetry(
	initialFetchURL string,
	userAgent string,
	channelID string,
	channelName string,
	knownInitialBaseURL string,
	isInitialBaseURLSet bool,
	maxRetries int,
	retryDelay time.Duration,
) (finalURL string, mpdObj *mpd.MPD, actualBaseURL string, err error) {

	log.Printf("fetchMPDWithBaseURLRetry: Fetching for channel %s (%s) from %s. Expecting BaseURL: '%s' (isSet: %t)",
		channelID, channelName, initialFetchURL, knownInitialBaseURL, isInitialBaseURLSet)

	fetchedFinalURL, fetchedMPDObj, fetchErr := mpd.FetchAndParseMPD(initialFetchURL, userAgent)
	if fetchErr != nil {
		// Return initialFetchURL as finalURL on error, consistent with original FetchAndParseMPD behavior for caller
		return initialFetchURL, nil, "", fmt.Errorf("initial fetch failed: %w", fetchErr)
	}

	var currentActualBaseURL string
	if fetchedMPDObj != nil && len(fetchedMPDObj.BaseURLs) > 0 && fetchedMPDObj.BaseURLs[0] != "" {
		currentActualBaseURL = fetchedMPDObj.BaseURLs[0]
	}

	if !isInitialBaseURLSet {
		// This is effectively the first fetch establishing the BaseURL, or InitialBaseURL wasn't set yet.
		// The caller (GetMPD) will use currentActualBaseURL to set the entry's InitialBaseURL.
		log.Printf("fetchMPDWithBaseURLRetry: Initial BaseURL for channel %s (%s) determined as: '%s' from URL %s (final: %s)",
			channelID, channelName, currentActualBaseURL, initialFetchURL, fetchedFinalURL)
		return fetchedFinalURL, fetchedMPDObj, currentActualBaseURL, nil
	}

	// InitialBaseURL is set, so we must compare.
	if currentActualBaseURL == knownInitialBaseURL {
		// Matches, all good.
		log.Printf("fetchMPDWithBaseURLRetry: BaseURL for channel %s (%s) matches expected: '%s'. URL: %s (final: %s)",
			channelID, channelName, knownInitialBaseURL, initialFetchURL, fetchedFinalURL)
		return fetchedFinalURL, fetchedMPDObj, currentActualBaseURL, nil
	}

	// Mismatch, start retry loop.
	log.Printf("fetchMPDWithBaseURLRetry: BaseURL mismatch for channel %s (%s). Expected: '%s', Got: '%s'. URL: %s (final: %s). Attempting retries...",
		channelID, channelName, knownInitialBaseURL, currentActualBaseURL, initialFetchURL, fetchedFinalURL)

	// Store the MPD from the first attempt in case all retries fail to fetch anything
	lastGoodMPD := fetchedMPDObj
	lastGoodFinalURL := fetchedFinalURL
	lastGoodActualBaseURL := currentActualBaseURL

	for i := 0; i < maxRetries; i++ {
		log.Printf("fetchMPDWithBaseURLRetry: Retry %d/%d for BaseURL mismatch on channel %s (%s). Delaying %s...",
			i+1, maxRetries, channelID, channelName, retryDelay)
		time.Sleep(retryDelay)

		log.Printf("fetchMPDWithBaseURLRetry: Retry %d/%d, fetching from %s for channel %s (%s)",
			i+1, maxRetries, initialFetchURL, channelID, channelName)

		retryFinalURL, retryMPDData, retryErr := mpd.FetchAndParseMPD(initialFetchURL, userAgent)
		if retryErr != nil {
			log.Printf("fetchMPDWithBaseURLRetry: Retry %d/%d fetch failed for channel %s (%s): %v",
				i+1, maxRetries, channelID, channelName, retryErr)
			if i == maxRetries-1 { // Last retry attempt also failed to fetch
				log.Printf("fetchMPDWithBaseURLRetry: Max retries reached, last retry fetch failed for channel %s (%s). Returning MPD from initial attempt (BaseURL: '%s').",
					channelID, channelName, lastGoodActualBaseURL)
				return lastGoodFinalURL, lastGoodMPD, lastGoodActualBaseURL, nil // Return data from before this failing retry
			}
			continue // Try next retry
		}

		var retryAttemptBaseURL string
		if retryMPDData != nil && len(retryMPDData.BaseURLs) > 0 && retryMPDData.BaseURLs[0] != "" {
			retryAttemptBaseURL = retryMPDData.BaseURLs[0]
		}

		if retryAttemptBaseURL == knownInitialBaseURL {
			log.Printf("fetchMPDWithBaseURLRetry: Retry %d/%d successful. BaseURL for channel %s (%s) now matches: '%s'. Final URL: %s",
				i+1, maxRetries, channelID, channelName, knownInitialBaseURL, retryFinalURL)
			return retryFinalURL, retryMPDData, retryAttemptBaseURL, nil // Success
		}

		// Still mismatch, update lastGoodMPD to this attempt's result for the next log or if it's the last one
		lastGoodMPD = retryMPDData
		lastGoodFinalURL = retryFinalURL
		lastGoodActualBaseURL = retryAttemptBaseURL // This is the new "actual" but still mismatched

		log.Printf("fetchMPDWithBaseURLRetry: Retry %d/%d BaseURL still mismatch for channel %s (%s). Expected: '%s', Got: '%s'. Final URL: %s",
			i+1, maxRetries, channelID, channelName, knownInitialBaseURL, retryAttemptBaseURL, retryFinalURL)

		if i == maxRetries-1 { // Last retry, fetch succeeded but still mismatch
			log.Printf("fetchMPDWithBaseURLRetry: Max retries reached, BaseURL still mismatch for channel %s (%s). Proceeding with this last fetched MPD (BaseURL: '%s').",
				channelID, channelName, retryAttemptBaseURL)
			return retryFinalURL, retryMPDData, retryAttemptBaseURL, nil // Return the MPD from the last retry attempt
		}
	}
	// This part should ideally not be reached if maxRetries >= 0.
	// If maxRetries is 0, the initial fetch result (handled before loop) is returned.
	// Fallback, though logically covered:
	log.Printf("fetchMPDWithBaseURLRetry: Exited retry loop unexpectedly for channel %s (%s). Returning last known good MPD (BaseURL: '%s').",
		channelID, channelName, lastGoodActualBaseURL)
	return lastGoodFinalURL, lastGoodMPD, lastGoodActualBaseURL, nil
}
