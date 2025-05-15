package mpd_manager

import (
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"dash2hlsproxy/internal/config"
	"dash2hlsproxy/internal/mpd"
	"net/url" // Added for precomputed data
)

// Helper function to identify STPP tracks
// IsSTPPTrack checks if the AdaptationSet represents an STPP subtitle track.
func IsSTPPTrack(as *mpd.AdaptationSet) bool {
	if as == nil {
		return false
	}
	// Check MimeType for "application/mp4" and Codecs for "stpp"
	// Also consider if ContentType is "text" and MimeType is "application/mp4" without specific codecs,
	// but the primary indicator is codecs="stpp".
	isMP4 := strings.Contains(as.MimeType, "application/mp4")
	hasSTPPCodec := false
	if len(as.Representations) > 0 { // Check codecs in representations
		for _, rep := range as.Representations {
			if strings.Contains(strings.ToLower(rep.Codecs), "stpp") {
				hasSTPPCodec = true
				break
			}
		}
	}
	// Fallback to checking AdaptationSet's codecs if not found in representations
	// (Though typically codecs are on Representation)
	// For simplicity, we'll assume codecs on Representation is sufficient for now.

	// A more direct check if AdaptationSet itself has codecs (less common for subtitles)
	// if !hasSTPPCodec && strings.Contains(strings.ToLower(as.Codecs), "stpp") {
	// 	hasSTPPCodec = true
	// }

	return isMP4 && hasSTPPCodec
}

const numLiveSegments = 5 // Number of segments to keep in a live media playlist

// PrecomputedSegmentData holds readily usable data for a specific stream/quality.
type PrecomputedSegmentData struct {
	ResolvedBaseURL  string // Fully resolved base URL for this segment set
	SegmentTemplate  *mpd.SegmentTemplate
	RepresentationID string
	AdaptationSet    *mpd.AdaptationSet  // Reference to the AS
	Representation   *mpd.Representation // Reference to the Rep
}

// CachedMPD holds a parsed MPD, its fetch time, and the final URL it was fetched from.
type CachedMPD struct {
	Data                 *mpd.MPD
	FetchedAt            time.Time
	FinalMPDURL          string
	Mux                  sync.RWMutex
	LastAccessedAt       time.Time
	stopAutoUpdateCh     chan struct{}     // Channel to signal the auto-updater to stop
	HLSBaseMediaSequence uint64            // Our own media sequence number for HLS playlists
	LastMPDPublishTime   time.Time         // Last seen MPD publish time to track actual updates
	InitialBaseURL       string            // BaseURL from the first successful fetch for this channel
	InitialBaseURLIsSet  bool              // True if InitialBaseURL has been populated
	MasterPlaylist       string            // Cached HLS Master Playlist
	MediaPlaylists       map[string]string // Cached HLS Media Playlists (e.g., by representation ID)
	// PrecomputedData: map[streamType]map[qualityOrLang]PrecomputedSegmentData
	PrecomputedData map[string]map[string]PrecomputedSegmentData
}

// MPDManager manages the MPD cache and updates.
type MPDManager struct {
	Config    *config.AppConfig
	MPDCache  map[string]*CachedMPD
	CacheLock sync.RWMutex
}

// NewMPDManager creates a new MPDManager.
func NewMPDManager(cfg *config.AppConfig) *MPDManager {
	return &MPDManager{
		Config:   cfg,
		MPDCache: make(map[string]*CachedMPD),
	}
}

// GetCachedEntry retrieves a cached MPD entry if it exists.
// It returns the entry and a boolean indicating if it was found.
// The caller is responsible for locking the entry's Mux if direct data access is needed.
func (m *MPDManager) GetCachedEntry(channelID string) (*CachedMPD, bool) {
	m.CacheLock.RLock()
	defer m.CacheLock.RUnlock()
	entry, exists := m.MPDCache[channelID]
	return entry, exists
}

// GetMPD retrieves a parsed MPD for a channel, using a cache.
// It returns the final URL from which the MPD was fetched, the parsed MPD, and an error.
func (m *MPDManager) GetMPD(channelCfg *config.ChannelConfig) (string, *mpd.MPD, error) {
	// log.Printf("GetMPD: Called for channel %s (%s)", channelCfg.Name, channelCfg.ID) // Removed success log
	m.CacheLock.RLock()
	cachedEntry, exists := m.MPDCache[channelCfg.ID]
	m.CacheLock.RUnlock()

	if exists {
		cachedEntry.Mux.RLock()
		if entryData := cachedEntry.Data; entryData != nil {
			// log.Printf("GetMPD: Using cached MPD for channel %s (%s), finalURL: %s, FetchedAt: %s", channelCfg.Name, channelCfg.ID, cachedEntry.FinalMPDURL, cachedEntry.FetchedAt.Format(time.RFC3339)) // Removed success log
			dataCopy := *entryData
			finalURLCopy := cachedEntry.FinalMPDURL
			cachedEntry.LastAccessedAt = time.Now()
			cachedEntry.Mux.RUnlock()
			return finalURLCopy, &dataCopy, nil
		}
		cachedEntry.Mux.RUnlock()
	}

	// log.Printf("GetMPD: Fetching new MPD for channel %s (%s) from manifest: %s", channelCfg.Name, channelCfg.ID, channelCfg.Manifest) // Removed success log

	m.CacheLock.Lock()
	entry, ok := m.MPDCache[channelCfg.ID]
	if !ok {
		entry = &CachedMPD{
			stopAutoUpdateCh:     make(chan struct{}),
			HLSBaseMediaSequence: 0, // Initial sequence
			PrecomputedData:      make(map[string]map[string]PrecomputedSegmentData),
		}
		m.MPDCache[channelCfg.ID] = entry
	}
	m.CacheLock.Unlock()

	entry.Mux.Lock()
	defer entry.Mux.Unlock()

	// If data exists and was fetched recently by another goroutine, use it.
	if entry.Data != nil {
		// log.Printf("GetMPD: MPD for channel %s (%s) was updated by another goroutine or already cached, using it. FinalURL: %s, FetchedAt: %s", channelCfg.Name, channelCfg.ID, entry.FinalMPDURL, entry.FetchedAt.Format(time.RFC3339)) // Removed success log
		dataCopy := *entry.Data // Create a shallow copy of MPD data for return
		finalURLCopy := entry.FinalMPDURL
		entry.LastAccessedAt = time.Now()
		// Note: PrecomputedData is part of 'entry' and is protected by its Mux.
		// The caller gets a copy of MPD.Data, but segmentProxyHandler will later use
		// GetCachedEntry to get the 'entry' itself to access PrecomputedData safely.
		return finalURLCopy, &dataCopy, nil
	}

	// Determine the URL to fetch from (cached final URL or initial manifest URL)
	urlToFetch := channelCfg.Manifest
	if entry.FinalMPDURL != "" {
		// log.Printf("GetMPD: Attempting to refresh MPD from cached FinalMPDURL: %s for channel %s (%s)", entry.FinalMPDURL, channelCfg.Name, channelCfg.ID) // Removed informational log
		urlToFetch = entry.FinalMPDURL
	} else {
		// log.Printf("GetMPD: No cached FinalMPDURL, fetching from initial manifest URL: %s for channel %s (%s)", channelCfg.Manifest, channelCfg.Name, channelCfg.ID) // Removed informational log
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
		// log.Printf("GetMPD: Initial BaseURL for channel %s (%s) set to: '%s'", channelCfg.ID, channelCfg.Name, entry.InitialBaseURL) // Removed success log
	}

	// log.Printf("GetMPD: Successfully processed MPD for channel %s (%s). FinalMPDURL: %s. MPD Type: %s, PublishTime: %s, Actual BaseURL: '%s'", // Removed success log
	//	channelCfg.Name, channelCfg.ID, newFinalURL, newMPDData.Type, newMPDData.PublishTime, fetchedBaseURL)

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
				// log.Printf("GetMPD: PublishTime changed for %s. Old: %s, New: %s. Incremented HLSBaseMediaSequence to %d.", channelCfg.ID, entry.LastMPDPublishTime.Format(time.RFC3339), newPublishTime.Format(time.RFC3339), entry.HLSBaseMediaSequence) // Removed success log
			} else {
				// log.Printf("GetMPD: First fetch for %s. HLSBaseMediaSequence is %d.", channelCfg.ID, entry.HLSBaseMediaSequence) // Removed success log
			}
			entry.LastMPDPublishTime = newPublishTime
		}
	} else {
		log.Printf("GetMPD: Warning - could not parse PublishTime '%s' for channel %s: %v", newMPDData.PublishTime, channelCfg.ID, ptErr)
	}

	if newMPDData.Type != "static" {
		minUpdatePeriod, errMUP := newMPDData.GetMinimumUpdatePeriod()
		if errMUP == nil && minUpdatePeriod > 0 {
			// log.Printf("GetMPD: Channel %s (%s) is dynamic with MinimumUpdatePeriod %s. Starting auto-updater.", channelCfg.Name, channelCfg.ID, minUpdatePeriod) // Removed success log
			go m.autoUpdateMPD(channelCfg, entry, minUpdatePeriod)
		} else if errMUP != nil {
			log.Printf("GetMPD: Channel %s (%s) is dynamic but error getting MinimumUpdatePeriod: %v. No auto-update.", channelCfg.Name, channelCfg.ID, errMUP)
		} else {
			// log.Printf("GetMPD: Channel %s (%s) is dynamic but MinimumUpdatePeriod is zero or not set (%s). No auto-update.", channelCfg.Name, channelCfg.ID, minUpdatePeriod) // Potentially useful, but can be verbose
		}
	}

	dataCopy := *newMPDData
	// log.Printf("GetMPD: Returning new MPD for channel %s (%s). FinalMPDURL: %s", channelCfg.Name, channelCfg.ID, newFinalURL) // Removed success log

	// Precompute data and generate HLS playlists
	// This needs to be done after entry.Data and entry.FinalMPDURL are set.
	errPrecompute := precomputeMPDData(entry) // Pass the whole entry
	if errPrecompute != nil {
		log.Printf("GetMPD: Error precomputing MPD data for channel %s (%s): %v", channelCfg.ID, channelCfg.Name, errPrecompute)
		// Decide if this is a fatal error for GetMPD. For now, log and continue.
		// The playlists might be empty or incomplete if precomputation fails partially.
	}

	masterPl, errMaster := generateMasterPlaylist(entry.Data, channelCfg.ID)
	if errMaster != nil {
		log.Printf("GetMPD: Error generating master playlist for channel %s (%s): %v", channelCfg.ID, channelCfg.Name, errMaster)
	} else {
		entry.MasterPlaylist = masterPl
	}

	mediaPls, errMedia := generateMediaPlaylists(entry.Data, entry.FinalMPDURL, channelCfg.ID, entry.HLSBaseMediaSequence, channelCfg.ParsedKey)
	if errMedia != nil {
		log.Printf("GetMPD: Error generating media playlists for channel %s (%s): %v", channelCfg.ID, channelCfg.Name, errMedia)
	} else {
		entry.MediaPlaylists = mediaPls
	}

	return newFinalURL, &dataCopy, nil
}

// precomputeMPDData populates the PrecomputedData map in CachedMPD.
// This function should be called when entry.Mux is WLock'd and entry.Data is populated.
func precomputeMPDData(cachedEntry *CachedMPD) error {
	if cachedEntry.Data == nil {
		return fmt.Errorf("MPD data is nil, cannot precompute")
	}
	if cachedEntry.FinalMPDURL == "" {
		return fmt.Errorf("FinalMPDURL is empty, cannot precompute base URLs")
	}

	finalMPDURL, errParseMPDURL := url.Parse(cachedEntry.FinalMPDURL)
	if errParseMPDURL != nil {
		return fmt.Errorf("error parsing FinalMPDURL '%s': %w", cachedEntry.FinalMPDURL, errParseMPDURL)
	}

	newPrecomputedData := make(map[string]map[string]PrecomputedSegmentData)

	for i := range cachedEntry.Data.Periods {
		p := &cachedEntry.Data.Periods[i]
		currentPeriodBase := finalMPDURL

		if len(p.BaseURLs) > 0 && p.BaseURLs[0] != "" {
			periodLevelBase, errParsePBase := url.Parse(p.BaseURLs[0])
			if errParsePBase == nil {
				currentPeriodBase = currentPeriodBase.ResolveReference(periodLevelBase)
			} else {
				log.Printf("precomputeMPDData: Error parsing Period BaseURL '%s': %v. Using parent base '%s'.", p.BaseURLs[0], errParsePBase, currentPeriodBase.String())
			}
		}

		for j := range p.AdaptationSets {
			as := &p.AdaptationSets[j]
			var streamTypeKey string
			// Determine streamTypeKey (e.g., "video", "audio", "subtitles")
			if as.ContentType == "video" || strings.Contains(as.MimeType, "video") {
				streamTypeKey = "video"
			} else if as.ContentType == "audio" || strings.Contains(as.MimeType, "audio") {
				streamTypeKey = "audio"
			} else if as.ContentType == "text" || strings.Contains(as.MimeType, "text") || strings.Contains(as.MimeType, "application/mp4") { // Add more subtitle mimetypes if necessary, include application/mp4 for STPP
				streamTypeKey = "subtitles"
			} else {
				continue // Skip unknown content types
			}

			if _, ok := newPrecomputedData[streamTypeKey]; !ok {
				newPrecomputedData[streamTypeKey] = make(map[string]PrecomputedSegmentData)
			}

			for k := range as.Representations {
				rep := &as.Representations[k]
				var qualityOrLangKey string

				switch streamTypeKey {
				case "video":
					qualityOrLangKey = rep.ID
					if qualityOrLangKey == "" { // Fallback if RepresentationID is empty
						qualityOrLangKey = fmt.Sprintf("video_rep%d_as%d_p%d", k, j, i) // More unique fallback
					}
				case "audio":
					qualityOrLangKey = as.Lang
					if qualityOrLangKey == "" {
						qualityOrLangKey = fmt.Sprintf("audio_default%d_as%d_p%d", k, j, i) // More unique fallback
					}
				case "subtitles":
					qualityOrLangKey = as.Lang
					if qualityOrLangKey == "" {
						qualityOrLangKey = fmt.Sprintf("sub_default%d_as%d_p%d", k, j, i) // More unique fallback
					}
				}
				if qualityOrLangKey == "" { // Should be rare with fallbacks
					log.Printf("precomputeMPDData: qualityOrLangKey is empty for stream %s, AS ID %s, Rep ID %s. Skipping.", streamTypeKey, as.ID, rep.ID)
					continue
				}

				asBase := currentPeriodBase
				if as.BaseURL != "" {
					parsedASSpecificBase, errParseASBase := url.Parse(as.BaseURL)
					if errParseASBase == nil {
						resolved := asBase.ResolveReference(parsedASSpecificBase)
						if resolved != nil {
							asBase = resolved
						}
					} else {
						log.Printf("precomputeMPDData: Error parsing AS.BaseURL ('%s'): %v. Using Period base '%s'.", as.BaseURL, errParseASBase, asBase.String())
					}
				}

				resolvedBaseURLStr := ""
				if asBase != nil {
					resolvedBaseURLStr = asBase.String()
				}
				if resolvedBaseURLStr != "" && !strings.HasSuffix(resolvedBaseURLStr, "/") {
					resolvedBaseURLStr += "/"
				}

				segTemplate := rep.SegmentTemplate
				if segTemplate == nil {
					segTemplate = as.SegmentTemplate
				}

				if segTemplate == nil { // Still no template, skip
					log.Printf("precomputeMPDData: No segment template found for stream %s, quality/lang %s. Skipping.", streamTypeKey, qualityOrLangKey)
					continue
				}

				newPrecomputedData[streamTypeKey][qualityOrLangKey] = PrecomputedSegmentData{
					ResolvedBaseURL:  resolvedBaseURLStr,
					SegmentTemplate:  segTemplate,
					RepresentationID: rep.ID,
					AdaptationSet:    as,
					Representation:   rep,
				}
			}
		}
	}
	cachedEntry.PrecomputedData = newPrecomputedData
	// log.Printf("Precomputed data generated for channel %s. Video entries: %d, Audio entries: %d, Subtitle entries: %d", cachedEntry.Data.ID, len(newPrecomputedData["video"]), len(newPrecomputedData["audio"]), len(newPrecomputedData["subtitles"]))
	return nil
}

func (m *MPDManager) autoUpdateMPD(channelCfg *config.ChannelConfig, cachedEntry *CachedMPD, initialMinUpdatePeriod time.Duration) {
	ticker := time.NewTicker(initialMinUpdatePeriod)
	defer ticker.Stop()

	// log.Printf("AutoUpdater started for channel %s (%s). Update interval: %s", channelCfg.Name, channelCfg.ID, initialMinUpdatePeriod) // Removed success log

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
				// log.Printf("AutoUpdater [%s]: Channel not accessed for over %s (last_accessed: %s, initial_mup: %s). Stopping auto-update.", // Informational, but can be verbose
				//	channelCfg.ID, inactivityTimeout, lastAccessed.Format(time.RFC3339), initialMinUpdatePeriod)
				return
			}

			urlToFetch := currentFinalMPDURL
			if urlToFetch == "" { // Fallback to initial manifest if final URL somehow became empty
				urlToFetch = channelCfg.Manifest
				log.Printf("AutoUpdater [%s]: FinalMPDURL was empty, falling back to initial manifest URL: %s", channelCfg.ID, urlToFetch) // Keep this, it's a recovery action
			}
			if urlToFetch == "" {
				log.Printf("AutoUpdater [%s]: No URL to fetch MPD from. Skipping update.", channelCfg.ID) // Keep this, it's a skip condition
				continue
			}

			// log.Printf("AutoUpdater [%s]: Time to refresh MPD. Will fetch from %s", channelCfg.ID, urlToFetch) // Removed success log

			newFinalURL, newMPDData, _, err := fetchMPDWithBaseURLRetry( // Assign fetchedBaseURL to _
				urlToFetch,
				channelCfg.UserAgent,
				channelCfg.ID,
				channelCfg.Name,
				knownInitialBaseURL,
				isInitialBaseURLSet,
				5,
				2*time.Second,
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
			// log.Printf("AutoUpdater [%s]: Successfully processed MPD. Updating cache. New FinalMPDURL: %s, New PublishTime: %s", // Removed success log, also removed fetchedBaseURL from here
			//	channelCfg.ID, newFinalURL, newMPDData.PublishTime)

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
					// log.Printf("AutoUpdater [%s]: PublishTime changed. Old: %s, New: %s. Incremented HLSBaseMediaSequence to %d.", // Removed success log
					//	channelCfg.ID, currentPublishTime.Format(time.RFC3339), newPt.Format(time.RFC3339), cachedEntry.HLSBaseMediaSequence)
				}
			} else {
				log.Printf("AutoUpdater [%s]: Warning - could not parse new PublishTime '%s': %v", channelCfg.ID, newMPDData.PublishTime, ptErr) // Keep warning
			}

			cachedEntry.Data = newMPDData
			cachedEntry.FetchedAt = time.Now()
			cachedEntry.FinalMPDURL = newFinalURL

			newMinUpdatePeriod, newMupErr := newMPDData.GetMinimumUpdatePeriod()
			if newMupErr == nil && newMinUpdatePeriod > 0 && newMinUpdatePeriod != currentMinUpdatePeriodDuration {
				// log.Printf("AutoUpdater [%s]: MinimumUpdatePeriod changed from %s to %s. Adjusting ticker.", // Informational
				//	channelCfg.ID, currentMinUpdatePeriodDuration, newMinUpdatePeriod)
				ticker.Reset(newMinUpdatePeriod)
				initialMinUpdatePeriod = newMinUpdatePeriod
			} else if newMupErr != nil {
				log.Printf("AutoUpdater [%s]: Error parsing new MinimumUpdatePeriod '%s': %v. Keeping current ticker interval %s.", // Keep error
					channelCfg.ID, newMPDData.MinimumUpdatePeriod, newMupErr, currentMinUpdatePeriodDuration)
			}
			if entryDataChanged {
				// log.Printf("AutoUpdater [%s]: MPD data and HLS sequence have been updated.", channelCfg.ID) // Removed success log
				// Regenerate HLS playlists and precompute data as MPD data or sequence number changed
				errPrecompute := precomputeMPDData(cachedEntry)
				if errPrecompute != nil {
					log.Printf("AutoUpdater [%s]: Error precomputing MPD data: %v", channelCfg.ID, errPrecompute)
				}

				masterPl, errMaster := generateMasterPlaylist(cachedEntry.Data, channelCfg.ID)
				if errMaster != nil {
					log.Printf("AutoUpdater [%s]: Error generating master playlist: %v", channelCfg.ID, errMaster)
				} else {
					cachedEntry.MasterPlaylist = masterPl
				}

				mediaPls, errMedia := generateMediaPlaylists(cachedEntry.Data, cachedEntry.FinalMPDURL, channelCfg.ID, cachedEntry.HLSBaseMediaSequence, channelCfg.ParsedKey)
				if errMedia != nil {
					log.Printf("AutoUpdater [%s]: Error generating media playlists: %v", channelCfg.ID, errMedia)
				} else {
					cachedEntry.MediaPlaylists = mediaPls
				}
				// log.Printf("AutoUpdater [%s]: HLS playlists and precomputed data regenerated.", channelCfg.ID) // Removed success log
			} else {
				// log.Printf("AutoUpdater [%s]: MPD data fetched, but no publish time change detected (Old: %s, New: %s). HLS sequence not incremented.", // Informational, but verbose
				//	channelCfg.ID, currentPublishTime.Format(time.RFC3339), newMPDData.PublishTime)
			}
			cachedEntry.Mux.Unlock()

		case <-cachedEntry.stopAutoUpdateCh:
			// log.Printf("AutoUpdater [%s]: Received stop signal. Shutting down.", channelCfg.ID) // Informational
			return
		}
	}
}

// fetchMPDWithBaseURLRetry fetches an MPD and retries if the BaseURL does not match a known initial BaseURL.
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

	// log.Printf("fetchMPDWithBaseURLRetry: Fetching for channel %s (%s) from %s. Expecting BaseURL: '%s' (isSet: %t)", // Removed success log
	//	channelID, channelName, initialFetchURL, knownInitialBaseURL, isInitialBaseURLSet)

	fetchedFinalURL, fetchedMPDObj, fetchErr := mpd.FetchAndParseMPD(initialFetchURL, userAgent)
	if fetchErr != nil {
		return initialFetchURL, nil, "", fmt.Errorf("initial fetch failed: %w", fetchErr)
	}

	var currentActualBaseURL string
	if fetchedMPDObj != nil && len(fetchedMPDObj.BaseURLs) > 0 && fetchedMPDObj.BaseURLs[0] != "" {
		currentActualBaseURL = fetchedMPDObj.BaseURLs[0]
	}

	if !isInitialBaseURLSet {
		// log.Printf("fetchMPDWithBaseURLRetry: Initial BaseURL for channel %s (%s) determined as: '%s' from URL %s (final: %s)", // Removed success log
		//	channelID, channelName, currentActualBaseURL, initialFetchURL, fetchedFinalURL)
		return fetchedFinalURL, fetchedMPDObj, currentActualBaseURL, nil
	}

	if currentActualBaseURL == knownInitialBaseURL {
		// log.Printf("fetchMPDWithBaseURLRetry: BaseURL for channel %s (%s) matches expected: '%s'. URL: %s (final: %s)", // Removed success log
		//	channelID, channelName, knownInitialBaseURL, initialFetchURL, fetchedFinalURL)
		return fetchedFinalURL, fetchedMPDObj, currentActualBaseURL, nil
	}

	log.Printf("fetchMPDWithBaseURLRetry: BaseURL mismatch for channel %s (%s). Expected: '%s', Got: '%s'. URL: %s (final: %s). Attempting retries...", // Keep this, it's a key event before retries
		channelID, channelName, knownInitialBaseURL, currentActualBaseURL, initialFetchURL, fetchedFinalURL)

	lastGoodMPD := fetchedMPDObj
	lastGoodFinalURL := fetchedFinalURL
	lastGoodActualBaseURL := currentActualBaseURL

	for i := 0; i < maxRetries; i++ {
		// log.Printf("fetchMPDWithBaseURLRetry: Retry %d/%d for BaseURL mismatch on channel %s (%s). Delaying %s...", // Informational
		//	i+1, maxRetries, channelID, channelName, retryDelay)
		time.Sleep(retryDelay)

		// log.Printf("fetchMPDWithBaseURLRetry: Retry %d/%d, fetching from %s for channel %s (%s)", // Informational
		//	i+1, maxRetries, initialFetchURL, channelID, channelName)

		retryFinalURL, retryMPDData, retryErr := mpd.FetchAndParseMPD(initialFetchURL, userAgent)
		if retryErr != nil {
			log.Printf("fetchMPDWithBaseURLRetry: Retry %d/%d fetch failed for channel %s (%s): %v", // Keep error
				i+1, maxRetries, channelID, channelName, retryErr)
			if i == maxRetries-1 {
				log.Printf("fetchMPDWithBaseURLRetry: Max retries reached, last retry fetch failed for channel %s (%s). Returning MPD from initial attempt (BaseURL: '%s').", // Keep info on returning old data
					channelID, channelName, lastGoodActualBaseURL)
				return lastGoodFinalURL, lastGoodMPD, lastGoodActualBaseURL, nil
			}
			continue
		}

		var retryAttemptBaseURL string
		if retryMPDData != nil && len(retryMPDData.BaseURLs) > 0 && retryMPDData.BaseURLs[0] != "" {
			retryAttemptBaseURL = retryMPDData.BaseURLs[0]
		}

		if retryAttemptBaseURL == knownInitialBaseURL {
			// log.Printf("fetchMPDWithBaseURLRetry: Retry %d/%d successful. BaseURL for channel %s (%s) now matches: '%s'. Final URL: %s", // Removed success log
			//	i+1, maxRetries, channelID, channelName, knownInitialBaseURL, retryFinalURL)
			return retryFinalURL, retryMPDData, retryAttemptBaseURL, nil
		}

		lastGoodMPD = retryMPDData
		lastGoodFinalURL = retryFinalURL
		lastGoodActualBaseURL = retryAttemptBaseURL

		log.Printf("fetchMPDWithBaseURLRetry: Retry %d/%d BaseURL still mismatch for channel %s (%s). Expected: '%s', Got: '%s'. Final URL: %s", // Keep info on persistent mismatch
			i+1, maxRetries, channelID, channelName, knownInitialBaseURL, retryAttemptBaseURL, retryFinalURL)

		if i == maxRetries-1 {
			log.Printf("fetchMPDWithBaseURLRetry: Max retries reached, BaseURL still mismatch for channel %s (%s). Proceeding with this last fetched MPD (BaseURL: '%s').", // Keep info on proceeding with mismatched data
				channelID, channelName, retryAttemptBaseURL)
			return retryFinalURL, retryMPDData, retryAttemptBaseURL, nil
		}
	}
	// This log indicates an unexpected state, so it should be kept.
	log.Printf("fetchMPDWithBaseURLRetry: Exited retry loop unexpectedly for channel %s (%s). Returning last known good MPD (BaseURL: '%s').",
		channelID, channelName, lastGoodActualBaseURL)
	return lastGoodFinalURL, lastGoodMPD, lastGoodActualBaseURL, nil
}

// generateMasterPlaylist generates the HLS master playlist string.
func generateMasterPlaylist(mpdData *mpd.MPD, channelID string) (string, error) {
	var playlist bytes.Buffer
	playlist.WriteString("#EXTM3U\n")
	playlist.WriteString("#EXT-X-VERSION:7\n") // Using a higher version for more features if needed

	audioGroupID := "audio_grp"
	subtitleGroupID := "subs_grp"
	audioStreamIndex := 0
	hasAudio := false
	hasSubtitles := false

	// First pass for #EXT-X-MEDIA:TYPE=AUDIO
	for _, period := range mpdData.Periods {
		for _, as := range period.AdaptationSets {
			if as.ContentType == "audio" || strings.Contains(as.MimeType, "audio") {
				if len(as.Representations) > 0 {
					// Use the first representation for codec info, or assume common AAC if not available
					repCodecs := ""
					if as.Representations[0].Codecs != "" {
						repCodecs = as.Representations[0].Codecs
					}

					lang := as.Lang
					if lang == "" {
						lang = fmt.Sprintf("audio%d", audioStreamIndex) // Default lang if not specified
					}
					name := lang
					// Attempt to get a more descriptive name if available
					// Removed as.Labels access as it's not in the current mpd.AdaptationSet struct
					if as.Lang != "" {
						name = fmt.Sprintf("Audio %s", as.Lang)
					} else {
						name = fmt.Sprintf("Audio %d", audioStreamIndex+1)
					}

					isDefault := "NO"
					if audioStreamIndex == 0 { // Make the first audio track default
						isDefault = "YES"
					}

					codecsString := ""
					if repCodecs != "" {
						codecsString = fmt.Sprintf(",CODECS=\"%s\"", repCodecs)
					}

					mediaPlaylistPath := fmt.Sprintf("/hls/%s/audio/%s/playlist.m3u8", channelID, lang)
					playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"%s\",NAME=\"%s\",LANGUAGE=\"%s\",AUTOSELECT=YES,DEFAULT=%s%s,URI=\"%s\"\n",
						audioGroupID, name, lang, isDefault, codecsString, mediaPlaylistPath))
					audioStreamIndex++
					hasAudio = true
				}
			}
		}
	}

	// Second pass for #EXT-X-MEDIA:TYPE=SUBTITLES
	subtitleStreamIndex := 0
	for _, period := range mpdData.Periods {
		for _, as := range period.AdaptationSets {
			// Adjusted condition to correctly identify subtitle tracks, including STPP
			if as.ContentType == "text" ||
				strings.Contains(as.MimeType, "text") ||
				strings.Contains(as.MimeType, "application/ttml+xml") ||
				(strings.Contains(as.MimeType, "application/mp4") && (strings.Contains(as.Representations[0].Codecs, "wvtt") || strings.Contains(as.Representations[0].Codecs, "stpp"))) {
				if len(as.Representations) > 0 {
					lang := as.Lang
					if lang == "" {
						lang = fmt.Sprintf("sub%d", subtitleStreamIndex)
					}
					name := lang
					// Removed as.Labels access
					if as.Lang != "" {
						name = fmt.Sprintf("Subtitles %s", as.Lang)
					} else {
						name = fmt.Sprintf("Subtitles %d", subtitleStreamIndex+1)
					}
					isDefault := "NO" // Subtitles usually not default unless only option or accessibility requirement

					mediaPlaylistPath := fmt.Sprintf("/hls/%s/subtitles/%s/playlist.m3u8", channelID, lang)
					playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"%s\",NAME=\"%s\",LANGUAGE=\"%s\",AUTOSELECT=YES,DEFAULT=%s,URI=\"%s\"\n",
						subtitleGroupID, name, lang, isDefault, mediaPlaylistPath))
					subtitleStreamIndex++
					hasSubtitles = true
				}
			}
		}
	}

	// Third pass for #EXT-X-STREAM-INF (video and associated audio/subtitles)
	for _, period := range mpdData.Periods {
		for _, as := range period.AdaptationSets {
			if as.ContentType == "video" || strings.Contains(as.MimeType, "video") {
				if len(as.Representations) == 0 {
					continue
				}

				// Find the representation with the highest bandwidth in this AdaptationSet
				var bestRep *mpd.Representation = nil
				var maxBandwidth uint // Changed to uint to match rep.Bandwidth

				for i := range as.Representations {
					rep := &as.Representations[i] // Use pointer to avoid copying
					if rep.Bandwidth > maxBandwidth {
						maxBandwidth = rep.Bandwidth
						bestRep = rep
					}
				}

				if bestRep != nil {
					rep := bestRep // Use the best representation found
					var streamInf strings.Builder
					streamInf.WriteString("#EXT-X-STREAM-INF:PROGRAM-ID=1") // PROGRAM-ID is optional but common

					if rep.Bandwidth > 0 {
						streamInf.WriteString(fmt.Sprintf(",BANDWIDTH=%d", rep.Bandwidth))
						streamInf.WriteString(fmt.Sprintf(",AVERAGE-BANDWIDTH=%d", rep.Bandwidth)) // AVERAGE-BANDWIDTH is optional
					}
					if rep.Width > 0 && rep.Height > 0 {
						streamInf.WriteString(fmt.Sprintf(",RESOLUTION=%dx%d", rep.Width, rep.Height))
					}
					if rep.Codecs != "" {
						streamInf.WriteString(fmt.Sprintf(",CODECS=\"%s\"", rep.Codecs))
					}
					// FRAME-RATE is optional
					if rep.FrameRate != "" {
						// MPD frame rate might be "25" or "30000/1001". HLS expects decimal.
						// This needs robust parsing if MPD FrameRate is fractional.
						// For simplicity, assuming it's a simple number or needs conversion.
						// streamInf.WriteString(fmt.Sprintf(",FRAME-RATE=%.3f", parseFrameRate(rep.FrameRate)))
					}

					if hasAudio {
						streamInf.WriteString(fmt.Sprintf(",AUDIO=\"%s\"", audioGroupID))
					}
					if hasSubtitles {
						streamInf.WriteString(fmt.Sprintf(",SUBTITLES=\"%s\"", subtitleGroupID))
					}
					playlist.WriteString(streamInf.String() + "\n")
					// Use rep.ID for the video media playlist path component
					videoMediaPlaylistPath := fmt.Sprintf("/hls/%s/video/%s/playlist.m3u8", channelID, rep.ID)
					playlist.WriteString(videoMediaPlaylistPath + "\n")
				}
			}
		}
	}
	return playlist.String(), nil
}

// generateMediaPlaylists generates all HLS media playlist strings.
func generateMediaPlaylists(mpdData *mpd.MPD, finalMPDURLStr string, channelID string, hlsBaseMediaSequence uint64, parsedKey []byte) (map[string]string, error) {
	playlists := make(map[string]string)
	// mpdBaseURL, err := url.Parse(finalMPDURLStr) // mpdBaseURL is not directly used for generating proxy-relative HLS URLs.
	// if err != nil {
	// 	log.Printf("generateMediaPlaylists: Error parsing finalMPDURLStr '%s': %v", finalMPDURLStr, err)
	// }

	for _, period := range mpdData.Periods {
		// Note: BaseURL resolution logic from segmentProxyHandler can be complex with multiple levels.
		// For playlist generation, we are creating HLS-proxy-relative URLs.
		// The `finalMPDURLStr` is mainly for context if needed, but segment URLs in HLS will be like /hls/...

		for _, as := range period.AdaptationSets {
			var streamType string
			if as.ContentType == "video" || strings.Contains(as.MimeType, "video") {
				streamType = "video"
			} else if as.ContentType == "audio" || strings.Contains(as.MimeType, "audio") {
				streamType = "audio"
				// Adjusted condition to correctly identify subtitle tracks for media playlist generation
			} else if as.ContentType == "text" ||
				strings.Contains(as.MimeType, "text") ||
				strings.Contains(as.MimeType, "application/ttml+xml") ||
				(strings.Contains(as.MimeType, "application/mp4") && len(as.Representations) > 0 && (strings.Contains(as.Representations[0].Codecs, "wvtt") || strings.Contains(as.Representations[0].Codecs, "stpp"))) {
				streamType = "subtitles"
			} else {
				continue // Skip unknown content types
			}

			for i, rep := range as.Representations {
				// Determine the quality/language identifier for the HLS URL and map key
				var qualityOrLangKey string
				if streamType == "video" {
					qualityOrLangKey = rep.ID
					if qualityOrLangKey == "" { // Fallback if RepresentationID is empty
						qualityOrLangKey = fmt.Sprintf("video_rep%d", i)
					}
				} else { // Audio or Subtitles
					qualityOrLangKey = as.Lang
					if qualityOrLangKey == "" { // Fallback if lang is empty
						if streamType == "audio" {
							qualityOrLangKey = fmt.Sprintf("audio%d", adaptacionSetAudioIndex(mpdData, &as))
						} else {
							qualityOrLangKey = fmt.Sprintf("sub%d", adaptaciónSetSubtitleIndex(mpdData, &as))
						}
					}
				}
				if qualityOrLangKey == "" { // Should not happen with fallbacks, but as a safeguard
					log.Printf("Warning: Empty qualityOrLangKey for AS contentType %s, lang %s, repID %s. Skipping.", as.ContentType, as.Lang, rep.ID)
					continue
				}

				playlistKey := fmt.Sprintf("%s/%s", streamType, qualityOrLangKey)
				var playlistBuf bytes.Buffer
				playlistBuf.WriteString("#EXTM3U\n")
				playlistBuf.WriteString("#EXT-X-VERSION:7\n") // Consistent with master

				// Determine SegmentTemplate (Representation or AdaptationSet level)
				segTemplate := rep.SegmentTemplate
				if segTemplate == nil {
					segTemplate = as.SegmentTemplate
				}

				if segTemplate == nil || segTemplate.SegmentTimeline == nil || len(segTemplate.SegmentTimeline.Segments) == 0 {
					log.Printf("generateMediaPlaylists: SegmentTemplate or SegmentTimeline missing/empty for %s in channel %s. Skipping playlist for key %s.", rep.ID, channelID, playlistKey)
					continue
				}

				timescale := uint64(1)
				if segTemplate.Timescale != nil {
					timescale = *segTemplate.Timescale
				}
				if timescale == 0 {
					timescale = 1 // Avoid division by zero
				}

				var maxSegDurSeconds float64 = 0
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
					for rIdx := 0; rIdx <= repeatCount; rIdx++ {
						segDurationSeconds := float64(s.D) / float64(timescale)
						if segDurationSeconds > maxSegDurSeconds {
							maxSegDurSeconds = segDurationSeconds
						}
						// HLS segment URL construction: /hls/<channelID>/<streamType>/<qualityOrLangKey>/<time_or_number>.[m4s|vtt]
						// The actual segment name for HLS is derived from its start time or a sequence number.
						// Using start time for now as it's directly available.
						segmentIdentifierForHLS := strconv.FormatUint(currentStartTime, 10)
						segmentFileExtension := ".m4s"
						if streamType == "subtitles" && IsSTPPTrack(&as) { // Check if it's an STPP track
							segmentFileExtension = ".vtt"
						}
						hlsSegmentURL := fmt.Sprintf("/hls/%s/%s/%s/%s%s", channelID, streamType, qualityOrLangKey, segmentIdentifierForHLS, segmentFileExtension)

						allSegments = append(allSegments, struct {
							StartTime, Duration uint64
							URL                 string
						}{
							StartTime: currentStartTime, Duration: s.D, URL: hlsSegmentURL, // Store the full HLS path
						})
						currentStartTime += s.D
					}
				}

				if len(allSegments) == 0 {
					log.Printf("generateMediaPlaylists: No segments generated for key %s in channel %s", playlistKey, channelID)
					continue // Skip this playlist if no segments
				}

				targetDuration := int(maxSegDurSeconds + 0.999) // Round up for target duration
				if targetDuration == 0 {
					targetDuration = 10 // Default target duration
				}
				playlistBuf.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", targetDuration))
				playlistBuf.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", hlsBaseMediaSequence))

				// EXT-X-MAP (Initialization Segment)
				// Omit MAP for STPP->WebVTT converted tracks as WebVTT doesn't use MP4 init segments
				if !(streamType == "subtitles" && IsSTPPTrack(&as)) {
					if segTemplate.Initialization != "" {
						// The HLS init segment URI will be proxied, so it needs a distinct name like "init.m4s"
						// For STPP (non-converted) or other MP4 based subtitles, it might still be init.m4s
						hlsInitSegmentName := "init.m4s"
						// If we were to support STPP as .mp4 segments directly in HLS (not converting to VTT),
						// this init segment name might need to be different or handled specifically.
						// But since all STPP is converted to VTT, this branch is for non-STPP or non-subtitle tracks.
						mapURI := fmt.Sprintf("/hls/%s/%s/%s/%s", channelID, streamType, qualityOrLangKey, hlsInitSegmentName)
						playlistBuf.WriteString(fmt.Sprintf("#EXT-X-MAP:URI=\"%s\"\n", mapURI))
					}
				}

				// EXT-X-KEY (Encryption)
				if len(parsedKey) > 0 && streamType != "subtitles" { // Typically, subtitles are not encrypted this way
					keyURI := fmt.Sprintf("/hls/%s/key", channelID)
					playlistBuf.WriteString(fmt.Sprintf("#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"%s\",KEYFORMAT=\"identity\"\n", keyURI))
				}

				// Segment list
				startIndex := 0
				if mpdData.Type == "dynamic" && len(allSegments) > numLiveSegments {
					startIndex = len(allSegments) - numLiveSegments
				}

				for k := startIndex; k < len(allSegments); k++ {
					seg := allSegments[k]
					segDurationSeconds := float64(seg.Duration) / float64(timescale)
					playlistBuf.WriteString(fmt.Sprintf("#EXTINF:%.3f,\n", segDurationSeconds))
					playlistBuf.WriteString(seg.URL + "\n") // Use the pre-constructed HLS path
				}

				if mpdData.Type == "static" {
					playlistBuf.WriteString("#EXT-X-ENDLIST\n")
				}
				playlists[playlistKey] = playlistBuf.String()
			}
		}
	}
	if len(playlists) == 0 {
		log.Printf("Warning: generateMediaPlaylists produced no playlists for channel %s", channelID)
	}
	return playlists, nil
}

// Helper functions to get a consistent index for default naming of audio/subtitle tracks
func adaptacionSetAudioIndex(mpdData *mpd.MPD, targetAs *mpd.AdaptationSet) int {
	idx := 0
	for _, p := range mpdData.Periods {
		for _, as := range p.AdaptationSets {
			if as.ContentType == "audio" || strings.Contains(as.MimeType, "audio") {
				if &as == targetAs { // Compare pointers
					return idx
				}
				idx++
			}
		}
	}
	return -1 // Should not happen if targetAs is from mpdData
}

func adaptaciónSetSubtitleIndex(mpdData *mpd.MPD, targetAs *mpd.AdaptationSet) int {
	idx := 0
	for _, p := range mpdData.Periods {
		for _, as := range p.AdaptationSets {
			// Adjusted condition to correctly identify subtitle tracks for indexing
			if as.ContentType == "text" ||
				strings.Contains(as.MimeType, "text") ||
				strings.Contains(as.MimeType, "application/ttml+xml") ||
				(strings.Contains(as.MimeType, "application/mp4") && (strings.Contains(as.Representations[0].Codecs, "wvtt") || strings.Contains(as.Representations[0].Codecs, "stpp"))) {
				if &as == targetAs { // Compare pointers
					return idx
				}
				idx++
			}
		}
	}
	return -1 // Should not happen
}
