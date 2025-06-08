package mpd_manager

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"dash2hlsproxy/internal/cache"
	"dash2hlsproxy/internal/config"
	"dash2hlsproxy/internal/downloader"
	"dash2hlsproxy/internal/fetch"
	"dash2hlsproxy/internal/mpd"
	"dash2hlsproxy/internal/playlist"
	"dash2hlsproxy/internal/updater"
)

// MPDManager 协调所有与 MPD 和 HLS 相关的操作
type MPDManager struct {
	config     *config.AppConfig // Renamed to lowercase
	Logger     *slog.Logger
	Cache      *cache.Manager
	Fetcher    *fetch.Fetcher
	Downloader *downloader.Downloader
	Updater    *updater.Service
}

// NewMPDManager 创建并初始化一个新的 MPDManager
func NewMPDManager(cfg *config.AppConfig, logger *slog.Logger, httpClient *http.Client) *MPDManager {
	cacheManager := cache.NewManager()
	fetcher := fetch.NewFetcher(logger, httpClient)
	dl := downloader.NewDownloader(logger, cacheManager, fetcher)

	m := &MPDManager{
		config:     cfg, // Use lowercase field
		Logger:     logger,
		Cache:      cacheManager,
		Fetcher:    fetcher,
		Downloader: dl,
	}

	// Updater needs a reference to the manager to call back for updates.
	updaterSvc := updater.NewService(cfg, logger, m)
	m.Updater = updaterSvc

	return m
}

// GetCachedEntry 是一个辅助方法，用于获取缓存条目（主要由内部使用）
func (m *MPDManager) GetCachedEntry(channelID string) (*cache.MPDEntry, bool) {
	entry, exists := m.Cache.GetEntry(channelID)
	if !exists {
		return nil, false
	}
	return entry.(*cache.MPDEntry), true
}

// Config returns the application configuration.
func (m *MPDManager) Config() *config.AppConfig {
	return m.config
}

// GetCache returns the cache manager.
func (m *MPDManager) GetCache() *cache.Manager {
	return m.Cache
}

// DeleteEntry removes an entry from the cache.
func (m *MPDManager) DeleteEntry(channelID string) {
	m.Cache.DeleteEntry(channelID)
}

// GetMPD 获取指定频道的 MPD。它会处理缓存、获取和更新逻辑。
func (m *MPDManager) GetMPD(ctx context.Context, channelCfg *config.ChannelConfig) (string, *mpd.MPD, error) {
	// First, try a quick read-lock to see if data is already available.
	if cachedEntry, exists := m.GetCachedEntry(channelCfg.ID); exists {
		cachedEntry.Mux.RLock()
		if entryData := cachedEntry.Data; entryData != nil {
			dataCopy := *entryData
			finalURLCopy := cachedEntry.FinalMPDURL
			cachedEntry.Mux.RUnlock()
			m.Logger.Debug("MPD found in cache", "channel_id", channelCfg.ID)
			return finalURLCopy, &dataCopy, nil
		}
		cachedEntry.Mux.RUnlock()
	}

	// If not found, get or create an entry and acquire a full lock.
	entry := m.Cache.GetOrCreateEntry(channelCfg.ID)
	entry.Mux.Lock()
	defer entry.Mux.Unlock()

	// Double-check if another goroutine populated the data while we were waiting for the lock.
	if entry.Data != nil {
		dataCopy := *entry.Data
		finalURLCopy := entry.FinalMPDURL
		m.Logger.Debug("MPD found in cache after acquiring lock", "channel_id", channelCfg.ID)
		return finalURLCopy, &dataCopy, nil
	}

	// The data is truly missing, so we fetch it.
	mpdData, err := m.updateChannelData(ctx, channelCfg, entry)
	if err != nil {
		// If fetching fails, remove the potentially corrupted entry to ensure a clean slate for the next attempt.
		m.Cache.DeleteEntry(channelCfg.ID)
		return "", nil, err
	}

	// Start the auto-updater for dynamic streams if it's not already running.
	if entry.Data.Type == "dynamic" && entry.StopAutoUpdateCh == nil {
		minUpdatePeriod, mupErr := entry.Data.GetMinimumUpdatePeriod()
		if mupErr == nil && minUpdatePeriod > 0 {
			m.Updater.StartAutoUpdater(channelCfg.ID, entry, minUpdatePeriod)
		}
	}

	return entry.FinalMPDURL, mpdData, nil
}

// updateChannelData is the core reusable function for fetching, processing, and updating channel data.
// It is NOT thread-safe and expects the caller to handle locking on the entry.
func (m *MPDManager) updateChannelData(ctx context.Context, channelCfg *config.ChannelConfig, entry *cache.MPDEntry) (*mpd.MPD, error) {
	urlToFetch := channelCfg.Manifest
	if entry.FinalMPDURL != "" {
		urlToFetch = entry.FinalMPDURL
	}

	newFinalURL, fetchedBaseURL, newMPDData, err := m.Fetcher.FetchMPDWithRetry(
		ctx, urlToFetch, m.config.UserAgent, // Use lowercase field
		entry.InitialBaseURL, entry.InitialBaseURLIsSet,
	)
	if err != nil {
		return nil, fmt.Errorf("error fetching/retrying MPD: %w", err)
	}

	if entry.Data != nil {
		newPublishTime, _ := fetch.ParsePublishTime(newMPDData.PublishTime)
		cachedPublishTime, _ := fetch.ParsePublishTime(entry.Data.PublishTime)
		if !newPublishTime.IsZero() && !cachedPublishTime.IsZero() && !newPublishTime.After(cachedPublishTime) {
			return entry.Data, nil // Cached data is newer or same, no update needed.
		}
	}

	entry.Data = newMPDData
	entry.FetchedAt = time.Now()
	entry.FinalMPDURL = newFinalURL
	if !entry.InitialBaseURLIsSet {
		entry.InitialBaseURL = fetchedBaseURL
		entry.InitialBaseURLIsSet = true
	}

	m.updateSequence(entry)
	m.processMPDUpdates(channelCfg, entry)

	dataCopy := *newMPDData
	return &dataCopy, nil
}

// ForceUpdateChannel is called by the updater to refresh a channel's MPD.
func (m *MPDManager) ForceUpdateChannel(ctx context.Context, channelID string) error {
	channelCfg, ok := m.config.ChannelMap[channelID] // Use lowercase field
	if !ok {
		return fmt.Errorf("channel config for %s not found", channelID)
	}

	entry, exists := m.GetCachedEntry(channelID)
	if !exists {
		// This can happen if the entry is evicted by the janitor between ticks. It's not a critical error.
		m.Logger.Info("Cached entry for channel not found during update, skipping.", "channel_id", channelID)
		return nil
	}

	entry.Mux.Lock()
	defer entry.Mux.Unlock()

	_, err := m.updateChannelData(ctx, channelCfg, entry)
	return err
}

func (m *MPDManager) processMPDUpdates(channelCfg *config.ChannelConfig, entry *cache.MPDEntry) {
	masterPl, selectedRepIDs, err := playlist.GenerateMasterPlaylist(entry.Data, channelCfg.ID)
	if err != nil {
		m.Logger.Error("Error generating master playlist", "channel_id", channelCfg.ID, "error", err)
		return
	}
	entry.MasterPlaylist = masterPl

	livePlaylistDuration := m.config.LivePlaylistDuration // Use lowercase field
	if livePlaylistDuration <= 0 {
		livePlaylistDuration = 30 // Fallback to a default value of 30 seconds
	}
	mediaPls, segmentsToPreload, validSegments, err := playlist.GenerateMediaPlaylists(m.Logger, entry.Data, entry.FinalMPDURL, channelCfg.ID, entry.HLSBaseMediaSequence, channelCfg.ParsedKey, selectedRepIDs, livePlaylistDuration)
	if err != nil {
		m.Logger.Error("Error generating media playlists", "channel_id", channelCfg.ID, "error", err)
	} else {
		entry.MediaPlaylists = mediaPls
		entry.PruneSegments(validSegments) // Prune old segments
		for segmentKey, upstreamURL := range segmentsToPreload {
			if !entry.SegmentCache.Has(segmentKey) {
				entry.SegmentDownloadSignals.LoadOrStore(segmentKey, make(chan struct{}))
				m.Downloader.EnqueueTask(&fetch.SegmentDownloadTask{
					ChannelID:   channelCfg.ID,
					SegmentKey:  segmentKey,
					UpstreamURL: upstreamURL,
					UserAgent:   m.config.UserAgent, // Use lowercase field
				})
			}
		}
	}
}

func (m *MPDManager) updateSequence(entry *cache.MPDEntry) {
	newPublishTime, ptErr := fetch.ParsePublishTime(entry.Data.PublishTime)
	if ptErr != nil {
		m.Logger.Warn("Could not parse PublishTime, sequence not updated", "publish_time", entry.Data.PublishTime, "error", ptErr)
		return
	}

	if entry.LastMPDPublishTime.IsZero() {
		entry.LastMPDPublishTime = newPublishTime
		entry.LastSegmentTimes = collectSegmentTimes(entry.Data)
		return
	}

	if !newPublishTime.After(entry.LastMPDPublishTime) {
		return
	}

	newSegmentTimes := collectSegmentTimes(entry.Data)
	hasNewSegments := false
	for timeKey := range newSegmentTimes {
		if _, exists := entry.LastSegmentTimes[timeKey]; !exists {
			hasNewSegments = true
			break
		}
	}

	if hasNewSegments {
		m.Logger.Debug("New segments detected, incrementing HLS base media sequence.", "channel_id", entry.Data.Periods[0].ID)
		entry.HLSBaseMediaSequence++
	}

	entry.LastMPDPublishTime = newPublishTime
	entry.LastSegmentTimes = newSegmentTimes
}

func collectSegmentTimes(mpdData *mpd.MPD) map[uint64]struct{} {
	times := make(map[uint64]struct{})
	if mpdData == nil {
		return times
	}

	for _, period := range mpdData.Periods {
		for _, as := range period.AdaptationSets {
			segTemplate := as.SegmentTemplate
			for _, rep := range as.Representations {
				if rep.SegmentTemplate != nil {
					segTemplate = rep.SegmentTemplate
				}

				if segTemplate != nil && segTemplate.SegmentTimeline != nil {
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
							times[currentStartTime] = struct{}{}
							currentStartTime += s.D
						}
					}
				}
			}
		}
	}
	return times
}

func (m *MPDManager) WaitForSegments(requestCtx context.Context, channelID string, segmentKeys []string, timeout time.Duration) error {
	cachedEntry, exists := m.GetCachedEntry(channelID)
	if !exists {
		m.Logger.Error("Channel entry not found in cache during WaitForSegments", "channel_id", channelID)
		return fmt.Errorf("channel entry %s not found", channelID)
	}

	ctx, cancel := context.WithTimeout(requestCtx, timeout)
	defer cancel()

	for _, key := range segmentKeys {
		if cachedEntry.SegmentCache.Has(key) {
			continue
		}

		if sig, ok := cachedEntry.SegmentDownloadSignals.Load(key); ok {
			select {
			case <-sig.(chan struct{}):
				if !cachedEntry.SegmentCache.Has(key) {
					err := fmt.Errorf("segment %s download failed or was cancelled", key)
					m.Logger.Warn("WaitForSegments: detected failed download", "segment_key", key, "channel_id", channelID)
					return err
				}
			case <-ctx.Done():
				err := fmt.Errorf("timed out waiting for segment %s", key)
				m.Logger.Error("WaitForSegments: timed out", "segment_key", key, "channel_id", channelID, "timeout", timeout)
				return err
			}
		} else {
			const maxRetries = 3
			const retryDelay = 50 * time.Millisecond
			found := false
			for i := 0; i < maxRetries; i++ {
				time.Sleep(retryDelay)
				if sig, ok := cachedEntry.SegmentDownloadSignals.Load(key); ok {
					select {
					case <-sig.(chan struct{}):
						if !cachedEntry.SegmentCache.Has(key) {
							return fmt.Errorf("segment %s download failed or was cancelled after retry", key)
						}
						found = true
					case <-ctx.Done():
						return fmt.Errorf("timed out waiting for segment %s after retry", key)
					}
					break
				}
			}

			if !found {
				m.Logger.Error("No download signal found for a segment not in cache after retries", "segment_key", key, "channel_id", channelID)
				return fmt.Errorf("segment %s is not available and not being downloaded", key)
			}
		}
	}
	return nil
}
