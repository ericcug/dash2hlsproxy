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
	Config     *config.AppConfig
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
	updaterSvc := updater.NewService(cfg, logger, cacheManager, fetcher, dl)

	m := &MPDManager{
		Config:     cfg,
		Logger:     logger,
		Cache:      cacheManager,
		Fetcher:    fetcher,
		Downloader: dl,
		Updater:    updaterSvc,
	}
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

// GetMPD 获取指定频道的 MPD。它会处理缓存、获取和更新逻辑。
func (m *MPDManager) GetMPD(ctx context.Context, channelCfg *config.ChannelConfig) (string, *mpd.MPD, error) {
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

	entry := m.Cache.GetOrCreateEntry(channelCfg.ID)

	entry.Mux.Lock()
	if entry.Data != nil {
		dataCopy := *entry.Data
		finalURLCopy := entry.FinalMPDURL
		entry.Mux.Unlock()
		m.Logger.Debug("MPD found in cache after acquiring lock", "channel_id", channelCfg.ID)
		return finalURLCopy, &dataCopy, nil
	}
	entry.Mux.Unlock()

	finalURL, mpdData, err := m.fetchAndProcessMPD(ctx, channelCfg, entry)
	if err != nil {
		m.Cache.DeleteEntry(channelCfg.ID)
		return "", nil, err
	}

	entry.Mux.RLock()
	isDynamic := entry.Data.Type == "dynamic"
	isUpdaterRunning := entry.StopAutoUpdateCh != nil
	entry.Mux.RUnlock()

	if isDynamic && !isUpdaterRunning {
		entry.Mux.Lock()
		if entry.StopAutoUpdateCh == nil {
			minUpdatePeriod, mupErr := entry.Data.GetMinimumUpdatePeriod()
			if mupErr == nil && minUpdatePeriod > 0 {
				m.Updater.StartAutoUpdater(channelCfg, entry, minUpdatePeriod)
			}
		}
		entry.Mux.Unlock()
	}

	return finalURL, mpdData, nil
}

func (m *MPDManager) fetchAndProcessMPD(ctx context.Context, channelCfg *config.ChannelConfig, entry *cache.MPDEntry) (string, *mpd.MPD, error) {
	entry.Mux.RLock()
	urlToFetch := channelCfg.Manifest
	if entry.FinalMPDURL != "" {
		urlToFetch = entry.FinalMPDURL
	}
	initialBaseURL := entry.InitialBaseURL
	initialBaseURLIsSet := entry.InitialBaseURLIsSet
	entry.Mux.RUnlock()

	newFinalURL, fetchedBaseURL, newMPDData, err := m.Fetcher.FetchMPDWithRetry(
		ctx, urlToFetch, m.Config.UserAgent,
		initialBaseURL, initialBaseURLIsSet,
	)
	if err != nil {
		return "", nil, fmt.Errorf("error fetching/retrying MPD: %w", err)
	}

	entry.Mux.Lock()
	defer entry.Mux.Unlock()

	if entry.Data != nil {
		newPublishTime, _ := fetch.ParsePublishTime(newMPDData.PublishTime)
		cachedPublishTime, _ := fetch.ParsePublishTime(entry.Data.PublishTime)
		if !newPublishTime.IsZero() && !cachedPublishTime.IsZero() && !newPublishTime.After(cachedPublishTime) {
			dataCopy := *entry.Data
			finalURLCopy := entry.FinalMPDURL
			return finalURLCopy, &dataCopy, nil
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
	return newFinalURL, &dataCopy, nil
}

func (m *MPDManager) processMPDUpdates(channelCfg *config.ChannelConfig, entry *cache.MPDEntry) {
	masterPl, selectedRepIDs, err := playlist.GenerateMasterPlaylist(entry.Data, channelCfg.ID)
	if err != nil {
		m.Logger.Error("Error generating master playlist", "channel_id", channelCfg.ID, "error", err)
		return
	}
	entry.MasterPlaylist = masterPl

	mediaPls, segmentsToPreload, validSegments, err := playlist.GenerateMediaPlaylists(m.Logger, entry.Data, entry.FinalMPDURL, channelCfg.ID, entry.HLSBaseMediaSequence, channelCfg.ParsedKey, selectedRepIDs)
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
					UserAgent:   m.Config.UserAgent,
				})
			}
		}
	}
}

func (m *MPDManager) updateSequence(entry *cache.MPDEntry) {
	newPublishTime, ptErr := fetch.ParsePublishTime(entry.Data.PublishTime)
	if ptErr == nil {
		if entry.LastMPDPublishTime.IsZero() || newPublishTime.After(entry.LastMPDPublishTime) {
			if !entry.LastMPDPublishTime.IsZero() {
				entry.HLSBaseMediaSequence++
			}
			entry.LastMPDPublishTime = newPublishTime
		}
	} else {
		m.Logger.Warn("Could not parse PublishTime", "publish_time", entry.Data.PublishTime, "error", ptErr)
	}
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
					// This now indicates that the download failed or was cancelled,
					// as the downloader signals completion regardless of the outcome.
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
			// If the segment is not in the cache and there's no download signal,
			// it implies that the segment was expected to be there but wasn't,
			// or the signal was never created. This is a logic error.
			// However, to prevent a fatal error for a client, we can log it
			// and return an error, making the system more robust.
			m.Logger.Error("No download signal found for a segment not in cache", "segment_key", key, "channel_id", channelID)
			return fmt.Errorf("segment %s is not available and not being downloaded", key)
		}
	}
	return nil
}
