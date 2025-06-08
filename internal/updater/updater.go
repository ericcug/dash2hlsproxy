package updater

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"dash2hlsproxy/internal/cache"
	"dash2hlsproxy/internal/config"
	"dash2hlsproxy/internal/fetch"
	"dash2hlsproxy/internal/mpd"
	"dash2hlsproxy/internal/playlist"
)

// Service 负责后台任务，如自动更新和清理
type Service struct {
	Config     *config.AppConfig
	Logger     *slog.Logger
	Cache      *cache.Manager
	Fetcher    *fetch.Fetcher
	Downloader interface {
		EnqueueTask(*fetch.SegmentDownloadTask)
	}
}

// NewService 创建一个新的 Updater 服务
func NewService(cfg *config.AppConfig, logger *slog.Logger, cache *cache.Manager, fetcher *fetch.Fetcher, downloader interface {
	EnqueueTask(*fetch.SegmentDownloadTask)
}) *Service {
	return &Service{
		Config:     cfg,
		Logger:     logger,
		Cache:      cache,
		Fetcher:    fetcher,
		Downloader: downloader,
	}
}

// StartJanitor 启动一个定期清理过期缓存的协程
func (s *Service) StartJanitor(ctx context.Context) {
	s.Logger.Info("Starting janitor...")
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.runJanitor()
			case <-ctx.Done():
				s.Logger.Info("Stopping janitor.")
				return
			}
		}
	}()
}

func (s *Service) runJanitor() {
	const maxSegmentAge = 5 * time.Minute

	s.Cache.CacheLock.RLock()
	defer s.Cache.CacheLock.RUnlock()

	for channelID, cachedEntry := range s.Cache.MPDCache {
		// 1. 为不活跃的动态频道停止自动更新
		cachedEntry.Mux.Lock()
		if cachedEntry.Data != nil && cachedEntry.Data.Type == "dynamic" && cachedEntry.StopAutoUpdateCh != nil {
			minUpdatePeriod, err := cachedEntry.Data.GetMinimumUpdatePeriod()
			if err == nil {
				if minUpdatePeriod == 0 {
					minUpdatePeriod = 5 * time.Second
				}
				inactivityThreshold := 5 * minUpdatePeriod
				if time.Since(cachedEntry.LastAccessedAt) > inactivityThreshold {
					s.Logger.Info("Channel inactive, stopping auto-updater.", "channel_id", channelID)
					close(cachedEntry.StopAutoUpdateCh)
					cachedEntry.StopAutoUpdateCh = nil // 标记为已停止
				}
			}
		}
		cachedEntry.Mux.Unlock()

		// 2. 清理过期的分片
		s.cleanupExpiredSegments(channelID, cachedEntry, maxSegmentAge)
	}
}

func (s *Service) cleanupExpiredSegments(channelID string, cachedEntry *cache.MPDEntry, maxAge time.Duration) {
	cachedEntry.Mux.RLock()
	playlists := make([]string, 0, len(cachedEntry.MediaPlaylists))
	for _, pl := range cachedEntry.MediaPlaylists {
		playlists = append(playlists, pl)
	}
	segmentCache := cachedEntry.SegmentCache
	cachedEntry.Mux.RUnlock()

	if segmentCache == nil {
		return
	}

	validSegments := make(map[string]struct{})
	for _, pl := range playlists {
		lines := strings.Split(pl, "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "/hls/") {
				validSegments[line] = struct{}{}
			}
		}
	}

	segmentCache.RLock()
	initialCount := len(segmentCache.Segments)
	keysToDelete := make([]string, 0)
	for key, segment := range segmentCache.Segments {
		if _, isValid := validSegments[key]; !isValid && time.Since(segment.FetchedAt) > maxAge {
			keysToDelete = append(keysToDelete, key)
		}
	}
	segmentCache.RUnlock()

	if len(keysToDelete) > 0 {
		segmentCache.Lock()
		for _, key := range keysToDelete {
			delete(segmentCache.Segments, key)
		}
		segmentCache.Unlock()
		finalCount := initialCount - len(keysToDelete)
		s.Logger.Info("Segment cache cleanup finished", "channel_id", channelID, "removed_count", len(keysToDelete), "initial_count", initialCount, "final_count", finalCount)
	}
}

// StartAutoUpdater 启动一个自动更新 MPD 的协程
func (s *Service) StartAutoUpdater(channelCfg *config.ChannelConfig, entry *cache.MPDEntry, initialMinUpdatePeriod time.Duration) {
	s.Logger.Info("Starting AutoUpdater for dynamic channel", "channel_id", channelCfg.ID, "update_period", initialMinUpdatePeriod)
	entry.StopAutoUpdateCh = make(chan struct{})

	go func() {
		ticker := time.NewTicker(initialMinUpdatePeriod)
		defer ticker.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// 监听停止信号
		go func() {
			select {
			case <-entry.StopAutoUpdateCh:
				cancel()
			case <-ctx.Done():
			}
		}()

		for {
			select {
			case <-ticker.C:
				_, _, err := s.fetchAndProcessMPD(ctx, channelCfg, entry)
				if err != nil {
					s.Logger.Error("AutoUpdater: Error fetching/processing MPD. Will retry on next tick.", "error", err)
					continue
				}

				entry.Mux.RLock()
				newMup, mupErr := entry.Data.GetMinimumUpdatePeriod()
				entry.Mux.RUnlock()

				if mupErr == nil && newMup > 0 && newMup != initialMinUpdatePeriod {
					ticker.Reset(newMup)
					initialMinUpdatePeriod = newMup
					s.Logger.Info("AutoUpdater: Updated refresh interval", "channel_id", channelCfg.ID, "new_period", newMup)
				}

			case <-ctx.Done():
				s.Logger.Info("AutoUpdater: Stop signal received. Exiting.", "channel_id", channelCfg.ID)
				return
			}
		}
	}()
}

// fetchAndProcessMPD 是 updater 内部的方法，用于获取和处理 MPD 更新
func (s *Service) fetchAndProcessMPD(ctx context.Context, channelCfg *config.ChannelConfig, entry *cache.MPDEntry) (string, *mpd.MPD, error) {
	entry.Mux.RLock()
	urlToFetch := channelCfg.Manifest
	if entry.FinalMPDURL != "" {
		urlToFetch = entry.FinalMPDURL
	}
	initialBaseURL := entry.InitialBaseURL
	initialBaseURLIsSet := entry.InitialBaseURLIsSet
	entry.Mux.RUnlock()

	newFinalURL, fetchedBaseURL, newMPDData, err := s.Fetcher.FetchMPDWithRetry(
		ctx, urlToFetch, s.Config.UserAgent,
		initialBaseURL, initialBaseURLIsSet,
	)
	if err != nil {
		return "", nil, err
	}

	entry.Mux.Lock()
	defer entry.Mux.Unlock()

	if entry.Data != nil {
		newPublishTime, _ := fetch.ParsePublishTime(newMPDData.PublishTime)
		cachedPublishTime, _ := fetch.ParsePublishTime(entry.Data.PublishTime)
		if !newPublishTime.IsZero() && !cachedPublishTime.IsZero() && !newPublishTime.After(cachedPublishTime) {
			return entry.FinalMPDURL, entry.Data, nil
		}
	}

	entry.Data = newMPDData
	entry.FetchedAt = time.Now()
	entry.FinalMPDURL = newFinalURL
	if !entry.InitialBaseURLIsSet {
		entry.InitialBaseURL = fetchedBaseURL
		entry.InitialBaseURLIsSet = true
	}

	s.updateSequence(entry)
	s.processMPDUpdates(channelCfg, entry)

	return newFinalURL, newMPDData, nil
}

func (s *Service) processMPDUpdates(channelCfg *config.ChannelConfig, entry *cache.MPDEntry) {
	masterPl, selectedRepIDs, err := playlist.GenerateMasterPlaylist(entry.Data, channelCfg.ID)
	if err != nil {
		s.Logger.Error("Error generating master playlist", "channel_id", channelCfg.ID, "error", err)
		return
	}
	entry.MasterPlaylist = masterPl

	mediaPls, segmentsToPreload, _, err := playlist.GenerateMediaPlaylists(s.Logger, entry.Data, entry.FinalMPDURL, channelCfg.ID, entry.HLSBaseMediaSequence, channelCfg.ParsedKey, selectedRepIDs)
	if err != nil {
		s.Logger.Error("Error generating media playlists", "channel_id", channelCfg.ID, "error", err)
	} else {
		entry.MediaPlaylists = mediaPls
		for segmentKey, upstreamURL := range segmentsToPreload {
			if !entry.SegmentCache.Has(segmentKey) {
				entry.SegmentDownloadSignals.LoadOrStore(segmentKey, make(chan struct{}))
				s.Downloader.EnqueueTask(&fetch.SegmentDownloadTask{
					ChannelID:   channelCfg.ID,
					SegmentKey:  segmentKey,
					UpstreamURL: upstreamURL,
					UserAgent:   s.Config.UserAgent,
				})
			}
		}
	}
}

func (s *Service) updateSequence(entry *cache.MPDEntry) {
	newPublishTime, ptErr := fetch.ParsePublishTime(entry.Data.PublishTime)
	if ptErr == nil {
		if entry.LastMPDPublishTime.IsZero() || newPublishTime.After(entry.LastMPDPublishTime) {
			if !entry.LastMPDPublishTime.IsZero() {
				entry.HLSBaseMediaSequence++
			}
			entry.LastMPDPublishTime = newPublishTime
		}
	} else {
		s.Logger.Warn("Could not parse PublishTime", "publish_time", entry.Data.PublishTime, "error", ptErr)
	}
}
