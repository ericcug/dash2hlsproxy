package updater

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"dash2hlsproxy/internal/cache"
	"dash2hlsproxy/internal/config"
)

// manager is an interface that defines the methods the updater service needs
// to interact with the MPDManager. This avoids a circular dependency.
type manager interface {
	ForceUpdateChannel(ctx context.Context, channelID string) error
	GetCachedEntry(channelID string) (*cache.MPDEntry, bool)
	Config() *config.AppConfig
	DeleteEntry(channelID string)
}

// Service 负责后台任务，如自动更新和清理
type Service struct {
	Config  *config.AppConfig
	Logger  *slog.Logger
	Manager manager
	Cache   *cache.Manager // Keep a direct reference to Cache for janitor
}

// NewService 创建一个新的 Updater 服务
func NewService(cfg *config.AppConfig, logger *slog.Logger, m manager) *Service {
	type cacheProvider interface {
		GetCache() *cache.Manager
	}
	cacheManager, _ := m.(cacheProvider)

	return &Service{
		Config:  cfg,
		Logger:  logger,
		Manager: m,
		Cache:   cacheManager.GetCache(),
	}
}

// StartJanitor 启动一个定期清理过期缓存的协程
func (s *Service) StartJanitor(ctx context.Context) {
	s.Logger.Info("Starting janitor...")
	// Use a configurable interval later if needed
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
	if s.Cache == nil {
		s.Logger.Error("Janitor: Cache is not initialized, cannot run cleanup.")
		return
	}

	s.Cache.CacheLock.RLock()
	allChannelIDs := make([]string, 0, len(s.Cache.MPDCache))
	for channelID := range s.Cache.MPDCache {
		allChannelIDs = append(allChannelIDs, channelID)
	}
	s.Cache.CacheLock.RUnlock()

	timeoutMinutes := time.Duration(s.Manager.Config().ChannelCacheTimeoutMinutes) * time.Minute
	if timeoutMinutes <= 0 {
		timeoutMinutes = 1440 * time.Minute // Default to 24 hours if not set
	}

	for _, channelID := range allChannelIDs {
		cachedEntry, exists := s.Manager.GetCachedEntry(channelID)
		if !exists {
			continue
		}

		cachedEntry.Mux.Lock()

		// 1. Evict entire channel entry if it hasn't been accessed in a long time
		if time.Since(cachedEntry.LastAccessedAt) > timeoutMinutes {
			s.Logger.Info("Channel cache timed out. Evicting entry.", "channel_id", channelID, "last_accessed", cachedEntry.LastAccessedAt)
			// Ensure auto-updater is stopped before evicting
			if cachedEntry.StopAutoUpdateCh != nil {
				close(cachedEntry.StopAutoUpdateCh)
				cachedEntry.StopAutoUpdateCh = nil
			}
			cachedEntry.Mux.Unlock() // Unlock before calling DeleteEntry to avoid deadlock
			s.Manager.DeleteEntry(channelID)
			continue // Move to the next channel
		}

		// 2. For active dynamic channels, stop auto-updater if inactive for a shorter period
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
					cachedEntry.StopAutoUpdateCh = nil // Mark as stopped
				}
			}
		}

		cachedEntry.Mux.Unlock()

		// 3. Cleanup expired segments for the entry
		s.cleanupExpiredSegments(channelID, cachedEntry)
	}
}

func (s *Service) cleanupExpiredSegments(channelID string, cachedEntry *cache.MPDEntry) {
	const maxSegmentAge = 5 * time.Minute

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
		if _, isValid := validSegments[key]; !isValid && time.Since(segment.FetchedAt) > maxSegmentAge {
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
func (s *Service) StartAutoUpdater(channelID string, entry *cache.MPDEntry, initialMinUpdatePeriod time.Duration) {
	s.Logger.Info("Starting AutoUpdater for dynamic channel", "channel_id", channelID, "update_period", initialMinUpdatePeriod)
	entry.StopAutoUpdateCh = make(chan struct{})

	go func() {
		ticker := time.NewTicker(initialMinUpdatePeriod)
		defer ticker.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

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
				err := s.Manager.ForceUpdateChannel(ctx, channelID)
				if err != nil {
					s.Logger.Error("AutoUpdater: Error updating channel data. Will retry on next tick.", "channel_id", channelID, "error", err)
					continue
				}

				entry.Mux.RLock()
				if entry.Data == nil {
					entry.Mux.RUnlock()
					continue
				}
				newMup, mupErr := entry.Data.GetMinimumUpdatePeriod()
				entry.Mux.RUnlock()

				if mupErr == nil && newMup > 0 && newMup != initialMinUpdatePeriod {
					ticker.Reset(newMup)
					initialMinUpdatePeriod = newMup
					s.Logger.Info("AutoUpdater: Updated refresh interval", "channel_id", channelID, "new_period", newMup)
				}

			case <-ctx.Done():
				s.Logger.Info("AutoUpdater: Stop signal received. Exiting.", "channel_id", channelID)
				return
			}
		}
	}()
}
