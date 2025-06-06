package mpd_manager

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"dash2hlsproxy/internal/config"
	"dash2hlsproxy/internal/mpd"
	"dash2hlsproxy/internal/playlist"
)

// 在实时媒体播放列表中保留的段数
const numLiveSegments = 5

// 全局最大并发下载数
const globalMaxConcurrentDownloads = 10

// --- New Downloader Component ---

// SegmentDownloadTask 定义一个 SEGMENT 下载任务
type SegmentDownloadTask struct {
	ChannelID   string
	SegmentKey  string // 缓存键: /hls/{channelID}/{streamType}/{qualityOrLang}/{segmentName}
	UpstreamURL string
	UserAgent   string
}

// Downloader manages segment downloads.
type Downloader struct {
	manager         *MPDManager
	httpClient      *http.Client
	downloadQueue   chan *SegmentDownloadTask
	activeDownloads chan struct{}
	stopCh          chan struct{}
	wg              sync.WaitGroup
}

// NewDownloader 创建一个新的 Downloader 实例
func NewDownloader(m *MPDManager, client *http.Client) *Downloader {
	return &Downloader{
		manager:         m,
		httpClient:      client,
		downloadQueue:   make(chan *SegmentDownloadTask, 200), // Increased buffer
		activeDownloads: make(chan struct{}, globalMaxConcurrentDownloads),
		stopCh:          make(chan struct{}),
	}
}

// Start 启动下载器的 worker 协程
func (d *Downloader) Start(ctx context.Context) {
	for i := 0; i < globalMaxConcurrentDownloads; i++ {
		d.wg.Add(1)
		go d.worker(ctx)
	}
	slog.Info("Downloader: Started worker goroutines", "count", globalMaxConcurrentDownloads)
}

// Stop 停止下载器
func (d *Downloader) Stop() {
	close(d.stopCh)
	d.wg.Wait()
	slog.Info("Downloader: Stopped.")
}

// EnqueueTask 将下载任务加入队列
func (d *Downloader) EnqueueTask(task *SegmentDownloadTask) {
	select {
	case d.downloadQueue <- task:
		slog.Debug("Downloader: Added segment to download queue", "segment_key", task.SegmentKey, "channel_id", task.ChannelID)
	default:
		slog.Warn("Downloader: Download queue is full. Dropping segment.", "segment_key", task.SegmentKey, "channel_id", task.ChannelID)
	}
}

// worker 协程从下载队列中获取任务并执行下载
func (d *Downloader) worker(ctx context.Context) {
	defer d.wg.Done()
	for {
		select {
		case task := <-d.downloadQueue:
			d.activeDownloads <- struct{}{} // Acquire a slot

			cachedEntry, exists := d.manager.GetCachedEntry(task.ChannelID)
			if !exists {
				slog.Warn("Downloader worker: Channel entry not found. Skipping download.", "channel_id", task.ChannelID, "segment_key", task.SegmentKey)
				d.signalDownloadCompletion(task.ChannelID, task.SegmentKey)
				<-d.activeDownloads // Release slot
				continue
			}

			if cachedEntry.SegmentCache.Has(task.SegmentKey) {
				d.signalDownloadCompletion(task.ChannelID, task.SegmentKey)
				<-d.activeDownloads // Release slot
				continue
			}

			data, contentType, err := d.fetchSegment(task.UpstreamURL, task.UserAgent)

			if err != nil {
				slog.Error("Downloader worker: Failed to download segment", "segment_key", task.SegmentKey, "channel_id", task.ChannelID, "error", err)
			} else {
				cachedEntry.SegmentCache.Set(task.SegmentKey, &CachedSegment{
					Data:        data,
					ContentType: contentType,
					FetchedAt:   time.Now(),
				})
				slog.Debug("Downloader worker: Successfully cached segment", "segment_key", task.SegmentKey, "channel_id", task.ChannelID)
			}

			d.signalDownloadCompletion(task.ChannelID, task.SegmentKey)
			<-d.activeDownloads // Release slot

		case <-d.stopCh:
			slog.Info("Downloader worker: Stop signal received. Exiting.")
			return
		case <-ctx.Done():
			slog.Info("Downloader worker: Context cancelled. Exiting.")
			return
		}
	}
}

func (d *Downloader) signalDownloadCompletion(channelID, segmentKey string) {
	cachedEntry, exists := d.manager.GetCachedEntry(channelID)
	if !exists {
		return
	}
	if ch, ok := cachedEntry.segmentDownloadSignals.LoadAndDelete(segmentKey); ok {
		close(ch.(chan struct{}))
	}
}

func (d *Downloader) fetchSegment(url string, userAgent string) ([]byte, string, error) {
	var lastErr error
	maxRetries := 3
	retryDelay := 500 * time.Millisecond
	requestTimeout := 8 * time.Second

	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, "", fmt.Errorf("创建请求失败: %w", err)
		}
		if userAgent != "" {
			req.Header.Set("User-Agent", userAgent)
		}

		resp, err := d.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("请求上游 SEGMENT 失败 (attempt %d/%d): %w", i+1, maxRetries, err)
			time.Sleep(retryDelay)
			continue
		}

		if resp.StatusCode == http.StatusOK {
			data, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return nil, "", fmt.Errorf("读取 SEGMENT 数据失败: %w", err)
			}
			contentType := resp.Header.Get("Content-Type")
			return data, contentType, nil
		}

		lastErr = fmt.Errorf("上游 SEGMENT 返回非 200 状态码 (attempt %d/%d): %s", i+1, maxRetries, resp.Status)
		resp.Body.Close()

		if resp.StatusCode < 500 && resp.StatusCode != http.StatusRequestTimeout && resp.StatusCode != http.StatusGatewayTimeout {
			break
		}

		time.Sleep(retryDelay)
	}

	return nil, "", lastErr
}

type CachedMPD struct {
	Data                   *mpd.MPD
	FetchedAt              time.Time
	FinalMPDURL            string
	Mux                    sync.RWMutex
	LastAccessedAt         time.Time
	stopAutoUpdateCh       chan struct{}
	HLSBaseMediaSequence   uint64
	LastMPDPublishTime     time.Time
	InitialBaseURL         string
	InitialBaseURLIsSet    bool
	MasterPlaylist         string
	MediaPlaylists         map[string]string
	SegmentCache           *SegmentCache
	segmentDownloadSignals sync.Map
}

type CachedSegment struct {
	Data        []byte
	ContentType string
	FetchedAt   time.Time
}

type SegmentCache struct {
	sync.RWMutex
	Segments map[string]*CachedSegment
}

func (sc *SegmentCache) Get(key string) (*CachedSegment, bool) {
	sc.RLock()
	defer sc.RUnlock()
	seg, exists := sc.Segments[key]
	return seg, exists
}

func (sc *SegmentCache) Set(key string, segment *CachedSegment) {
	sc.Lock()
	defer sc.Unlock()
	if sc.Segments == nil {
		sc.Segments = make(map[string]*CachedSegment)
	}
	sc.Segments[key] = segment
}

func (sc *SegmentCache) Has(key string) bool {
	sc.RLock()
	defer sc.RUnlock()
	_, exists := sc.Segments[key]
	return exists
}

type MPDManager struct {
	Config     *config.AppConfig
	MPDCache   map[string]*CachedMPD
	CacheLock  sync.RWMutex
	Downloader *Downloader
	Logger     *slog.Logger
}

func NewMPDManager(cfg *config.AppConfig, logger *slog.Logger, httpClient *http.Client) *MPDManager {
	_ = url.URL{}
	m := &MPDManager{
		Config:   cfg,
		MPDCache: make(map[string]*CachedMPD),
		Logger:   logger,
	}
	m.Downloader = NewDownloader(m, httpClient)
	return m
}

func (m *MPDManager) GetCachedEntry(channelID string) (*CachedMPD, bool) {
	m.CacheLock.RLock()
	defer m.CacheLock.RUnlock()
	entry, exists := m.MPDCache[channelID]
	return entry, exists
}

func (m *MPDManager) GetMPD(channelCfg *config.ChannelConfig) (string, *mpd.MPD, error) {
	m.CacheLock.RLock()
	cachedEntry, exists := m.MPDCache[channelCfg.ID]
	m.CacheLock.RUnlock()

	if exists {
		cachedEntry.Mux.RLock()
		if entryData := cachedEntry.Data; entryData != nil {
			dataCopy := *entryData
			finalURLCopy := cachedEntry.FinalMPDURL
			cachedEntry.Mux.RUnlock()
			return finalURLCopy, &dataCopy, nil
		}
		cachedEntry.Mux.RUnlock()
	}

	m.CacheLock.Lock()
	entry, ok := m.MPDCache[channelCfg.ID]
	if !ok {
		entry = &CachedMPD{
			SegmentCache: &SegmentCache{
				Segments: make(map[string]*CachedSegment),
			},
		}
		m.MPDCache[channelCfg.ID] = entry
	}
	m.CacheLock.Unlock()

	entry.Mux.Lock()
	if entry.Data != nil {
		dataCopy := *entry.Data
		finalURLCopy := entry.FinalMPDURL
		entry.Mux.Unlock()
		return finalURLCopy, &dataCopy, nil
	}
	entry.Mux.Unlock()

	finalURL, mpdData, err := m.fetchAndProcessMPD(channelCfg, entry)
	if err != nil {
		m.CacheLock.Lock()
		delete(m.MPDCache, channelCfg.ID)
		m.CacheLock.Unlock()
		return "", nil, err
	}

	entry.Mux.RLock()
	isDynamic := entry.Data.Type == "dynamic"
	isUpdaterRunning := entry.stopAutoUpdateCh != nil
	entry.Mux.RUnlock()

	if isDynamic && !isUpdaterRunning {
		entry.Mux.Lock()
		if entry.stopAutoUpdateCh == nil {
			minUpdatePeriod, mupErr := entry.Data.GetMinimumUpdatePeriod()
			if mupErr == nil && minUpdatePeriod > 0 {
				m.Logger.Info("Starting AutoUpdater for dynamic channel", "channel_id", channelCfg.ID, "update_period", minUpdatePeriod)
				entry.stopAutoUpdateCh = make(chan struct{})
				go m.autoUpdateMPD(channelCfg, entry, minUpdatePeriod)
			}
		}
		entry.Mux.Unlock()
	}

	return finalURL, mpdData, nil
}

func (m *MPDManager) fetchAndProcessMPD(channelCfg *config.ChannelConfig, entry *CachedMPD) (string, *mpd.MPD, error) {
	entry.Mux.Lock()
	urlToFetch := channelCfg.Manifest
	if entry.FinalMPDURL != "" {
		urlToFetch = entry.FinalMPDURL
	}
	initialBaseURL := entry.InitialBaseURL
	initialBaseURLIsSet := entry.InitialBaseURLIsSet
	entry.Mux.Unlock()

	newFinalURL, fetchedBaseURL, newMPDData, err := m.fetchMPDWithBaseURLRetry(
		urlToFetch, m.Config.UserAgent,
		initialBaseURL, initialBaseURLIsSet,
	)
	if err != nil {
		return "", nil, fmt.Errorf("error fetching/retrying MPD: %w", err)
	}

	entry.Mux.Lock()
	defer entry.Mux.Unlock()

	if entry.Data != nil {
		newPublishTime, _ := parsePublishTime(newMPDData.PublishTime)
		cachedPublishTime, _ := parsePublishTime(entry.Data.PublishTime)
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

func (m *MPDManager) processMPDUpdates(channelCfg *config.ChannelConfig, entry *CachedMPD) {
	masterPl, selectedRepIDs, err := playlist.GenerateMasterPlaylist(entry.Data, channelCfg.ID)
	if err != nil {
		m.Logger.Error("Error generating master playlist", "channel_id", channelCfg.ID, "error", err)
		return
	}
	entry.MasterPlaylist = masterPl

	mediaPls, segmentsToPreload, _, err := playlist.GenerateMediaPlaylists(m.Logger, entry.Data, entry.FinalMPDURL, channelCfg.ID, entry.HLSBaseMediaSequence, channelCfg.ParsedKey, selectedRepIDs)
	if err != nil {
		m.Logger.Error("Error generating media playlists", "channel_id", channelCfg.ID, "error", err)
	} else {
		entry.MediaPlaylists = mediaPls
		for segmentKey, upstreamURL := range segmentsToPreload {
			if !entry.SegmentCache.Has(segmentKey) {
				entry.segmentDownloadSignals.LoadOrStore(segmentKey, make(chan struct{}))
				m.Downloader.EnqueueTask(&SegmentDownloadTask{
					ChannelID:   channelCfg.ID,
					SegmentKey:  segmentKey,
					UpstreamURL: upstreamURL,
					UserAgent:   m.Config.UserAgent,
				})
			}
		}
	}
}

func (m *MPDManager) updateSequence(entry *CachedMPD) {
	newPublishTime, ptErr := parsePublishTime(entry.Data.PublishTime)
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

func (m *MPDManager) autoUpdateMPD(channelCfg *config.ChannelConfig, cachedEntry *CachedMPD, initialMinUpdatePeriod time.Duration) {
	ticker := time.NewTicker(initialMinUpdatePeriod)
	defer ticker.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-cachedEntry.stopAutoUpdateCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	for {
		select {
		case <-ticker.C:
			_, _, err := m.fetchAndProcessMPD(channelCfg, cachedEntry)
			if err != nil {
				m.Logger.Error("AutoUpdater: Error fetching/processing MPD. Will retry on next tick.", "error", err)
				continue
			}

			cachedEntry.Mux.RLock()
			newMup, mupErr := cachedEntry.Data.GetMinimumUpdatePeriod()
			cachedEntry.Mux.RUnlock()

			if mupErr == nil && newMup > 0 && newMup != initialMinUpdatePeriod {
				ticker.Reset(newMup)
				initialMinUpdatePeriod = newMup
			}

		case <-ctx.Done():
			m.Logger.Info("AutoUpdater: Stop signal received. Exiting.", "channel_id", channelCfg.ID)
			return
		}
	}
}

func (m *MPDManager) WaitForSegments(channelID string, segmentKeys []string, timeout time.Duration) error {
	cachedEntry, exists := m.GetCachedEntry(channelID)
	if !exists {
		return fmt.Errorf("channel entry %s not found", channelID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for _, key := range segmentKeys {
		if cachedEntry.SegmentCache.Has(key) {
			continue
		}

		if sig, ok := cachedEntry.segmentDownloadSignals.Load(key); ok {
			select {
			case <-sig.(chan struct{}):
				if !cachedEntry.SegmentCache.Has(key) {
					return fmt.Errorf("segment %s download finished but not found in cache", key)
				}
			case <-ctx.Done():
				return fmt.Errorf("timed out waiting for segment %s", key)
			}
		} else {
			return fmt.Errorf("no download signal found for segment %s", key)
		}
	}
	return nil
}

func (m *MPDManager) fetchMPDWithBaseURLRetry(
	initialFetchURL, userAgent, knownInitialBaseURL string,
	isInitialBaseURLSet bool,
) (string, string, *mpd.MPD, error) {
	finalURL, mpdObj, err := mpd.FetchAndParseMPD(initialFetchURL, userAgent)
	if err != nil {
		return initialFetchURL, "", nil, fmt.Errorf("initial fetch failed: %w", err)
	}
	var actualBaseURL string

	if mpdObj != nil && len(mpdObj.BaseURLs) > 0 && mpdObj.BaseURLs[0] != "" {
		actualBaseURL = mpdObj.BaseURLs[0]
	}

	if !isInitialBaseURLSet || actualBaseURL == knownInitialBaseURL {
		return finalURL, actualBaseURL, mpdObj, nil
	}

	m.Logger.Warn("BaseURL mismatch. Attempting retries...", "expected", knownInitialBaseURL, "got", actualBaseURL)
	return finalURL, actualBaseURL, mpdObj, nil
}

func (m *MPDManager) StartJanitor(ctx context.Context) {
	m.Logger.Info("Starting janitor...")
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.runJanitor()
			case <-ctx.Done():
				m.Logger.Info("Stopping janitor.")
				return
			}
		}
	}()
}

func (m *MPDManager) runJanitor() {
	const maxSegmentAge = 5 * time.Minute
	m.CacheLock.RLock()
	defer m.CacheLock.RUnlock()

	for channelID, cachedEntry := range m.MPDCache {
		// --- 1. Stop auto-updater for inactive dynamic channels (requires write lock) ---
		cachedEntry.Mux.Lock()
		if cachedEntry.Data != nil && cachedEntry.Data.Type == "dynamic" && cachedEntry.stopAutoUpdateCh != nil {
			minUpdatePeriod, err := cachedEntry.Data.GetMinimumUpdatePeriod()
			if err == nil {
				if minUpdatePeriod == 0 {
					minUpdatePeriod = 5 * time.Second
				}
				inactivityThreshold := 5 * minUpdatePeriod
				if time.Since(cachedEntry.LastAccessedAt) > inactivityThreshold {
					m.Logger.Info("Channel inactive, stopping auto-updater.", "channel_id", channelID)
					close(cachedEntry.stopAutoUpdateCh)
					cachedEntry.stopAutoUpdateCh = nil // Mark as stopped
				}
			}
		}
		cachedEntry.Mux.Unlock()

		// --- 2. Cleanup expired segments (optimized locking) ---

		// Step 2a: Get playlists with a read lock
		cachedEntry.Mux.RLock()
		playlists := make([]string, 0, len(cachedEntry.MediaPlaylists))
		for _, pl := range cachedEntry.MediaPlaylists {
			playlists = append(playlists, pl)
		}
		segmentCache := cachedEntry.SegmentCache
		cachedEntry.Mux.RUnlock()

		if segmentCache == nil {
			continue
		}

		// Step 2b: Build valid segments set without a lock
		validSegments := make(map[string]struct{})
		for _, pl := range playlists {
			lines := strings.Split(pl, "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "/hls/") {
					validSegments[line] = struct{}{}
				}
			}
		}

		// Step 2c: Identify segments to delete with a read lock
		segmentCache.RLock()
		initialCount := len(segmentCache.Segments)
		keysToDelete := make([]string, 0)
		for key, segment := range segmentCache.Segments {
			if _, isValid := validSegments[key]; !isValid && time.Since(segment.FetchedAt) > maxSegmentAge {
				keysToDelete = append(keysToDelete, key)
			}
		}
		segmentCache.RUnlock()

		// Step 2d: Delete segments with a write lock
		if len(keysToDelete) > 0 {
			segmentCache.Lock()
			for _, key := range keysToDelete {
				delete(segmentCache.Segments, key)
			}
			segmentCache.Unlock()

			finalCount := initialCount - len(keysToDelete)
			m.Logger.Info("Segment cache cleanup finished", "channel_id", channelID, "removed_count", len(keysToDelete), "initial_count", initialCount, "final_count", finalCount)
		}
	}
}

func parsePublishTime(pt string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, pt)
	if err != nil {
		t, err = time.Parse("2006-01-02T15:04:05Z", pt)
	}
	return t, err
}
