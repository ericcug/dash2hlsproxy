package mpd_manager

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"dash2hlsproxy/internal/config"
	"dash2hlsproxy/internal/mpd"
)

// 在实时媒体播放列表中保留的段数
const numLiveSegments = 5

// 全局最大并发下载数
const globalMaxConcurrentDownloads = 5

// SegmentDownloadTask 定义一个 SEGMENT 下载任务
type SegmentDownloadTask struct {
	ChannelID   string
	SegmentKey  string // 缓存键: streamType/qualityOrLang/segmentIdentifier
	UpstreamURL string
	UserAgent   string
}

// SegmentPreloader 接口定义了 SEGMENT 预加载器的行为
type SegmentPreloader interface {
	Start(ctx context.Context)
	Stop()
	UpdateSegmentsToPreload(channelID string, segmentsToCache map[string]string)
	// SetHTTPClient 允许在创建后设置 HTTP 客户端
	SetHTTPClient(client *http.Client)
}

// segmentPreloader 是 SegmentPreloader 接口的具体实现
type segmentPreloader struct {
	httpClient *http.Client
	// 待下载任务队列
	downloadQueue chan *SegmentDownloadTask
	// 控制全局并发度的信号量
	activeDownloads chan struct{}

	// channelSegments 跟踪每个频道当前需要缓存的 SEGMENT 键
	// map[channelID]map[segmentKey]struct{}
	// 使用 sync.Map 替代 map[string]map[string]struct{} 以避免外部锁
	channelSegments sync.Map

	// 停止信号
	stopCh chan struct{}
	// 等待所有 worker 协程退出
	wg sync.WaitGroup
}

// NewSegmentPreloader 创建一个新的 SegmentPreloader 实例
func NewSegmentPreloader() SegmentPreloader {
	return &segmentPreloader{
		// 缓冲队列，防止阻塞
		downloadQueue:   make(chan *SegmentDownloadTask, 100),
		activeDownloads: make(chan struct{}, globalMaxConcurrentDownloads),
		stopCh:          make(chan struct{}),
	}
}

// SetHTTPClient 设置 HTTP 客户端
func (sp *segmentPreloader) SetHTTPClient(client *http.Client) {
	sp.httpClient = client
}

// Start 启动预加载器的 worker 协程
func (sp *segmentPreloader) Start(ctx context.Context) {
	for i := 0; i < globalMaxConcurrentDownloads; i++ {
		sp.wg.Add(1)
		go sp.worker(ctx)
	}
	slog.Info("SegmentPreloader: Started worker goroutines", "count", globalMaxConcurrentDownloads)
}

// Stop 停止预加载器
func (sp *segmentPreloader) Stop() {
	close(sp.stopCh)
	sp.wg.Wait()
	slog.Info("SegmentPreloader: Stopped.")
}

// worker 协程从下载队列中获取任务并执行下载
func (sp *segmentPreloader) worker(ctx context.Context) {
	defer sp.wg.Done()
	for {
		select {
		case task := <-sp.downloadQueue:
			// 获取一个并发槽
			sp.activeDownloads <- struct{}{}

			// 检查任务是否有效
			cachedEntry, exists := mpdManagerInstance.GetCachedEntry(task.ChannelID)
			if !exists || cachedEntry.SegmentCache == nil {
				slog.Warn("Preloader worker: Channel or its SegmentCache not found. Skipping download.", "channel_id", task.ChannelID, "segment_key", task.SegmentKey)
				// 释放并发槽
				<-sp.activeDownloads
				continue
			}

			// 1. 首先检查缓存中是否已存在
			cachedEntry.SegmentCache.RLock()
			_, segmentExists := cachedEntry.SegmentCache.segments[task.SegmentKey]
			cachedEntry.SegmentCache.RUnlock()

			if segmentExists {
				// 如果已经存在，则跳过下载
				// 释放并发槽
				<-sp.activeDownloads
				continue
			}

			// 2. 如果缓存中不存在，再检查是否正在被其他 worker 下载
			if _, loaded := cachedEntry.SegmentCache.downloading.LoadOrStore(task.SegmentKey, make(chan struct{})); loaded {
				// 另一个 goroutine 正在下载，跳过
				// 释放并发槽
				<-sp.activeDownloads
				continue
			}

			// 执行下载
			slog.Debug("Preloader worker: Downloading segment", "segment_key", task.SegmentKey, "channel_id", task.ChannelID, "url", task.UpstreamURL)
			data, err := sp.fetchSegment(task.UpstreamURL, task.UserAgent)

			// 下载完成后，通知等待者
			if ch, ok := cachedEntry.SegmentCache.downloading.Load(task.SegmentKey); ok {
				close(ch.(chan struct{}))
				cachedEntry.SegmentCache.downloading.Delete(task.SegmentKey)
			}

			if err != nil {
				slog.Error("Preloader worker: Failed to download segment", "segment_key", task.SegmentKey, "channel_id", task.ChannelID, "error", err)
			} else {
				cachedEntry.Mux.Lock()
				// Double check in case it was cleared
				if cachedEntry.SegmentCache == nil {
					cachedEntry.SegmentCache = &SegmentCache{
						segments: make(map[string]*CachedSegment),
					}
				}
				cachedEntry.SegmentCache.segments[task.SegmentKey] = &CachedSegment{
					Data:      data,
					FetchedAt: time.Now(),
				}
				cachedEntry.Mux.Unlock()
				slog.Debug("Preloader worker: Successfully cached segment", "segment_key", task.SegmentKey, "channel_id", task.ChannelID)
			}
			// 释放并发槽
			<-sp.activeDownloads

		case <-sp.stopCh:
			slog.Info("Preloader worker: Stop signal received. Exiting.")
			return
		case <-ctx.Done():
			slog.Info("Preloader worker: Context cancelled. Exiting.")
			return
		}
	}
}

// fetchSegment 实际执行 SEGMENT 下载
func (sp *segmentPreloader) fetchSegment(url string, userAgent string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}

	resp, err := sp.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求上游 SEGMENT 失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("上游 SEGMENT 返回非 200 状态码: %s", resp.Status)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取 SEGMENT 数据失败: %w", err)
	}
	return data, nil
}

// UpdateSegmentsToPreload 更新指定频道需要预加载的 SEGMENT 列表
func (sp *segmentPreloader) UpdateSegmentsToPreload(channelID string, segmentsToCache map[string]string) {
	// 获取当前频道已知的 SEGMENT 列表
	currentSegmentsVal, _ := sp.channelSegments.LoadOrStore(channelID, &sync.Map{})
	currentSegments := currentSegmentsVal.(*sync.Map)

	// 从缓存中删除旧 SEGMENT
	segmentsToDelete := make([]string, 0)
	// 假设 mpdManagerInstance 可用
	cachedEntry, exists := mpdManagerInstance.GetCachedEntry(channelID)
	if exists && cachedEntry.SegmentCache != nil {
		cachedEntry.Mux.Lock()
		currentSegments.Range(func(key, value interface{}) bool {
			segmentKey := key.(string)
			if _, existsInNewList := segmentsToCache[segmentKey]; !existsInNewList {
				segmentsToDelete = append(segmentsToDelete, segmentKey)
			}
			return true
		})

		for _, key := range segmentsToDelete {
			delete(cachedEntry.SegmentCache.segments, key)
			// 从跟踪列表中删除
			currentSegments.Delete(key)
			slog.Debug("Preloader: Removed expired segment from cache", "segment_key", key, "channel_id", channelID)
		}
		cachedEntry.Mux.Unlock()
	}

	// 识别需要添加的新 SEGMENT 并加入下载队列
	for segmentKey, upstreamURL := range segmentsToCache {
		if _, loaded := currentSegments.LoadOrStore(segmentKey, struct{}{}); !loaded {
			// 新的 SEGMENT，加入下载队列
			var userAgent string
			if mpdManagerInstance != nil && mpdManagerInstance.Config != nil && mpdManagerInstance.Config.ChannelMap != nil {
				if channelCfg, ok := mpdManagerInstance.Config.ChannelMap[channelID]; ok {
					userAgent = channelCfg.UserAgent
				}
			}

			sp.downloadQueue <- &SegmentDownloadTask{
				ChannelID:   channelID,
				SegmentKey:  segmentKey,
				UpstreamURL: upstreamURL,
				UserAgent:   userAgent,
			}
			slog.Debug("Preloader: Added new segment to download queue", "segment_key", segmentKey, "channel_id", channelID)
		}
	}
}

// mpdManagerInstance 是一个全局变量，用于在 SegmentPreloader 中访问 MPDManager
// 这是一个临时的解决方案，更好的方式是通过依赖注入传递
var mpdManagerInstance *MPDManager

// SetMPDManagerInstance 设置全局 MPDManager 实例
func SetMPDManagerInstance(m *MPDManager) {
	mpdManagerInstance = m
}

// PrecomputedSegmentData holds readily usable data for a specific stream/quality.
type PrecomputedSegmentData struct {
	// Fully resolved base URL for this segment set
	ResolvedBaseURL  string
	SegmentTemplate  *mpd.SegmentTemplate
	RepresentationID string
	// Reference to the AS
	AdaptationSet *mpd.AdaptationSet
	// Reference to the Rep
	Representation *mpd.Representation
}

// CachedMPD holds a parsed MPD, its fetch time, and the final URL it was fetched from.
type CachedMPD struct {
	Data                 *mpd.MPD
	FetchedAt            time.Time
	FinalMPDURL          string
	Mux                  sync.RWMutex
	LastAccessedAt       time.Time
	stopAutoUpdateCh     chan struct{}
	HLSBaseMediaSequence uint64
	LastMPDPublishTime   time.Time
	InitialBaseURL       string
	InitialBaseURLIsSet  bool
	MasterPlaylist       string
	MediaPlaylists       map[string]string
	// PrecomputedData: map[streamType]map[qualityOrLang]PrecomputedSegmentData
	PrecomputedData map[string]map[string]PrecomputedSegmentData
	// 用于 SEGMENT 缓存的新字段
	SegmentCache *SegmentCache
	// 用于通知预加载器更新其 SEGMENT 列表
	preloaderUpdateCh chan struct{}
}

// CachedSegment 存储 SEGMENT 数据及其获取时间。
type CachedSegment struct {
	Data      []byte
	FetchedAt time.Time
}

// SegmentCache 管理特定频道/表示的缓存 SEGMENT。
type SegmentCache struct {
	sync.RWMutex
	segments map[string]*CachedSegment // 键: streamType/qualityOrLang/segmentIdentifier
	// downloading 用于协调并发下载。键: segmentKey, 值: chan struct{} (下载完成信号)
	downloading sync.Map
}

// MPDManager manages the MPD cache and updates.
type MPDManager struct {
	Config    *config.AppConfig
	MPDCache  map[string]*CachedMPD
	CacheLock sync.RWMutex
	// 全局的 SegmentPreloader 实例
	Preloader SegmentPreloader
	Logger    *slog.Logger
}

// NewMPDManager creates a new MPDManager.
// Note: The SegmentPreloader must be set after creation, as it depends on HTTPClient from AppContext.
func NewMPDManager(cfg *config.AppConfig, logger *slog.Logger) *MPDManager {
	return &MPDManager{
		Config:   cfg,
		MPDCache: make(map[string]*CachedMPD),
		Logger:   logger,
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
	m.CacheLock.RLock()
	cachedEntry, exists := m.MPDCache[channelCfg.ID]
	m.CacheLock.RUnlock()

	if exists {
		cachedEntry.Mux.RLock()
		if entryData := cachedEntry.Data; entryData != nil {
			dataCopy := *entryData
			finalURLCopy := cachedEntry.FinalMPDURL
			cachedEntry.LastAccessedAt = time.Now()
			cachedEntry.Mux.RUnlock()
			return finalURLCopy, &dataCopy, nil
		}
		cachedEntry.Mux.RUnlock()
	}

	m.CacheLock.Lock()
	entry, ok := m.MPDCache[channelCfg.ID]
	if !ok {
		entry = &CachedMPD{
			stopAutoUpdateCh: make(chan struct{}),
			// Initial sequence
			HLSBaseMediaSequence: 0,
			PrecomputedData:      make(map[string]map[string]PrecomputedSegmentData),
			// 初始化 SegmentCache
			SegmentCache: &SegmentCache{
				segments: make(map[string]*CachedSegment),
			},
			// 缓冲 channel，防止阻塞
			preloaderUpdateCh: make(chan struct{}, 1),
		}
		m.MPDCache[channelCfg.ID] = entry
	}
	m.CacheLock.Unlock()

	entry.Mux.Lock()
	defer entry.Mux.Unlock()

	// If data exists and was fetched recently by another goroutine, use it.
	if entry.Data != nil {
		// Create a shallow copy of MPD data for return
		dataCopy := *entry.Data
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
		urlToFetch = entry.FinalMPDURL
	}

	// Fetch MPD with BaseURL consistency check and retries
	// Stored InitialBaseURL and InitialBaseURLIsSet are read while 'entry' is locked.
	newFinalURL, newMPDData, fetchedBaseURL, err := m.fetchMPDWithBaseURLRetry(
		urlToFetch,
		channelCfg.UserAgent,
		channelCfg.ID,
		channelCfg.Name,
		entry.InitialBaseURL,
		entry.InitialBaseURLIsSet,
		// maxRetries
		5,
		// retryDelay
		2*time.Second,
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
		// actualBaseURL from the first successful fetch
		entry.InitialBaseURL = fetchedBaseURL
		entry.InitialBaseURLIsSet = true
	}

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
			// Don't increment for the very first fetch
			if !entry.LastMPDPublishTime.IsZero() {
				entry.HLSBaseMediaSequence++
			}
			entry.LastMPDPublishTime = newPublishTime
		}
	} else {
		m.Logger.Warn("Could not parse PublishTime", "publish_time", newMPDData.PublishTime, "channel_id", channelCfg.ID, "error", ptErr)
	}

	if newMPDData.Type != "static" {
		minUpdatePeriod, errMUP := newMPDData.GetMinimumUpdatePeriod()
		if errMUP == nil && minUpdatePeriod > 0 {
			go m.autoUpdateMPD(channelCfg, entry, minUpdatePeriod)
		} else if errMUP != nil {
			m.Logger.Warn("Channel is dynamic but error getting MinimumUpdatePeriod. No auto-update.", "channel_name", channelCfg.Name, "channel_id", channelCfg.ID, "error", errMUP)
		}
	}

	dataCopy := *newMPDData

	// Precompute data and generate HLS playlists
	// This needs to be done after entry.Data and entry.FinalMPDURL are set.
	// Pass the whole entry
	errPrecompute := m.precomputeMPDData(entry)
	if errPrecompute != nil {
		m.Logger.Error("Error precomputing MPD data", "channel_id", channelCfg.ID, "channel_name", channelCfg.Name, "error", errPrecompute)
		// Decide if this is a fatal error for GetMPD. For now, log and continue.
		// The playlists might be empty or incomplete if precomputation fails partially.
	}

	masterPl, errMaster := generateMasterPlaylist(entry.Data, channelCfg.ID)
	if errMaster != nil {
		m.Logger.Error("Error generating master playlist", "channel_id", channelCfg.ID, "channel_name", channelCfg.Name, "error", errMaster)
	} else {
		entry.MasterPlaylist = masterPl
	}

	mediaPls, segmentsToPreload, errMedia := m.generateMediaPlaylists(entry.Data, entry.FinalMPDURL, channelCfg.ID, entry.HLSBaseMediaSequence, channelCfg.ParsedKey)
	if errMedia != nil {
		m.Logger.Error("Error generating media playlists", "channel_id", channelCfg.ID, "error", errMedia)
	} else {
		entry.MediaPlaylists = mediaPls
		// 通知预加载器更新 SEGMENT 列表
		if m.Preloader != nil {
			m.Preloader.UpdateSegmentsToPreload(channelCfg.ID, segmentsToPreload)
		}
	}

	return newFinalURL, &dataCopy, nil
}

// precomputeMPDData populates the PrecomputedData map in CachedMPD.
// This function should be called when entry.Mux is WLock'd and entry.Data is populated.
func (m *MPDManager) precomputeMPDData(cachedEntry *CachedMPD) error {
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
				m.Logger.Warn("Error parsing Period BaseURL. Using parent base.", "base_url", p.BaseURLs[0], "parent_base", currentPeriodBase.String(), "error", errParsePBase)
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
			} else if as.ContentType == "text" || strings.Contains(as.MimeType, "text") || strings.Contains(as.MimeType, "application/ttml+xml") || (strings.Contains(as.MimeType, "application/mp4") && len(as.Representations) > 0 && strings.Contains(as.Representations[0].Codecs, "wvtt")) {
				streamTypeKey = "subtitles"
			} else {
				// Skip unknown content types
				continue
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
					// Fallback if RepresentationID is empty
					if qualityOrLangKey == "" {
						// More unique fallback
						qualityOrLangKey = fmt.Sprintf("video_rep%d_as%d_p%d", k, j, i)
					}
				case "audio":
					qualityOrLangKey = as.Lang
					if qualityOrLangKey == "" {
						// More unique fallback
						qualityOrLangKey = fmt.Sprintf("audio_default%d_as%d_p%d", k, j, i)
					}
				case "subtitles":
					qualityOrLangKey = as.Lang
					if qualityOrLangKey == "" {
						// More unique fallback
						qualityOrLangKey = fmt.Sprintf("sub_default%d_as%d_p%d", k, j, i)
					}
				}
				// Should be rare with fallbacks
				if qualityOrLangKey == "" {
					m.Logger.Warn("qualityOrLangKey is empty. Skipping.", "stream_type", streamTypeKey, "as_id", as.ID, "rep_id", rep.ID)
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
						m.Logger.Warn("Error parsing AS.BaseURL. Using Period base.", "as_base_url", as.BaseURL, "period_base", asBase.String(), "error", errParseASBase)
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

				// Still no template, skip
				if segTemplate == nil {
					m.Logger.Warn("No segment template found for stream. Skipping.", "stream_type", streamTypeKey, "quality_or_lang", qualityOrLangKey)
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
	return nil
}

func (m *MPDManager) autoUpdateMPD(channelCfg *config.ChannelConfig, cachedEntry *CachedMPD, initialMinUpdatePeriod time.Duration) {
	ticker := time.NewTicker(initialMinUpdatePeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var currentMinUpdatePeriodDuration time.Duration
			var lastAccessed time.Time
			var currentFinalMPDURL string
			var currentPublishTime time.Time
			// For BaseURL consistency check
			var knownInitialBaseURL string
			// For BaseURL consistency check
			var isInitialBaseURLSet bool

			cachedEntry.Mux.RLock()
			// Should not happen if auto-updater is running
			if cachedEntry.Data == nil {
				m.Logger.Error("AutoUpdater: Cached MPD data is nil. Stopping.", "channel_id", channelCfg.ID)
				cachedEntry.Mux.RUnlock()
				return
			}
			var mupErr error
			currentMinUpdatePeriodDuration, mupErr = cachedEntry.Data.GetMinimumUpdatePeriod()
			if mupErr != nil {
				m.Logger.Error("AutoUpdater: Error getting current MinimumUpdatePeriod from cached MPD. Stopping.", "channel_id", channelCfg.ID, "error", mupErr)
				cachedEntry.Mux.RUnlock()
				return
			}
			lastAccessed = cachedEntry.LastAccessedAt
			currentFinalMPDURL = cachedEntry.FinalMPDURL
			// Use our stored LastMPDPublishTime
			currentPublishTime = cachedEntry.LastMPDPublishTime
			knownInitialBaseURL = cachedEntry.InitialBaseURL
			isInitialBaseURLSet = cachedEntry.InitialBaseURLIsSet
			cachedEntry.Mux.RUnlock()

			// Stop auto-update if channel is inactive for 5 times the initial minimum update period.
			inactivityTimeout := 5 * initialMinUpdatePeriod
			// Safety fallback, as initialMinUpdatePeriod should be > 0 when autoUpdateMPD is called
			if inactivityTimeout <= 0 {
				// e.g., 50 minutes if MUP was unexpectedly zero or negative
				inactivityTimeout = 5 * 10 * time.Minute
			}

			if time.Since(lastAccessed) > inactivityTimeout {
				m.Logger.Info("AutoUpdater: Channel inactive. Stopping auto-updater and clearing cache.",
					"channel_id", channelCfg.ID,
					"inactivity_timeout", inactivityTimeout,
					"last_access", lastAccessed.Format(time.RFC3339),
					"initial_mup", initialMinUpdatePeriod)

				m.CacheLock.Lock()
				delete(m.MPDCache, channelCfg.ID)
				m.CacheLock.Unlock()
				m.Logger.Info("AutoUpdater: Cache cleared due to inactivity.", "channel_id", channelCfg.ID)
				return
			}

			urlToFetch := currentFinalMPDURL
			// Fallback to initial manifest if final URL somehow became empty
			if urlToFetch == "" {
				urlToFetch = channelCfg.Manifest
				m.Logger.Warn("AutoUpdater: FinalMPDURL was empty, falling back to initial manifest URL", "channel_id", channelCfg.ID, "url", urlToFetch)
			}
			if urlToFetch == "" {
				m.Logger.Warn("AutoUpdater: No URL to fetch MPD from. Skipping update.", "channel_id", channelCfg.ID)
				continue
			}

			// Assign fetchedBaseURL to _
			newFinalURL, newMPDData, _, err := m.fetchMPDWithBaseURLRetry(
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
				m.Logger.Error("AutoUpdater: Error fetching/retrying MPD. Will retry on next tick.", "channel_id", channelCfg.ID, "url", urlToFetch, "error", err)
				continue
			}
			if newMPDData == nil {
				m.Logger.Error("AutoUpdater: Failed to obtain MPD data after retries. Will retry on next tick.", "channel_id", channelCfg.ID, "url", urlToFetch)
				continue
			}

			cachedEntry.Mux.Lock()

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
				}
			} else {
				m.Logger.Warn("AutoUpdater: Could not parse new PublishTime", "channel_id", channelCfg.ID, "publish_time", newMPDData.PublishTime, "error", ptErr)
			}

			cachedEntry.Data = newMPDData
			cachedEntry.FetchedAt = time.Now()
			cachedEntry.FinalMPDURL = newFinalURL

			newMinUpdatePeriod, newMupErr := newMPDData.GetMinimumUpdatePeriod()
			if newMupErr == nil && newMinUpdatePeriod > 0 && newMinUpdatePeriod != currentMinUpdatePeriodDuration {
				ticker.Reset(newMinUpdatePeriod)
				initialMinUpdatePeriod = newMinUpdatePeriod
			} else if newMupErr != nil {
				m.Logger.Warn("AutoUpdater: Error parsing new MinimumUpdatePeriod. Keeping current ticker interval.", "channel_id", channelCfg.ID, "mup", newMPDData.MinimumUpdatePeriod, "error", newMupErr, "current_interval", currentMinUpdatePeriodDuration)
			}
			if entryDataChanged {
				// Regenerate HLS playlists and precompute data as MPD data or sequence number changed
				errPrecompute := m.precomputeMPDData(cachedEntry)
				if errPrecompute != nil {
					m.Logger.Error("AutoUpdater: Error precomputing MPD data", "channel_id", channelCfg.ID, "error", errPrecompute)
				}

				masterPl, errMaster := generateMasterPlaylist(cachedEntry.Data, channelCfg.ID)
				if errMaster != nil {
					m.Logger.Error("AutoUpdater: Error generating master playlist", "channel_id", channelCfg.ID, "error", errMaster)
				} else {
					cachedEntry.MasterPlaylist = masterPl
				}

				mediaPls, segmentsToPreload, errMedia := m.generateMediaPlaylists(cachedEntry.Data, cachedEntry.FinalMPDURL, channelCfg.ID, cachedEntry.HLSBaseMediaSequence, channelCfg.ParsedKey)
				if errMedia != nil {
					m.Logger.Error("AutoUpdater: Error generating media playlists", "channel_id", channelCfg.ID, "error", errMedia)
				} else {
					cachedEntry.MediaPlaylists = mediaPls
					// 通知预加载器更新 SEGMENT 列表
					if m.Preloader != nil {
						m.Preloader.UpdateSegmentsToPreload(channelCfg.ID, segmentsToPreload)
					}
				}
			}
			cachedEntry.Mux.Unlock()

		case <-cachedEntry.stopAutoUpdateCh:
			// 通知预加载器停止预加载该频道的 SEGMENT
			if m.Preloader != nil {
				// 传入空 map 清理
				m.Preloader.UpdateSegmentsToPreload(channelCfg.ID, map[string]string{})
			}
			return
		}
	}
}

// fetchMPDWithBaseURLRetry fetches an MPD and retries if the BaseURL does not match a known initial BaseURL.
func (m *MPDManager) fetchMPDWithBaseURLRetry(
	initialFetchURL string,
	userAgent string,
	channelID string,
	channelName string,
	knownInitialBaseURL string,
	isInitialBaseURLSet bool,
	maxRetries int,
	retryDelay time.Duration,
) (finalURL string, mpdObj *mpd.MPD, actualBaseURL string, err error) {
	fetchedFinalURL, fetchedMPDObj, fetchErr := mpd.FetchAndParseMPD(initialFetchURL, userAgent)
	if fetchErr != nil {
		return initialFetchURL, nil, "", fmt.Errorf("initial fetch failed: %w", fetchErr)
	}

	var currentActualBaseURL string
	if fetchedMPDObj != nil && len(fetchedMPDObj.BaseURLs) > 0 && fetchedMPDObj.BaseURLs[0] != "" {
		currentActualBaseURL = fetchedMPDObj.BaseURLs[0]
	}

	if !isInitialBaseURLSet {
		return fetchedFinalURL, fetchedMPDObj, currentActualBaseURL, nil
	}

	if currentActualBaseURL == knownInitialBaseURL {
		return fetchedFinalURL, fetchedMPDObj, currentActualBaseURL, nil
	}

	m.Logger.Warn("BaseURL mismatch. Attempting retries...",
		"channel_id", channelID,
		"channel_name", channelName,
		"expected_base_url", knownInitialBaseURL,
		"got_base_url", currentActualBaseURL,
		"url", initialFetchURL,
		"final_url", fetchedFinalURL)

	lastGoodMPD := fetchedMPDObj
	lastGoodFinalURL := fetchedFinalURL
	lastGoodActualBaseURL := currentActualBaseURL

	for i := 0; i < maxRetries; i++ {
		time.Sleep(retryDelay)

		retryFinalURL, retryMPDData, retryErr := mpd.FetchAndParseMPD(initialFetchURL, userAgent)
		if retryErr != nil {
			m.Logger.Error("Retry fetch failed", "retry_attempt", i+1, "max_retries", maxRetries, "channel_id", channelID, "error", retryErr)
			if i == maxRetries-1 {
				m.Logger.Error("Max retries reached, last retry fetch failed. Returning MPD from initial attempt.", "channel_id", channelID, "initial_base_url", lastGoodActualBaseURL)
				return lastGoodFinalURL, lastGoodMPD, lastGoodActualBaseURL, nil
			}
			continue
		}

		var retryAttemptBaseURL string
		if retryMPDData != nil && len(retryMPDData.BaseURLs) > 0 && retryMPDData.BaseURLs[0] != "" {
			retryAttemptBaseURL = retryMPDData.BaseURLs[0]
		}

		if retryAttemptBaseURL == knownInitialBaseURL {
			return retryFinalURL, retryMPDData, retryAttemptBaseURL, nil
		}

		lastGoodMPD = retryMPDData
		lastGoodFinalURL = retryFinalURL
		lastGoodActualBaseURL = retryAttemptBaseURL

		m.Logger.Warn("Retry BaseURL still mismatch",
			"retry_attempt", i+1,
			"max_retries", maxRetries,
			"channel_id", channelID,
			"expected_base_url", knownInitialBaseURL,
			"got_base_url", retryAttemptBaseURL,
			"final_url", retryFinalURL)

		if i == maxRetries-1 {
			m.Logger.Warn("Max retries reached, BaseURL still mismatch. Proceeding with this last fetched MPD.", "channel_id", channelID, "base_url", retryAttemptBaseURL)
			return retryFinalURL, retryMPDData, retryAttemptBaseURL, nil
		}
	}
	// This log indicates an unexpected state, so it should be kept.
	m.Logger.Error("Exited retry loop unexpectedly. Returning last known good MPD.", "channel_id", channelID, "base_url", lastGoodActualBaseURL)
	return lastGoodFinalURL, lastGoodMPD, lastGoodActualBaseURL, nil
}

// generateMasterPlaylist generates the HLS master playlist string.
func generateMasterPlaylist(mpdData *mpd.MPD, channelID string) (string, error) {
	var playlist bytes.Buffer
	playlist.WriteString("#EXTM3U\n")
	// Using a higher version for more features if needed
	playlist.WriteString("#EXT-X-VERSION:7\n")

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
					// Default lang if not specified
					if lang == "" {
						lang = fmt.Sprintf("audio%d", audioStreamIndex)
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
					// Make the first audio track default
					if audioStreamIndex == 0 {
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
				(strings.Contains(as.MimeType, "application/mp4") && len(as.Representations) > 0 && strings.Contains(as.Representations[0].Codecs, "wvtt")) {
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
					// Subtitles usually not default unless only option or accessibility requirement
					isDefault := "NO"

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
				// Changed to uint to match rep.Bandwidth
				var maxBandwidth uint

				for i := range as.Representations {
					// Use pointer to avoid copying
					rep := &as.Representations[i]
					if rep.Bandwidth > maxBandwidth {
						maxBandwidth = rep.Bandwidth
						bestRep = rep
					}
				}

				if bestRep != nil {
					// Use the best representation found
					rep := bestRep
					var streamInf strings.Builder
					// PROGRAM-ID is optional but common
					streamInf.WriteString("#EXT-X-STREAM-INF:PROGRAM-ID=1")

					if rep.Bandwidth > 0 {
						streamInf.WriteString(fmt.Sprintf(",BANDWIDTH=%d", rep.Bandwidth))
						// AVERAGE-BANDWIDTH is optional
						streamInf.WriteString(fmt.Sprintf(",AVERAGE-BANDWIDTH=%d", rep.Bandwidth))
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
// It also returns a map of segment keys to their upstream URLs for preloading.
func (m *MPDManager) generateMediaPlaylists(mpdData *mpd.MPD, finalMPDURLStr string, channelID string, hlsBaseMediaSequence uint64, parsedKey []byte) (map[string]string, map[string]string, error) {
	playlists := make(map[string]string)
	// New map to return segment URLs for preloading
	segmentsToPreload := make(map[string]string)

	finalMPDURL, errParseMPDURL := url.Parse(finalMPDURLStr)
	if errParseMPDURL != nil {
		m.Logger.Error("Error parsing finalMPDURLStr", "url", finalMPDURLStr, "error", errParseMPDURL)
		return playlists, segmentsToPreload, fmt.Errorf("error parsing final MPD URL: %w", errParseMPDURL)
	}

	for _, period := range mpdData.Periods {
		currentPeriodBase := finalMPDURL

		if len(period.BaseURLs) > 0 && period.BaseURLs[0] != "" {
			periodLevelBase, errParsePBase := url.Parse(period.BaseURLs[0])
			if errParsePBase == nil {
				currentPeriodBase = currentPeriodBase.ResolveReference(periodLevelBase)
			} else {
				m.Logger.Warn("Error parsing Period BaseURL. Using parent base.", "base_url", period.BaseURLs[0], "parent_base", currentPeriodBase.String(), "error", errParsePBase)
			}
		}

		for _, as := range period.AdaptationSets {
			var streamType string
			if as.ContentType == "video" || strings.Contains(as.MimeType, "video") {
				streamType = "video"
			} else if as.ContentType == "audio" || strings.Contains(as.MimeType, "audio") {
				streamType = "audio"
			} else if as.ContentType == "text" ||
				strings.Contains(as.MimeType, "text") ||
				strings.Contains(as.MimeType, "application/ttml+xml") ||
				(strings.Contains(as.MimeType, "application/mp4") && len(as.Representations) > 0 && strings.Contains(as.Representations[0].Codecs, "wvtt")) {
				streamType = "subtitles"
			} else {
				// Skip unknown content types
				continue
			}

			for i, rep := range as.Representations {
				var qualityOrLangKey string
				if streamType == "video" {
					qualityOrLangKey = rep.ID
					if qualityOrLangKey == "" {
						qualityOrLangKey = fmt.Sprintf("video_rep%d", i)
					}
				} else {
					qualityOrLangKey = as.Lang
					if qualityOrLangKey == "" {
						if streamType == "audio" {
							qualityOrLangKey = fmt.Sprintf("audio%d", adaptationSetAudioIndex(mpdData, &as))
						} else {
							qualityOrLangKey = fmt.Sprintf("sub%d", adaptationSetSubtitleIndex(mpdData, &as))
						}
					}
				}
				if qualityOrLangKey == "" {
					m.Logger.Warn("Empty qualityOrLangKey. Skipping.", "content_type", as.ContentType, "lang", as.Lang, "rep_id", rep.ID)
					continue
				}

				playlistKey := fmt.Sprintf("%s/%s", streamType, qualityOrLangKey)
				var playlistBuf bytes.Buffer
				playlistBuf.WriteString("#EXTM3U\n")
				playlistBuf.WriteString("#EXT-X-VERSION:7\n")

				segTemplate := rep.SegmentTemplate
				if segTemplate == nil {
					segTemplate = as.SegmentTemplate
				}

				if segTemplate == nil || segTemplate.SegmentTimeline == nil || len(segTemplate.SegmentTimeline.Segments) == 0 {
					m.Logger.Warn("SegmentTemplate or SegmentTimeline missing/empty. Skipping playlist.", "rep_id", rep.ID, "channel_id", channelID, "playlist_key", playlistKey)
					continue
				}

				timescale := uint64(1)
				if segTemplate.Timescale != nil {
					timescale = *segTemplate.Timescale
				}
				if timescale == 0 {
					timescale = 1
				}

				var maxSegDurSeconds float64 = 0
				var allSegments []struct {
					StartTime, Duration uint64
					// HLS proxy URL
					HLSURL string
					// Original upstream URL
					UpstreamURL string
				}
				currentStartTime := uint64(0)
				if len(segTemplate.SegmentTimeline.Segments) > 0 && segTemplate.SegmentTimeline.Segments[0].T != nil {
					currentStartTime = *segTemplate.SegmentTimeline.Segments[0].T
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
						m.Logger.Warn("Error parsing AS.BaseURL. Using Period base.", "as_base_url", as.BaseURL, "period_base", asBase.String(), "error", errParseASBase)
					}
				}

				resolvedBaseURLStr := ""
				if asBase != nil {
					resolvedBaseURLStr = asBase.String()
				}
				if resolvedBaseURLStr != "" && !strings.HasSuffix(resolvedBaseURLStr, "/") {
					resolvedBaseURLStr += "/"
				}

				// Handle Initialization Segment for preloading
				if streamType != "subtitles" && segTemplate.Initialization != "" {
					hlsInitSegmentName := "init.m4s"
					hlsInitSegmentURL := fmt.Sprintf("/hls/%s/%s/%s/%s", channelID, streamType, qualityOrLangKey, hlsInitSegmentName)

					initRelativePath := strings.ReplaceAll(segTemplate.Initialization, "$RepresentationID$", rep.ID)
					upstreamInitURL := resolvedBaseURLStr + initRelativePath

					segmentsToPreload[hlsInitSegmentURL] = upstreamInitURL
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

						segmentIdentifierForHLS := strconv.FormatUint(currentStartTime, 10)
						segmentFileExtension := ".m4s"
						if streamType == "subtitles" && strings.Contains(as.MimeType, "application/ttml+xml") {
							segmentFileExtension = ".vtt"
						}
						hlsSegmentURL := fmt.Sprintf("/hls/%s/%s/%s/%s%s", channelID, streamType, qualityOrLangKey, segmentIdentifierForHLS, segmentFileExtension)

						// Construct upstream URL for the segment
						mpdPathSegmentIdentifier := segmentIdentifierForHLS
						tempPath := strings.ReplaceAll(segTemplate.Media, "$RepresentationID$", rep.ID)
						var relativeSegmentPath string
						if strings.Contains(tempPath, "$Time$") {
							relativeSegmentPath = strings.ReplaceAll(tempPath, "$Time$", mpdPathSegmentIdentifier)
						} else if strings.Contains(tempPath, "$Number$") {
							relativeSegmentPath = strings.ReplaceAll(tempPath, "$Number$", mpdPathSegmentIdentifier)
						} else {
							m.Logger.Warn("MPD Media template does not contain $Time$ or $Number$. Using template directly.", "stream_type", streamType, "quality_or_lang", qualityOrLangKey, "template", tempPath)
							relativeSegmentPath = tempPath
						}
						upstreamSegmentURL := resolvedBaseURLStr + relativeSegmentPath

						allSegments = append(allSegments, struct {
							StartTime, Duration uint64
							HLSURL              string
							UpstreamURL         string
						}{
							StartTime: currentStartTime, Duration: s.D, HLSURL: hlsSegmentURL, UpstreamURL: upstreamSegmentURL,
						})
						currentStartTime += s.D
					}
				}

				if len(allSegments) == 0 {
					m.Logger.Warn("No segments generated for key", "playlist_key", playlistKey, "channel_id", channelID)
					continue
				}

				targetDuration := int(maxSegDurSeconds + 0.999)
				if targetDuration == 0 {
					targetDuration = 10
				}
				playlistBuf.WriteString(fmt.Sprintf("#EXT-X-TARGETDURATION:%d\n", targetDuration))
				playlistBuf.WriteString(fmt.Sprintf("#EXT-X-MEDIA-SEQUENCE:%d\n", hlsBaseMediaSequence))

				// EXT-X-MAP (Initialization Segment)
				if streamType != "subtitles" {
					if segTemplate.Initialization != "" {
						hlsInitSegmentName := "init.m4s"
						mapURI := fmt.Sprintf("/hls/%s/%s/%s/%s", channelID, streamType, qualityOrLangKey, hlsInitSegmentName)
						playlistBuf.WriteString(fmt.Sprintf("#EXT-X-MAP:URI=\"%s\"\n", mapURI))
					}
				}

				// EXT-X-KEY (Encryption)
				if len(parsedKey) > 0 && streamType != "subtitles" {
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
					// Use the HLS proxy URL
					playlistBuf.WriteString(seg.HLSURL + "\n")
					// Add to preloading map
					segmentsToPreload[seg.HLSURL] = seg.UpstreamURL
				}

				if mpdData.Type == "static" {
					playlistBuf.WriteString("#EXT-X-ENDLIST\n")
				}
				playlists[playlistKey] = playlistBuf.String()
			}
		}
	}
	if len(playlists) == 0 {
		m.Logger.Warn("generateMediaPlaylists produced no playlists", "channel_id", channelID)
	}
	return playlists, segmentsToPreload, nil
}

// Helper functions to get a consistent index for default naming of audio/subtitle tracks
func adaptationSetAudioIndex(mpdData *mpd.MPD, targetAs *mpd.AdaptationSet) int {
	idx := 0
	for _, p := range mpdData.Periods {
		for i := range p.AdaptationSets {
			// Get a pointer to the actual element
			as := &p.AdaptationSets[i]
			if as.ContentType == "audio" || strings.Contains(as.MimeType, "audio") {
				// Compare IDs
				if as.ID == targetAs.ID {
					return idx
				}
				idx++
			}
		}
	}
	// Should not happen if targetAs is from mpdData
	return -1
}

func adaptationSetSubtitleIndex(mpdData *mpd.MPD, targetAs *mpd.AdaptationSet) int {
	idx := 0
	for _, p := range mpdData.Periods {
		for i := range p.AdaptationSets {
			// Get a pointer to the actual element
			as := &p.AdaptationSets[i]
			// Adjusted condition to correctly identify subtitle tracks for indexing
			if as.ContentType == "text" ||
				strings.Contains(as.MimeType, "text") ||
				strings.Contains(as.MimeType, "application/ttml+xml") ||
				(strings.Contains(as.MimeType, "application/mp4") && len(as.Representations) > 0 && strings.Contains(as.Representations[0].Codecs, "wvtt")) {
				// Compare IDs
				if as.ID == targetAs.ID {
					return idx
				}
				idx++
			}
		}
	}
	// Should not happen
	return -1
}
