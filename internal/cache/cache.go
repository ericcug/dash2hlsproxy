package cache

import (
	"strings"
	"sync"
	"time"

	"dash2hlsproxy/internal/fetch"
	"dash2hlsproxy/internal/mpd"
)

// SegmentCache 缓存下载的媒体分片
type SegmentCache struct {
	sync.RWMutex
	Segments map[string]*fetch.CachedSegment
}

// Get 从缓存中检索一个分片
func (sc *SegmentCache) Get(key string) (*fetch.CachedSegment, bool) {
	sc.RLock()
	defer sc.RUnlock()
	seg, exists := sc.Segments[key]
	return seg, exists
}

// Set 将一个分片添加到缓存中
func (sc *SegmentCache) Set(key string, segment *fetch.CachedSegment) {
	sc.Lock()
	defer sc.Unlock()
	if sc.Segments == nil {
		sc.Segments = make(map[string]*fetch.CachedSegment)
	}
	sc.Segments[key] = segment
}

// Has 检查缓存中是否存在一个分片
func (sc *SegmentCache) Has(key string) bool {
	sc.RLock()
	defer sc.RUnlock()
	_, exists := sc.Segments[key]
	return exists
}

// Prune 从缓存中删除所有不在提供的 validKeys map 中的分片
func (sc *SegmentCache) Prune(validKeys map[string]struct{}, initSegmentTTL time.Duration) {
	sc.Lock()
	defer sc.Unlock()
	for key, segment := range sc.Segments {
		if _, ok := validKeys[key]; !ok {
			// 对于 init.m4s 文件，我们希望它在缓存中保留更长时间
			if strings.HasSuffix(key, "init.m4s") {
				if time.Since(segment.FetchedAt) > initSegmentTTL {
					delete(sc.Segments, key)
				}
			} else {
				delete(sc.Segments, key)
			}
		}
	}
}

// MPDEntry 代表一个缓存的 MPD 及其相关状态
type MPDEntry struct {
	Data                   *mpd.MPD
	FetchedAt              time.Time
	FinalMPDURL            string
	Mux                    sync.RWMutex
	LastAccessedAt         time.Time
	StopAutoUpdateCh       chan struct{}
	HLSBaseMediaSequence   uint64
	LastMPDPublishTime     time.Time
	InitialBaseURL         string
	InitialBaseURLIsSet    bool
	MasterPlaylist         string
	MediaPlaylists         map[string]string
	SegmentCache           *SegmentCache
	SegmentDownloadSignals sync.Map
}

// HasSegment 检查此条目是否已缓存特定分片
func (e *MPDEntry) HasSegment(segmentKey string) bool {
	return e.SegmentCache.Has(segmentKey)
}

// StoreSegment 在此条目中存储一个分片
func (e *MPDEntry) StoreSegment(segmentKey string, segment *fetch.CachedSegment) {
	e.SegmentCache.Set(segmentKey, segment)
}

// SignalSegmentDownloaded 发送分片已下载的信号
func (e *MPDEntry) SignalSegmentDownloaded(segmentKey string) {
	if ch, ok := e.SegmentDownloadSignals.LoadAndDelete(segmentKey); ok {
		close(ch.(chan struct{}))
	}
}

// PruneSegments 清理此条目中的陈旧分片
func (e *MPDEntry) PruneSegments(validSegments map[string]struct{}, initSegmentTTL time.Duration) {
	e.SegmentCache.Prune(validSegments, initSegmentTTL)
}

// Manager 管理 MPD 缓存
type Manager struct {
	MPDCache  map[string]*MPDEntry
	CacheLock sync.RWMutex
}

// NewManager 创建一个新的缓存管理器
func NewManager() *Manager {
	return &Manager{
		MPDCache: make(map[string]*MPDEntry),
	}
}

// GetEntry 从缓存中检索一个 MPD 条目
func (m *Manager) GetEntry(channelID string) (interface {
	HasSegment(segmentKey string) bool
	StoreSegment(segmentKey string, segment *fetch.CachedSegment)
	SignalSegmentDownloaded(segmentKey string)
}, bool) {
	m.CacheLock.RLock()
	defer m.CacheLock.RUnlock()
	entry, exists := m.MPDCache[channelID]
	return entry, exists
}

// GetOrCreateEntry 获取一个现有的 MPD 条目，如果不存在则创建一个新的
func (m *Manager) GetOrCreateEntry(channelID string) *MPDEntry {
	m.CacheLock.Lock()
	defer m.CacheLock.Unlock()

	entry, ok := m.MPDCache[channelID]
	if !ok {
		entry = &MPDEntry{
			SegmentCache: &SegmentCache{
				Segments: make(map[string]*fetch.CachedSegment),
			},
			// Initialize other fields as needed
		}
		m.MPDCache[channelID] = entry
	}
	return entry
}

// DeleteEntry 从缓存中删除一个条目
func (m *Manager) DeleteEntry(channelID string) {
	m.CacheLock.Lock()
	defer m.CacheLock.Unlock()
	delete(m.MPDCache, channelID)
}
