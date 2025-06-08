package downloader

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"dash2hlsproxy/internal/fetch"
)

// 全局最大并发下载数
const globalMaxConcurrentDownloads = 10

// CacheAccessor 定义了 Downloader 与缓存交互所需的合约
type CacheAccessor interface {
	GetEntry(channelID string) (entry interface {
		HasSegment(segmentKey string) bool
		StoreSegment(segmentKey string, segment *fetch.CachedSegment)
		SignalSegmentDownloaded(segmentKey string)
	}, exists bool)
}

// Downloader 管理分片下载
type Downloader struct {
	cacheAccessor          CacheAccessor
	fetcher                *fetch.Fetcher
	downloadQueue          chan *fetch.SegmentDownloadTask
	activeDownloads        chan struct{}
	stopCh                 chan struct{}
	wg                     sync.WaitGroup
	logger                 *slog.Logger
	segmentDownloadTimeout time.Duration
}

// NewDownloader 创建一个新的 Downloader 实例
func NewDownloader(logger *slog.Logger, accessor CacheAccessor, fetcher *fetch.Fetcher) *Downloader {
	return &Downloader{
		logger:                 logger,
		cacheAccessor:          accessor,
		fetcher:                fetcher,
		downloadQueue:          make(chan *fetch.SegmentDownloadTask, 200),
		activeDownloads:        make(chan struct{}, globalMaxConcurrentDownloads),
		stopCh:                 make(chan struct{}),
		segmentDownloadTimeout: 15 * time.Second, // 可配置的超时
	}
}

// Start 启动下载器的 worker 协程
func (d *Downloader) Start(ctx context.Context) {
	for i := 0; i < globalMaxConcurrentDownloads; i++ {
		d.wg.Add(1)
		go d.worker(ctx)
	}
	d.logger.Info("Downloader: Started worker goroutines", "count", globalMaxConcurrentDownloads)
}

// Stop 停止下载器
func (d *Downloader) Stop() {
	close(d.stopCh)
	d.wg.Wait()
	d.logger.Info("Downloader: Stopped.")
}

// EnqueueTask 将下载任务加入队列
func (d *Downloader) EnqueueTask(task *fetch.SegmentDownloadTask) {
	select {
	case d.downloadQueue <- task:
		d.logger.Debug("Downloader: Added segment to download queue", "segment_key", task.SegmentKey, "channel_id", task.ChannelID)
	default:
		d.logger.Warn("Downloader: Download queue is full. Dropping segment.", "segment_key", task.SegmentKey, "channel_id", task.ChannelID)
	}
}

// worker 协程从下载队列中获取任务并执行下载
func (d *Downloader) worker(ctx context.Context) {
	defer d.wg.Done()
	for {
		select {
		case task := <-d.downloadQueue:
			d.activeDownloads <- struct{}{} // Acquire a slot

			cachedEntry, exists := d.cacheAccessor.GetEntry(task.ChannelID)
			if !exists {
				d.logger.Warn("Downloader worker: Channel entry not found. Skipping download.", "channel_id", task.ChannelID, "segment_key", task.SegmentKey)
				// Even if the entry doesn't exist, we must signal completion to unblock any potential waiters.
				// This is a safe operation.
				d.signalDownloadCompletion(task.ChannelID, task.SegmentKey)
				<-d.activeDownloads // Release slot
				continue
			}

			if cachedEntry.HasSegment(task.SegmentKey) {
				d.signalDownloadCompletion(task.ChannelID, task.SegmentKey)
				<-d.activeDownloads // Release slot
				continue
			}

			const maxRetries = 3
			var data []byte
			var contentType string
			var err error

			for i := 0; i < maxRetries; i++ {
				downloadCtx, cancel := context.WithTimeout(ctx, d.segmentDownloadTimeout)
				data, contentType, err = d.fetcher.FetchSegment(downloadCtx, task.UpstreamURL, task.UserAgent)
				cancel() // 及时释放资源

				if err == nil {
					break // Download successful
				}

				d.logger.Warn("Downloader worker: Failed to download segment, retrying...",
					"segment_key", task.SegmentKey,
					"channel_id", task.ChannelID,
					"retry_count", i+1,
					"error", err)

				// Exponential backoff
				time.Sleep(time.Second * time.Duration(1<<i))
			}

			if err != nil {
				d.logger.Error("Downloader worker: Failed to download segment after all retries", "segment_key", task.SegmentKey, "channel_id", task.ChannelID, "error", err)
				d.signalDownloadCompletion(task.ChannelID, task.SegmentKey)
			} else {
				cachedEntry.StoreSegment(task.SegmentKey, &fetch.CachedSegment{
					Data:        data,
					ContentType: contentType,
					FetchedAt:   time.Now(),
				})
				d.logger.Debug("Downloader worker: Successfully cached segment", "segment_key", task.SegmentKey, "channel_id", task.ChannelID)
				d.signalDownloadCompletion(task.ChannelID, task.SegmentKey)
			}

			<-d.activeDownloads // Release slot

		case <-d.stopCh:
			d.logger.Info("Downloader worker: Stop signal received. Exiting.")
			return
		case <-ctx.Done():
			d.logger.Info("Downloader worker: Context cancelled. Exiting.")
			return
		}
	}
}

// signalDownloadCompletion 通知等待者分片下载已完成（或失败）
func (d *Downloader) signalDownloadCompletion(channelID, segmentKey string) {
	if entry, exists := d.cacheAccessor.GetEntry(channelID); exists {
		entry.SignalSegmentDownloaded(segmentKey)
	}
}
