package playlist

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"

	"dash2hlsproxy/internal/mpd"
)

// 在实时媒体播放列表中保留的段数
const numLiveSegments = 5

// GenerateMasterPlaylist generates the HLS master playlist string and returns a list of selected representation IDs.
func GenerateMasterPlaylist(mpdData *mpd.MPD, channelID string) (string, []string, error) {
	var playlist bytes.Buffer
	playlist.WriteString("#EXTM3U\n")
	playlist.WriteString("#EXT-X-VERSION:7\n")

	var selectedRepIDs []string
	audioGroupID := "audio_grp"
	subtitleGroupID := "subs_grp"
	hasAudio := false
	hasSubtitles := false

	// Select all audio and subtitle representations
	for _, period := range mpdData.Periods {
		for _, as := range period.AdaptationSets {
			if as.ContentType == "audio" || strings.Contains(as.MimeType, "audio") {
				hasAudio = true
				for _, rep := range as.Representations {
					selectedRepIDs = append(selectedRepIDs, rep.ID)
				}
			} else if as.ContentType == "text" ||
				strings.Contains(as.MimeType, "text") ||
				strings.Contains(as.MimeType, "application/ttml+xml") ||
				(strings.Contains(as.MimeType, "application/mp4") && len(as.Representations) > 0 && strings.Contains(as.Representations[0].Codecs, "wvtt")) {
				hasSubtitles = true
				for _, rep := range as.Representations {
					selectedRepIDs = append(selectedRepIDs, rep.ID)
				}
			}
		}
	}

	// Find the single best video representation across all video adaptation sets
	var bestVideoRep *mpd.Representation
	var maxBandwidth uint
	for _, period := range mpdData.Periods {
		for _, as := range period.AdaptationSets {
			if as.ContentType == "video" || strings.Contains(as.MimeType, "video") {
				for i := range as.Representations {
					rep := &as.Representations[i]
					if rep.Bandwidth > maxBandwidth {
						maxBandwidth = rep.Bandwidth
						bestVideoRep = rep
					}
				}
			}
		}
	}

	if bestVideoRep != nil {
		selectedRepIDs = append(selectedRepIDs, bestVideoRep.ID)
	}

	// Generate #EXT-X-MEDIA for audio
	audioStreamIndex := 0
	for _, period := range mpdData.Periods {
		for _, as := range period.AdaptationSets {
			if as.ContentType == "audio" || strings.Contains(as.MimeType, "audio") {
				if len(as.Representations) > 0 {
					repCodecs := as.Representations[0].Codecs
					lang := as.Lang
					if lang == "" {
						lang = fmt.Sprintf("audio%d", audioStreamIndex)
					}
					name := fmt.Sprintf("Audio %s", lang)
					isDefault := "NO"
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
				}
			}
		}
	}

	// Generate #EXT-X-MEDIA for subtitles
	subtitleStreamIndex := 0
	for _, period := range mpdData.Periods {
		for _, as := range period.AdaptationSets {
			if as.ContentType == "text" ||
				strings.Contains(as.MimeType, "text") ||
				strings.Contains(as.MimeType, "application/ttml+xml") ||
				(strings.Contains(as.MimeType, "application/mp4") && len(as.Representations) > 0 && strings.Contains(as.Representations[0].Codecs, "wvtt")) {
				if len(as.Representations) > 0 {
					lang := as.Lang
					if lang == "" {
						lang = fmt.Sprintf("sub%d", subtitleStreamIndex)
					}
					name := fmt.Sprintf("Subtitles %s", lang)
					isDefault := "NO"
					mediaPlaylistPath := fmt.Sprintf("/hls/%s/subtitles/%s/playlist.m3u8", channelID, lang)
					playlist.WriteString(fmt.Sprintf("#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"%s\",NAME=\"%s\",LANGUAGE=\"%s\",AUTOSELECT=YES,DEFAULT=%s,URI=\"%s\"\n",
						subtitleGroupID, name, lang, isDefault, mediaPlaylistPath))
					subtitleStreamIndex++
				}
			}
		}
	}

	// Generate #EXT-X-STREAM-INF for the best video representation
	if bestVideoRep != nil {
		var streamInf strings.Builder
		streamInf.WriteString("#EXT-X-STREAM-INF:PROGRAM-ID=1")
		if bestVideoRep.Bandwidth > 0 {
			streamInf.WriteString(fmt.Sprintf(",BANDWIDTH=%d", bestVideoRep.Bandwidth))
			streamInf.WriteString(fmt.Sprintf(",AVERAGE-BANDWIDTH=%d", bestVideoRep.Bandwidth))
		}
		if bestVideoRep.Width > 0 && bestVideoRep.Height > 0 {
			streamInf.WriteString(fmt.Sprintf(",RESOLUTION=%dx%d", bestVideoRep.Width, bestVideoRep.Height))
		}
		if bestVideoRep.Codecs != "" {
			streamInf.WriteString(fmt.Sprintf(",CODECS=\"%s\"", bestVideoRep.Codecs))
		}
		if hasAudio {
			streamInf.WriteString(fmt.Sprintf(",AUDIO=\"%s\"", audioGroupID))
		}
		if hasSubtitles {
			streamInf.WriteString(fmt.Sprintf(",SUBTITLES=\"%s\"", subtitleGroupID))
		}
		playlist.WriteString(streamInf.String() + "\n")
		videoMediaPlaylistPath := fmt.Sprintf("/hls/%s/video/%s/playlist.m3u8", channelID, bestVideoRep.ID)
		playlist.WriteString(videoMediaPlaylistPath + "\n")
	}

	return playlist.String(), selectedRepIDs, nil
}

// GenerateMediaPlaylists generates HLS media playlists only for the selected representations.
func GenerateMediaPlaylists(logger *slog.Logger, mpdData *mpd.MPD, finalMPDURLStr string, channelID string, hlsBaseMediaSequence uint64, parsedKey []byte, selectedRepIDs []string) (map[string]string, map[string]string, map[string]struct{}, error) {
	playlists := make(map[string]string)
	segmentsToPreload := make(map[string]string)
	validSegments := make(map[string]struct{})

	selectedRepIDSet := make(map[string]struct{})
	for _, id := range selectedRepIDs {
		selectedRepIDSet[id] = struct{}{}
	}

	finalMPDURL, errParseMPDURL := url.Parse(finalMPDURLStr)
	if errParseMPDURL != nil {
		logger.Error("Error parsing finalMPDURLStr", "url", finalMPDURLStr, "error", errParseMPDURL)
		return nil, nil, nil, fmt.Errorf("error parsing final MPD URL: %w", errParseMPDURL)
	}

	for _, period := range mpdData.Periods {
		currentPeriodBase := finalMPDURL
		if len(period.BaseURLs) > 0 && period.BaseURLs[0] != "" {
			periodLevelBase, errParsePBase := url.Parse(period.BaseURLs[0])
			if errParsePBase == nil {
				currentPeriodBase = currentPeriodBase.ResolveReference(periodLevelBase)
			} else {
				logger.Warn("Error parsing Period BaseURL. Using parent base.", "base_url", period.BaseURLs[0], "parent_base", currentPeriodBase.String(), "error", errParsePBase)
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
				continue
			}

			for _, rep := range as.Representations {
				if _, ok := selectedRepIDSet[rep.ID]; !ok {
					continue // Skip representations that were not selected in the master playlist
				}

				var qualityOrLangKey string
				if streamType == "video" {
					qualityOrLangKey = rep.ID
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
					logger.Warn("Empty qualityOrLangKey. Skipping.", "content_type", as.ContentType, "lang", as.Lang, "rep_id", rep.ID)
					continue
				}

				playlistKey := fmt.Sprintf("%s/%s", streamType, qualityOrLangKey)
				var playlistBuf bytes.Buffer
				playlistBuf.WriteString("#EXTM3U\n")
				playlistBuf.WriteString("#EXT-X-VERSION:7\n")
				playlistBuf.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")

				segTemplate := rep.SegmentTemplate
				if segTemplate == nil {
					segTemplate = as.SegmentTemplate
				}

				if segTemplate == nil || segTemplate.SegmentTimeline == nil || len(segTemplate.SegmentTimeline.Segments) == 0 {
					logger.Warn("SegmentTemplate or SegmentTimeline missing/empty. Skipping playlist.", "rep_id", rep.ID, "channel_id", channelID, "playlist_key", playlistKey)
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
						logger.Warn("Error parsing AS.BaseURL. Using Period base.", "as_base_url", as.BaseURL, "period_base", asBase.String(), "error", errParseASBase)
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
					validSegments[hlsInitSegmentURL] = struct{}{}
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
							logger.Warn("MPD Media template does not contain $Time$ or $Number$. Using template directly.", "stream_type", streamType, "quality_or_lang", qualityOrLangKey, "template", tempPath)
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
					logger.Warn("No segments generated for key", "playlist_key", playlistKey, "channel_id", channelID)
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
					// Add to valid segments set
					validSegments[seg.HLSURL] = struct{}{}
				}

				if mpdData.Type == "static" {
					playlistBuf.WriteString("#EXT-X-ENDLIST\n")
				}
				playlists[playlistKey] = playlistBuf.String()
			}
		}
	}
	if len(playlists) == 0 {
		logger.Warn("generateMediaPlaylists produced no playlists", "channel_id", channelID)
	}
	return playlists, segmentsToPreload, validSegments, nil
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
