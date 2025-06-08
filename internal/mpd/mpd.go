package mpd

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// ContentProtection 表示 ContentProtection XML 元素。
type ContentProtection struct {
	XMLName     xml.Name `xml:"ContentProtection"`
	SchemeIDURI string   `xml:"schemeIdUri,attr"`
	Value       string   `xml:"value,attr,omitempty"`
	// 解析 cenc:default_KID 属性
	CencDefaultKID string `xml:"urn:mpeg:cenc:2013 cenc:default_KID,attr,omitempty"`
	PSSH           string `xml:"pssh,omitempty"`
	MsprPro        string `xml:"pro,omitempty"`
}

// S 表示 SegmentTimeline 中的一个段。
type S struct {
	XMLName xml.Name `xml:"S"`
	T       *uint64  `xml:"t,attr"`
	D       uint64   `xml:"d,attr"`
	R       *int     `xml:"r,attr"`
}

// SegmentTimeline 表示 SegmentTimeline XML 元素。
type SegmentTimeline struct {
	XMLName  xml.Name `xml:"SegmentTimeline"`
	Segments []S      `xml:"S"`
}

// SegmentTemplate 表示 SegmentTemplate XML 元素。
type SegmentTemplate struct {
	XMLName                xml.Name         `xml:"SegmentTemplate"`
	Timescale              *uint64          `xml:"timescale,attr"`
	Initialization         string           `xml:"initialization,attr,omitempty"`
	Media                  string           `xml:"media,attr"`
	PresentationTimeOffset *uint64          `xml:"presentationTimeOffset,attr,omitempty"`
	SegmentTimeline        *SegmentTimeline `xml:"SegmentTimeline"`
}

// Representation 表示 Representation XML 元素。
type Representation struct {
	XMLName            xml.Name `xml:"Representation"`
	ID                 string   `xml:"id,attr"`
	Bandwidth          uint     `xml:"bandwidth,attr"`
	Width              uint     `xml:"width,attr,omitempty"`
	Height             uint     `xml:"height,attr,omitempty"`
	FrameRate          string   `xml:"frameRate,attr,omitempty"`
	Sar                string   `xml:"sar,attr,omitempty"`
	Codecs             string   `xml:"codecs,attr,omitempty"`
	MimeType           string   `xml:"mimeType,attr,omitempty"`
	AudioSamplingRate  string   `xml:"audioSamplingRate,attr,omitempty"`
	AudioChannelConfig *struct {
		SchemeIDURI string `xml:"schemeIdUri,attr"`
		Value       string `xml:"value,attr"`
	} `xml:"AudioChannelConfiguration,omitempty"`
	SegmentTemplate *SegmentTemplate `xml:"SegmentTemplate"`
}

// AdaptationSet 表示 AdaptationSet XML 元素。
type AdaptationSet struct {
	XMLName            xml.Name            `xml:"AdaptationSet"`
	ID                 string              `xml:"id,attr,omitempty"`
	ContentType        string              `xml:"contentType,attr,omitempty"`
	Lang               string              `xml:"lang,attr,omitempty"`
	MimeType           string              `xml:"mimeType,attr"`
	SegmentAlignment   bool                `xml:"segmentAlignment,attr,omitempty"`
	StartWithSAP       int                 `xml:"startWithSAP,attr,omitempty"`
	MaxWidth           uint                `xml:"maxWidth,attr,omitempty"`
	MaxHeight          uint                `xml:"maxHeight,attr,omitempty"`
	MinFrameRate       string              `xml:"minFrameRate,attr,omitempty"`
	MaxFrameRate       string              `xml:"maxFrameRate,attr,omitempty"`
	Par                string              `xml:"par,attr,omitempty"`
	ContentProtections []ContentProtection `xml:"ContentProtection"`
	Representations    []Representation    `xml:"Representation"`
	SegmentTemplate    *SegmentTemplate    `xml:"SegmentTemplate"`
	BaseURL            string              `xml:"BaseURL,omitempty"`
}

// Period 表示 Period XML 元素。
type Period struct {
	XMLName        xml.Name        `xml:"Period"`
	ID             string          `xml:"id,attr,omitempty"`
	Start          string          `xml:"start,attr,omitempty"`
	BaseURLs       []string        `xml:"BaseURL"`
	AdaptationSets []AdaptationSet `xml:"AdaptationSet"`
}

// MPD 表示根 MPD XML 元素。
type MPD struct {
	XMLName               xml.Name `xml:"urn:mpeg:dash:schema:mpd:2011 MPD"`
	Profiles              string   `xml:"profiles,attr"`
	Type                  string   `xml:"type,attr"`
	MinimumUpdatePeriod   string   `xml:"minimumUpdatePeriod,attr,omitempty"`
	TimeShiftBufferDepth  string   `xml:"timeShiftBufferDepth,attr,omitempty"`
	AvailabilityStartTime string   `xml:"availabilityStartTime,attr,omitempty"`
	PublishTime           string   `xml:"publishTime,attr,omitempty"`
	MaxSegmentDuration    string   `xml:"maxSegmentDuration,attr,omitempty"`
	MinBufferTime         string   `xml:"minBufferTime,attr,omitempty"`
	Periods               []Period `xml:"Period"`
	BaseURLs              []string `xml:"BaseURL"`
}

func parseDuration(dashDuration string) (time.Duration, error) {
	if !strings.HasPrefix(dashDuration, "PT") {
		err := fmt.Errorf("invalid DASH duration format: missing PT prefix: %s", dashDuration)
		slog.Error("Failed to parse DASH duration", "duration", dashDuration, "error", err)
		return 0, err
	}
	trimmed := strings.TrimPrefix(dashDuration, "PT")
	if strings.Contains(trimmed, "H") || strings.Contains(trimmed, "M") {
		if strings.HasSuffix(trimmed, "S") && !strings.Contains(trimmed, "H") && !strings.Contains(trimmed, "M") {
			return time.ParseDuration(strings.ToLower(trimmed))
		}
		err := fmt.Errorf("complex DASH duration parsing not yet fully implemented for: %s", dashDuration)
		slog.Error("Failed to parse complex DASH duration", "duration", dashDuration, "error", err)
		return 0, err
	}
	return time.ParseDuration(strings.ToLower(trimmed))
}

func (m *MPD) GetMinimumUpdatePeriod() (time.Duration, error) {
	if m.MinimumUpdatePeriod == "" {
		err := fmt.Errorf("MinimumUpdatePeriod is not set in MPD")
		slog.Warn("Cannot get MinimumUpdatePeriod", "error", err)
		return 0, err
	}
	return parseDuration(m.MinimumUpdatePeriod)
}

func (m *MPD) GetMaxSegmentDuration() (time.Duration, error) {
	if m.MaxSegmentDuration == "" {
		err := fmt.Errorf("MaxSegmentDuration is not set in MPD and inference is not yet implemented")
		slog.Warn("Cannot get MaxSegmentDuration", "error", err)
		return 0, err
	}
	return parseDuration(m.MaxSegmentDuration)
}

func FetchAndParseMPD(ctx context.Context, client *http.Client, initialMPDURL string, userAgent string) (string, *MPD, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", initialMPDURL, nil)
	if err != nil {
		slog.Error("Failed to create request for MPD", "url", initialMPDURL, "error", err)
		return "", nil, fmt.Errorf("failed to create request for MPD: %w", err)
	}
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	req.Header.Set("Accept", "application/dash+xml,application/xml,text/xml")

	resp, err := client.Do(req)
	if err != nil {
		slog.Error("Failed to fetch MPD", "url", initialMPDURL, "error", err)
		return "", nil, fmt.Errorf("failed to fetch MPD from %s: %w", initialMPDURL, err)
	}
	defer resp.Body.Close()

	finalMPDURL := resp.Request.URL.String()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		err := fmt.Errorf("failed to fetch MPD: status %s, initial_url %s, final_url %s, body: %s", resp.Status, initialMPDURL, finalMPDURL, string(bodyBytes))
		slog.Error("Failed to fetch MPD with non-200 status", "status", resp.Status, "initial_url", initialMPDURL, "final_url", finalMPDURL, "error", err)
		return finalMPDURL, nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Error("Failed to read MPD response body", "url", finalMPDURL, "error", err)
		return finalMPDURL, nil, fmt.Errorf("failed to read MPD response body: %w", err)
	}

	var mpdData MPD
	if err := xml.Unmarshal(body, &mpdData); err != nil {
		bodySnippet := string(body)
		if len(bodySnippet) > 512 {
			bodySnippet = bodySnippet[:512] + "..."
		}
		err := fmt.Errorf("failed to unmarshal MPD XML: %w. XML Body Snippet: %s", err, bodySnippet)
		slog.Error("Failed to unmarshal MPD XML", "url", finalMPDURL, "error", err)
		return finalMPDURL, nil, err
	}
	return finalMPDURL, &mpdData, nil
}

func (as *AdaptationSet) GetDefaultKID() string {
	if as == nil {
		slog.Warn("GetDefaultKID called on nil AdaptationSet")
		return ""
	}
	logger := slog.With("module", "mpd", "as_id", as.ID, "content_type", as.ContentType, "lang", as.Lang)
	logger.Debug("Checking for default KID", "cp_count", len(as.ContentProtections))

	for i, cp := range as.ContentProtections {
		cpLogger := logger.With("cp_index", i, "scheme", cp.SchemeIDURI, "value", cp.Value, "kid_attr", cp.CencDefaultKID)
		isCENCSystem := strings.ToLower(cp.SchemeIDURI) == "urn:mpeg:dash:mp4protection:2011" && cp.Value == "cenc"

		if isCENCSystem {
			if cp.CencDefaultKID != "" {
				kid := strings.ReplaceAll(cp.CencDefaultKID, "-", "")
				cpLogger.Info("Found CENC ContentProtection with default KID", "processed_kid", kid)
				return kid
			} else {
				cpLogger.Debug("CENC ContentProtection found, but cenc:default_KID attribute is empty or missing.")
			}
		}
	}
	logger.Debug("No suitable cenc:default_KID found in AdaptationSet")
	return ""
}
