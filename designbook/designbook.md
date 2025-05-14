# Software Design Document: DASH MPD to HLS Live Proxy

**Version:** 1.0  
**Date:** 2025-05-08

## Table of Contents:
1. Introduction
   1.1. Purpose 
   1.2. System Overview 
   1.3. Definitions and Acronyms
2. Requirements
   2.1. Functional Requirements
   2.2. Non-Functional Requirements
3. System Architecture
   3.1. High-Level Architecture
   3.2. Components
4. Detailed Design
   4.1. Channel Management
   4.2. MPD Fetching and Parsing
   4.3. HLS Playlist Generation
   4.4. Segment Proxying
   4.5. Key Server
   4.6. HTTP Endpoints and Routing
5. Data Structures
6. Error Handling
7. Caching Strategy
8. Concurrency Model
9. Go Language Specifics

## 1. Introduction

### 1.1. Purpose
This document outlines the design for a Go-based proxy application that converts DASH (Dynamic Adaptive Streaming over HTTP) MPD (Media Presentation Description) live streams into HLS (HTTP Live Streaming) format. The application will also provide a key server for client-side decryption of encrypted content.

### 1.2. System Overview
The proxy will receive HTTP requests for HLS playlists and segments. Based on a channel identifier in the request URL, it will fetch the corresponding DASH MPD from an upstream server, parse it, and generate HLS master and media playlists. These playlists will support multiple video resolutions, audio tracks, and subtitles as defined in the MPD. For encrypted streams, the proxy will not perform decryption but will serve decryption keys to the client via a dedicated key server endpoint, using key information provided in a local configuration file (channels.json).

### 1.3. Definitions and Acronyms
- DASH: Dynamic Adaptive Streaming over HTTP
- MPD: Media Presentation Description (XML manifest for DASH)
- HLS: HTTP Live Streaming
- M3U8: Playlist file format for HLS
- KID: Key ID, used to identify a specific decryption key
- CENC: Common Encryption
- API: Application Programming Interface

## 2. Requirements

### 2.1. Functional Requirements
- FR1: The system shall accept HTTP GET requests in the format http://<server_ip>:<port>/hls/<channel_id>.
- FR2: The system shall use the <channel_id> to look up the corresponding DASH MPD manifest URL and encryption key information from the channels.json file.
- FR3: The system shall fetch the DASH MPD from the configured manifest URL for the requested channel.
- FR4: The system shall parse the MPD to identify available video, audio, and subtitle streams, including their respective representations (resolutions, bitrates, languages).
- FR5: The system shall generate an HLS master playlist (.m3u8) that includes:
  - #EXT-X-STREAM-INF tags for each video resolution found in the MPD.
  - #EXT-X-MEDIA tags for available audio tracks, grouped by a common GROUP-ID and linked to video streams.
  - #EXT-X-MEDIA tags for available subtitle tracks, grouped by a common GROUP-ID and linked to video streams.
- FR6: The system shall generate HLS media playlists (.m3u8) for each video, audio, and subtitle representation.
- FR7: Media playlists for encrypted content shall include #EXT-X-KEY tags specifying:
  - METHOD=AES-128.
  - A URI pointing to the system's internal key server, including the KID (e.g., derived from cenc:default_KID in the MPD).
  - KEYFORMAT="identity".
  - An IV (Initialization Vector), which can be derived from the KID (if it's 16 bytes). The index.xml example shows a cenc:default_KID of 0958b9c6-5762-2c46-5a62-05eb2252b8ed, which is 16 bytes when hyphens are removed.
- FR8: The system shall proxy requests for media segments from the client to the upstream DASH server, constructing the correct upstream segment URL based on the MPD's BaseURL and SegmentTemplate.
- FR9: The system shall provide a key server endpoint (e.g., /key?kid=<KID_value>).
- FR10: The key server shall, upon request, retrieve the appropriate decryption key from the channels.json data based on the provided KID and return the raw key bytes to the client. The channels.json contains KID:Key pairs (e.g., "0958b9c657622c465a6205eb2252b8ed:2d2fd7b1661b1e28de38268872b48480" for channel "jade").
- FR11: The proxy shall not perform any content decryption itself.
- FR12: The system shall periodically refresh the MPD for live streams, according to the minimumUpdatePeriod specified in the MPD (e.g., "PT8S" in index.xml), and update the HLS playlists accordingly.
- FR13: The system shall use the User-Agent specified in channels.json when making upstream requests if provided.

### 2.2. Non-Functional Requirements
- NFR1: The system should be performant and handle multiple concurrent client requests efficiently.
- NFR2: The system should be resilient to temporary upstream errors (e.g., network issues when fetching MPD or segments) and implement appropriate retry mechanisms or error reporting.
- NFR3: The system should be easily configurable through the channels.json file.
- NFR4: The system should have logging for diagnostic purposes.

## 3. System Architecture

### 3.1. High-Level Architecture
```
+-------------------+ HTTP +-----------------------+ HTTP +---------------------+
| HLS Client        |<--------------->| Go Proxy Application |<--------------->| Upstream DASH Server|
| (Player)          | (M3U8, TS)      |                     | (MPD, Segments)  | (e.g., mytv.115082.com) |
+-------------------+                 +-----------------------+                 +---------------------+
                                      | ^
                                      | | (Key Request)
                                      | |
                                      | +-----------------------+
                                      | Key Server Endpoint    |
                                      +-----------------------+
                                      | (Reads)
                                      V
                                      +-----------------------+
                                      | channels.json         |
                                      +-----------------------+
```

### 3.2. Components

- **Channel Manager**:
  - Responsible for loading and managing channel configurations from channels.json.
  - Provides channel-specific information (MPD URL, keys) to other components.

- **HTTP Request Handler**:
  - The main entry point for client requests.
  - Routes requests to appropriate handlers based on the URL path (master playlist, media playlist, segment, key).

- **MPD Fetcher & Parser**:
  - Fetches the MPD XML from the upstream server for a given channel.
  - Parses the MPD XML content (e.g., using encoding/xml in Go).
  - Extracts stream metadata: Adaptation Sets, Representations, Content Protection (KID), Base URLs, Segment Templates, and live stream update parameters.

- **HLS Playlist Generator**:
  - Master Playlist Generator: Creates the main .m3u8 file listing available video qualities, audio, and subtitle tracks with links to their respective media playlists.
  - Media Playlist Generator: Creates individual .m3u8 files for each video, audio, and subtitle stream, including segment information and encryption details (#EXT-X-KEY).

- **Segment Proxy**:
  - Handles requests for media segments (.ts, .cmfv, .cmfa, .cmft etc., based on MPD).
  - Constructs the upstream DASH segment URL using information from the parsed MPD.
  - Fetches the segment from the upstream server and streams it back to the client.

- **Key Server**:
  - An HTTP endpoint that serves decryption keys.
  - Receives requests containing a KID.
  - Looks up the key associated with the KID (and potentially channel ID) in the channels.json data.
  - Returns the raw key bytes.

## 4. Detailed Design

### 4.1. Channel Management
- On startup, the application will load channels.json into memory.
- A map or slice of structs will store channel data, indexed by channelId.
  - Each entry will contain the Manifest URL, User-Agent, and a map of KIDs to Keys (hex strings initially).
  - Keys will be pre-parsed from KID_hex:Key_hex format into a more usable structure (e.g., map[string]string).

### 4.2. MPD Fetching and Parsing
- When a channel is requested for the first time, or when the MPD needs refreshing:
  - The MPD Fetcher will make an HTTP GET request to the channel's Manifest URL (from channels.json), using the configured UserAgent if available.
  - The fetched XML will be parsed. Key elements to extract include:
    - MPD@type, MPD@minimumUpdatePeriod, MPD@availabilityStartTime, MPD@publishTime.
    - Period/BaseURL.
    - AdaptationSet elements for video, audio, and text (subtitles).
    - Attributes: id, contentType, lang, mimeType.
    - ContentProtection elements, specifically cenc:default_KID.
    - Role elements.
    - Representation elements within each AdaptationSet.
    - Attributes: id, bandwidth, width, height, codecs, audioSamplingRate.
    - AudioChannelConfiguration.
    - SegmentTemplate (attributes: timescale, initialization, media).
    - SegmentTimeline and its S elements (attributes: t, d, r).
- The parsed MPD data will be stored in Go structs, possibly cached per channel.
- A background goroutine per active channel will handle periodic MPD refresh based on minimumUpdatePeriod.

### 4.3. HLS Playlist Generation

#### 4.3.1. Master Playlist
- Generated when a client requests /hls/<channel_id>.
- Structure:

```
#EXTM3U
#EXT-X-VERSION:3 (or higher, depending on features used)
# --- Audio Tracks ---
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio_grp",NAME="English",LANGUAGE="eng",AUTOSELECT=YES,DEFAULT=YES,URI="audio_eng/playlist.m3u8"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio_grp",NAME="Spanish",LANGUAGE="spa",AUTOSELECT=YES,DEFAULT=NO,URI="audio_spa/playlist.m3u8"
# (Based on AdaptationSet contentType="audio" lang attributes from MPD)
# --- Subtitle Tracks ---
#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs_grp",NAME="English Subs",LANGUAGE="eng",AUTOSELECT=YES,DEFAULT=YES,URI="subtitles_eng/playlist.m3u8"
# (Based on AdaptationSet contentType="text" lang attributes from MPD)

# --- Video Renditions ---
#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=<bandwidth>,RESOLUTION=<width>x<height>,CODECS="<codecs>",AUDIO="audio_grp",SUBTITLES="subs_grp"
video_1080p/playlist.m3u8
#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=<bandwidth2>,RESOLUTION=<width2>x<height2>,CODECS="<codecs2>",AUDIO="audio_grp",SUBTITLES="subs_grp"
video_720p/playlist.m3u8
# (Based on video AdaptationSet and its Representation elements from MPD)
```

- AUDIO and SUBTITLES attributes will point to the GROUP-IDs defined in #EXT-X-MEDIA tags.
- URIs for media playlists will be relative to the master playlist path.

#### 4.3.2. Media Playlists (Video, Audio, Subtitles)
- Generated dynamically when a client requests a specific media playlist (e.g., /hls/<channel_id>/video_1080p/playlist.m3u8).
- Structure:

```
#EXTM3U
#EXT-X-VERSION:3 (or higher)
#EXT-X-TARGETDURATION:<max_segment_duration_seconds> (Calculated from MPD maxSegmentDuration or SegmentTimeline)
#EXT-X-MEDIA-SEQUENCE:<sequence_number> (Incrementing for live streams)
# If encrypted:
#EXT-X-KEY:METHOD=AES-128,URI="<key_server_url_base>/key?kid=<default_KID_from_MPD>",KEYFORMAT="identity",IV=0x<default_KID_as_IV_hex_if_applicable>
#EXTINF:<duration_seconds>,
<segment_base_url>/<RepresentationID>/D<TimeValue>.cmfv (Example for video segments from index.xml)
#EXTINF:<duration_seconds>,
...
#EXT-X-ENDLIST (If VOD or if stream ends, not for typical live)
```

- Segment URLs will be constructed based on the MPD's BaseURL, SegmentTemplate@media, and SegmentTimeline. The $RepresentationID$ and $Time$ (or $Number$) placeholders will be filled.
- The Time values are derived from the MPD's SegmentTimeline S@t values.
- For live streams, the playlist needs to be updated with new segments as the MPD's SegmentTimeline progresses.
- The #EXT-X-KEY URI will include the cenc:default_KID (e.g., 0958b9c6-5762-2c46-5a62-05eb2252b8ed from index.xml) as a query parameter to the key server. The IV will be the same KID value (16 bytes), formatted as a hex string prefixed with 0x.

### 4.4. Segment Proxying
- Client requests for segments (e.g., /hls/<channel_id>/video_1080p/D<timestamp>.cmfv).
- The handler extracts channel, representation, and segment timing/name.
- It constructs the full upstream DASH segment URL. For example, using BaseURL from MPD 7/ and SegmentTemplate@media like $RepresentationID$/D$Time$.cmfv:
  http://<upstream_dash_host>/<mpd_path_prefix_if_any>/7/v5000000_33/D313881042240000.cmfv
- An HTTP GET request is made to the upstream server using the UserAgent from channels.json.
- Response headers (e.g., Content-Type) from the upstream are relayed.
- The segment data is streamed directly to the client.
- Appropriate caching headers (e.g., Cache-Control: no-cache for live segments) should be considered.

### 4.5. Key Server
- Endpoint: e.g., /key?kid=<KID_value>. (The channelid might be implicitly part of the base URL or not strictly needed if KIDs are globally unique across channels, but it's safer to scope by channel if ambiguity could arise).
  - Example from index.xml: the #EXT-X-KEY:URI could be http://<proxy_ip>:<port>/key?kid=0958b9c6-5762-2c46-5a62-05eb2252b8ed.
- The handler extracts the kid query parameter.
- It looks up the channelId (e.g., "jade") to access its specific key list from the loaded channels.json.
- It iterates through the Keys array for the channel (e.g., ["0958b9c657622c465a6205eb2252b8ed:2d2fd7b1661b1e28de38268872b48480"]).
- It parses each entry (e.g., KID_from_file:Key_hex_from_file).
- If KID_from_file (after removing hyphens if the MPD KID has them, ensuring consistent comparison) matches the requested kid (e.g., 0958b9c657622c465a6205eb2252b8ed), the corresponding Key_hex_from_file (e.g., 2d2fd7b1661b1e28de38268872b48480) is used.
- The hex-encoded key string is decoded into raw bytes.
- The raw key bytes (typically 16 bytes for AES-128) are returned as the HTTP response body with Content-Type: application/octet-stream.

### 4.6. HTTP Endpoints and Routing (Example using Go net/http)
- /hls/{channel_id} -> MasterPlaylistHandler(channel_id)
- /hls/{channel_id}/{stream_type}/{quality_or_lang}/playlist.m3u8 -> MediaPlaylistHandler(channel_id, stream_type, quality_or_lang)
  - stream_type could be "video", "audio", "subtitles".
  - quality_or_lang would identify the specific representation (e.g., "1080p", "eng").
- /hls/{channel_id}/{stream_type}/{quality_or_lang}/{segment_filename} -> SegmentProxyHandler(channel_id, stream_type, quality_or_lang, segment_filename)
- /key -> KeyServerHandler(kid_from_query_param)

## 5. Data Structures (Go)

```go
package main

// From channels.json
type ChannelConfig struct {
    Name      string   `json:"Name"`
    ID        string   `json:"Id"`
    Manifest  string   `json:"Manifest"`
    Keys      []string `json:"Keys"` // "KID:Key" hex strings
    UserAgent string   `json:"UserAgent"`
    
    // Internal parsed keys
    ParsedKeys map[string][]byte // map[KID_hex_no_hyphens]RawKeyBytes
}

type AppConfig struct {
    Name      string          `json:"Name"`
    ID        string          `json:"Id"`
    Channels  []ChannelConfig `json:"Channels"`
    
    // Internal map for quick lookup
    ChannelMap map[string]*ChannelConfig
}

// Simplified Parsed MPD Data (Illustrative - actual structs would be more detailed matching MPD schema)
type MPDRepresentation struct {
    ID                   string
    Bandwidth            int
    Width                int
    Height               int
    Codecs               string
    MimeType             string
    Lang                 string // For audio/subtitles
    SegmentTemplateMedia string
    InitializationPattern string
    // ... other relevant fields like audioSampleRate, sar
}

type MPDAdaptationSet struct {
    ID                        string
    ContentType               string // "video", "audio", "text"
    Lang                      string // For audio/text
    Representations           []MPDRepresentation
    DefaultKID                string // cenc:default_KID (hex, no hyphens)
    BaseURL                   string
    SegmentTemplateInitialization string
    SegmentTemplateMedia      string
    SegmentTimelineS          []struct{ T uint64; D uint64; R int }
    Timescale                 uint64
    // ... other relevant fields
}

type ParsedMPD struct {
    Type                string // "dynamic" or "static"
    MinimumUpdatePeriod int    // seconds
    PublishTime         string
    BaseURLs            []string // Global BaseURLs
    AdaptationSets      []MPDAdaptationSet
    // ... other global MPD attributes
}
```

## 6. Error Handling
- Channel Not Found: If <channel_id> from the URL does not exist in channels.json, return HTTP 404.
- MPD Fetch Error: If the upstream MPD cannot be fetched, return HTTP 502 (Bad Gateway) or 504 (Gateway Timeout) after retries. Log the error.
- MPD Parse Error: If the fetched MPD is invalid, return HTTP 500 (Internal Server Error). Log the error.
- Segment Fetch Error: If an upstream segment cannot be fetched, return HTTP 502 or 504 to the client. Log the error.
- Key Not Found: If the Key Server receives a request for a KID not present for the channel, return HTTP 404.
- Graceful handling of empty or malformed channels.json.

## 7. Caching Strategy
- **Channel Configuration (channels.json)**: Loaded once at startup. Restart application to reload.
- **Parsed MPD Data**: Cache the parsed MPD structure per channel in memory.
  - Evict/refresh based on MPD@minimumUpdatePeriod. A per-channel goroutine can manage this.
- **Generated HLS Playlists (Master & Media)**:
  - Can be generated on-demand.
  - For live streams, these need to reflect the latest MPD information, so caching them for a very short duration (e.g., half of segment duration or 1-2 seconds) or regenerating on each valid request might be appropriate.
  - Avoid serving stale playlists that point to expired segments.
- **Media Segments**: Not cached by the proxy itself to avoid disk/memory bloat for live streams. The proxy acts as a pass-through. Client-side and CDN caching are expected.

## 8. Concurrency Model
- The Go net/http server handles each incoming request in a separate goroutine by default.
- Shared data structures (like cached MPD data, channel configurations) must be protected with mutexes (sync.Mutex or sync.RWMutex) to prevent race conditions.
- A dedicated goroutine per active channel for periodic MPD fetching and updating the cached ParsedMPD structure. This goroutine will use a ticker based on minimumUpdatePeriod.

## 9. Go Language Specifics
- HTTP Server: net/http package for handling requests.
- JSON Parsing: encoding/json for channels.json.
- XML Parsing: encoding/xml for MPD.
- Hex Decoding: encoding/hex for keys.
- Concurrency: Goroutines and channels for background tasks (MPD refresh) and managing concurrent access to shared data.
- Logging: Standard log package or a more advanced logging library (e.g., logrus, zap).
- Context Management: Use context.Context for request-scoped values, cancellation, and timeouts, especially for upstream HTTP calls.