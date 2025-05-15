# dash2hlsproxy

`dash2hlsproxy` 是一个将 DASH (Dynamic Adaptive Streaming over HTTP) 流转换为 HLS (HTTP Live Streaming) 流的代理服务器。

## 功能

*   将 DASH 清单转换为 HLS 主播放列表和媒体播放列表。
*   支持通过配置文件定义多个频道。
*   支持通过环境变量进行灵活配置。

## 通过 Docker 运行

本项目提供了 Dockerfile 以方便容器化部署。

### 构建 Docker 镜像

在项目根目录下执行以下命令来构建 Docker 镜像：

```bash
docker build -t dash2hlsproxy-app .
```

### 运行 Docker 容器

您可以通过以下命令运行 Docker 容器。请确保根据您的实际情况调整参数：

*   **端口映射**: 使用 `-p <host_port>:<container_port>` 将容器的端口映射到主机。程序默认在容器内监听 `:8080` 端口，可以通过 `D2H_LISTEN_ADDR` 环境变量修改。
*   **频道配置文件**:
    *   将您的频道配置文件（例如 `mytv.json`）挂载到容器中，例如挂载到 `/app/config/channels.json`。
    *   通过环境变量 `D2H_CHANNELS_JSON_PATH` 指定容器内频道配置文件的完整路径。
*   **监听地址 (可选)**: 通过环境变量 `D2H_LISTEN_ADDR` 可以更改容器内程序监听的地址和端口 (例如 `0.0.0.0:8888` 或 `:8888`)。

**示例:**

假设您的频道配置文件名为 `mytv.json`，位于当前主机的 `./sampledata` 目录下。您希望应用在容器的 `8888` 端口上监听，并将该端口映射到主机的 `8888` 端口。

```bash
docker run -d \
  -p 8888:8888 \
  -v $(pwd)/sampledata/mytv.json:/app/config/mytv.json \
  -e D2H_CHANNELS_JSON_PATH="/app/config/mytv.json" \
  -e D2H_LISTEN_ADDR=":8888" \
  --name my-dash2hlsproxy \
  dash2hlsproxy-app
```

### 环境变量

*   `CHANNELS_JSON`: **必需**。指定容器内频道配置文件的路径。
*   `LISTEN_ADDR`: 可选。指定程序在容器内监听的地址和端口。默认为 `:8080`。

## 本地开发和运行 (不使用 Docker)

### 依赖

*   Go (版本 1.20 或更高)

### 配置

程序通过一个 JSON 文件配置频道。默认情况下，它会查找名为 `channels.json` 的文件。您可以通过 `-config` 命令行参数指定不同的配置文件路径。

示例 `channels.json`:
```json
{
  "Name": "My Channel Lineup",
  "Id": "my-lineup-001",
  "Channels": [
    {
      "Name": "Channel 1",
      "Id": "ch1",
      "Manifest": "http://example.com/channel1/manifest.mpd",
      "Key": "aabbccddeeff00112233445566778899",
      "UserAgent": "MyCustomPlayer/1.0"
    }
    // ...更多频道
  ]
}
```

### 运行

```bash
go run main.go -config path/to/your/channels.json -listen :8080
```

或者，如果使用默认配置文件名 (`channels.json` 在项目根目录) 和默认监听端口 (`:8080`):
```bash
go run main.go
