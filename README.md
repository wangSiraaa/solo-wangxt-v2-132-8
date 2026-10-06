# range-cache-proxy

内网下载测试用的、正确处理 Range 的缓存代理。服务端用 Rust + Axum + reqwest
实现，元数据存 SQLite，字节落本地文件。无任何页面/UI。

## 安全边界

- 上游**只能**是启动时配置的一个 `UPSTREAM_BASE`（默认强制 loopback）。
  目标 URL 由请求路径与该 base 重新拼接，并再次核对 scheme/host/port；
  reqwest 的重定向被完全禁用，因此 3xx 无法把代理带到外部主机。
- 路径穿越（`..`、`.`、`%2e%2e`、反斜杠、NUL）在请求进入时以 403 拒绝。
- 只接受 GET。

## 正确性要点

- **版本身份只认强验证器**：只有不带 `W/` 的 ETag 才能建立/复用缓存版本。
  弱 ETag 的 200/206 一律透传，绝不落盘、绝不参与区段合并。
- **区段合并必须来自同一对象版本**：回填缺口的每个请求都带
  `If-Range: "<strong etag>"`；上游回 200 说明对象已变，立即切换到新版本，
  绝不会把新字节拼进旧区段。
- **文件长度相同不等于内容相同**：即便请求区间已完全在磁盘上，也会先用
  `If-None-Match` 做一次条件请求——上游 304 才返回缓存字节；200（同长度、
  新 ETag、新内容）则提交新版本后再回答。
- **完整上游响应先落临时 spool 文件**，严格核对 Content-Length 后才提交到
  blob 并写 SQLite 区段。长度不符/连接提前结束一律 502，绝不返回“看起来
  成功”的截断响应。
- **客户端断开即取消上游下载**：spool 存活在请求 future 内，没有后台续传
  任务；future 被取消时临时文件删除，不提交任何区段。
- **缓存文件损坏不能伪装成成功**：读取时校验 blob 长度，短于元数据则返回
  502 路径（内部会重置区段并重取，绝不返回短一截的 206）。
- 完整实现 `Range`（闭区间、开区间、尾部 suffix、clamp）、`If-Range`
  （强 ETag / 弱 ETag 拒绝 / HTTP-date）、206 `Content-Range`、416
  `bytes */<length>`。多区间请求透传，不生成 multipart/byteranges。

## 存储布局

```
<CACHE_DIR>/
  range-cache.sqlite3      # objects / versions / segments
  blobs/v<version_id>.bin  # 每个版本独立 blob；版本间绝不共享
  tmp/spool-*.tmp          # 回源临时文件，成功后 rename/拷入 blob
```

`segments(start,end)` 为半开区间，按版本隔离并在写入时合并。

## 运行

```bash
# 1) 本地测试上游（仅监听 loopback）
cargo run --release --features test-support --bin test_support -- 127.0.0.1:9000

# 2) 代理
UPSTREAM_BASE=http://127.0.0.1:9000/ \
LISTEN_ADDR=127.0.0.1:8000 \
CACHE_DIR=./cache-data \
cargo run --release --bin proxy
```

测试上游提供：`/obj/alpha` `/obj/tiny` `/obj/weak` `/obj/noetag`
`/obj/slow?ms=&len=` `/obj/truncated` `/obj/ignores-range` `/obj/mutable`
（POST 切换同长度新版本）`/redir`（跳外部）`/stats`。

## 测试

```bash
cargo test --features test-support
```

- 9 个库单元测试：Range 解析/边界、缺口/合并、ETag 强弱、三种 HTTP-date、
  If-Range 规则。
- 12 个端到端集成测试：冷启动全量、重叠区段合并、suffix/开区间尾部、
  bytes=-0 与越界 416、上游忽略 Range（200 提交）、If-Range 强/弱/陈旧、
  同长度对象换版、缓存命中 304 强校验零额外字节、上游 Content-Length 说谎、
  客户端断开取消回源且不落缓存、blob 被截断后不返回短成功响应、
  allow-list（原始 TCP 报文测穿越/绝对形式 URL/外部重定向）。

所有涉及字节的断言都比对实际响应体的 SHA-256 与期望切片摘要，而非仅看
状态码或响应头。
