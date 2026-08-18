# FMP WeKnora Bridge

受控的 Financial Modeling Prep (FMP) MCP 服务和 WeKnora 研究知识库同步器。

## 能力

- Streamable HTTP MCP：`/mcp`，通过 Bearer Token 保护。
- 受限工具：标的搜索、行情、价格历史、公司研究、年度财报、新闻、经济日历。
- 美股/ETF、加密货币和外汇；所有响应附带 FMP 来源端点和 UTC 获取时间。
- PostgreSQL 保存标的目录、同步任务及 WeKnora 文档版本；Redis 提供缓存和限流。
- 每日刷新目录、按小时刷新行情；只有研究内容发生变化时才更新 WeKnora 手工知识文档。

当前提供的 Starter FMP 权限应保持 `annual` 基本面模式。全市场同步会先估算请求数；当估算超过 `FMP_DAILY_REQUEST_BUDGET` 时，预检会拒绝启动，避免意外超额。

## 部署

1. 创建 WeKnora 内的专用“FMP 研究”知识库，并生成具有该知识库写权限的 API Key。
2. 复制 `.env.example` 为 `.env`，填入密钥和知识库 ID。不要把 `.env` 提交到 Git。
3. 找到 WeKnora compose 网络名，设置 `WEKNORA_DOCKER_NETWORK`；例如 `weknora_default`。
4. 启动：`docker compose up -d --build`。
5. 默认仅提供实时 MCP 查询，`SYNC_ENABLED=false` 不会启动定时同步，也会拒绝手工触发
   `/admin/sync/catalog` 和 `/admin/sync/hourly`，以避免将高频市场数据写入知识库并产生 embedding 消耗。
6. 预检：

   ```powershell
   $headers = @{ Authorization = "Bearer $env:MCP_BEARER_TOKEN" }
   Invoke-RestMethod http://localhost:8000/admin/sync/preflight -Method Post -Headers $headers
   ```

需要明确将 FMP 数据索引至 WeKnora 时，先将 `SYNC_ENABLED=true`，再执行目录同步，并用较小的
`SYNC_BOOTSTRAP_LIMIT` 试运行小时同步。生产全市场轮换可设置
   `SYNC_UNIVERSES=crypto,nasdaq,forex_g10`、`SYNC_ROTATION_BATCH_SIZE=1000`；服务每天从 FMP 刷新目录，
   并以稳定的跨资产轮换顺序每小时处理下一批。NASDAQ 上市股票来自 FMP 的
   `company-screener?exchange=NASDAQ` 分页目录，G10 外汇只保留 USD、EUR、JPY、GBP、CHF、CAD、AUD、NZD、SEK、NOK 的交叉对。
   `SYNC_SYMBOLS=AAPL,BTCUSD,EURUSD` 是手工补充标的，会与市场范围合并；不配置市场范围时，手工代码
   仍会覆盖 `SYNC_BOOTSTRAP_LIMIT`。预检会拒绝目录中不存在的手工代码，并基于每小时轮换批次估算日调用量：

   `nasdaq` 范围需要当前 FMP Key 有 `company-screener` 端点权限；若目录中没有可用 NASDAQ
   成分，预检会拒绝启动，避免服务无提示地降级为只同步其他市场。

   ```powershell
   Invoke-RestMethod http://localhost:8000/admin/sync/catalog -Method Post -Headers $headers
   Invoke-RestMethod http://localhost:8000/admin/sync/hourly -Method Post -Headers $headers
   ```

在 WeKnora 的“设置 → MCP 服务”中新建 HTTP Streamable 服务：

- URL（同一 Docker 网络）：`http://fmp-weknora-bridge:8000/mcp`
- Authorization：`Bearer <MCP_BEARER_TOKEN>`

主机上检查 `/health` 和 `/ready`；任务记录使用受保护的 `/admin/runs`，Prometheus 指标使用受保护的 `/metrics`。

## 开发与验证

```powershell
uv sync --all-groups
uv run pytest
uv run ruff check .
```

本项目不记录或打印 FMP、MCP、WeKnora 的密钥。生产环境应通过 Docker secrets 或外部密钥管理器注入这些环境变量。
