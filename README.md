# model-gateway

多渠道 LLM 聚合网关：把多个 LLM API 渠道（官方/免费/中转）聚合成一个 OpenAI 兼容端点，自动择优、健康巡检、熔断回落。

## 特性

- **多渠道路由组**：按用途分组（如 coding / transcribe），`model` 填组名即自动择优；支持 `渠道/模型` 限定写法与 `前缀-*/模型` 池通配（多 key 池整体调度）
- **健康巡检 + 熔断**：5 分钟轮询探活，真实调用失败自动熔断换道；`no_probe` 渠道跳过探针、以实单判活
- **双协议**：`/v1/chat/completions`（OpenAI）与 `/v1/messages`（Anthropic）全渠道可用，自动互转
- **视觉/上下文路由**：带图请求自动跳过纯文本模型；上下文装不下的候选自动降级
- **Web 监控面板**（`:8000`）：渠道卡片、稳定性表、消耗统计、实时调度卡（置顶/跳过/排序即点即生效）
- **Qt 监控 GUI**：PySide6 暗色仪表盘（KPI 瓷砖 / 最近模型 / 调用日志 / 在途高亮 / 拖框复制）
- **控制台监控**：`monitor.py` 黑窗口版，与 GUI 同数据源
- **全配置热重载**：providers / routers / 模型规格改动即时生效，只有 app.py 变更才需重启

## 快速开始

```bash
pip install -r requirements.txt

# 1. 配置（复制 example 并填入你自己的渠道与密钥）
cp config.example.json config.json       # 本地鉴权 key
cp providers.example.json providers.json # 你的上游渠道

# 2. 启动（固定端口 8000）
python app.py

# 3. 调用（OpenAI 兼容）
curl http://127.0.0.1:8000/v1/chat/completions \
  -H "Authorization: Bearer <你的 local_api_key>" \
  -H "Content-Type: application/json" \
  -d '{"model": "coding", "messages": [{"role": "user", "content": "hi"}]}'
```

`model` 直接填路由组名（`routers.json` 里定义）即自动在组内择优回落；也可以填 `渠道/模型` 点名单调。

## 监控

- **Web 面板**：http://127.0.0.1:8000（Bearer 你的 local_api_key）
- **Qt GUI**：双击 `监控GUI.bat` 或 `python monitor_gui.py`（需 `pip install PySide6`）
- **控制台**：`python monitor.py [刷新秒数]`

## 配置说明

| 文件 | 作用 |
|---|---|
| `config.json` | `local_api_key`（客户端访问网关的鉴权）等 |
| `providers.json` | 上游渠道列表：name / base_url / api_key / models / billing / note / no_probe 等 |
| `routers.json` | 路由组定义：组名 → 候选列表（支持 `渠道/模型` 与 `前缀-*/模型` 池通配） |
| `models_meta.json` | 模型规格（总参/激活/上下文/视觉），面板展示用 |

上游不回 usage 时自动粗估记账；推理模型（reasoning_content 流式）也已纳入统计。

## 说明

- 本仓库不含任何真实 API 密钥；`providers.json` / `config.json` 请自行配置且勿提交
- 端口固定 8000，客户端都指向这里

## License

见 [LICENSE](LICENSE)
