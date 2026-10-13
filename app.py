import json
import asyncio
import contextvars
import time
import logging
import re
import base64
import hashlib
import hmac
from email.utils import formatdate
from pathlib import Path
from contextlib import asynccontextmanager
from collections import deque

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import httpx
from tool_shim import inject_tools, extract_tool_calls as _ts_extract
import secrets
import uuid

# ============================================================
# 日志
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("gateway")

# ============================================================
# 路径与常量
# ============================================================
import sys

if getattr(sys, 'frozen', False):
    APP_DIR = Path(sys._MEIPASS)
    DATA_DIR = Path(sys.executable).parent
else:
    APP_DIR = Path(__file__).parent
    DATA_DIR = Path(__file__).parent

DATA_FILE = DATA_DIR / "providers.json"
CONFIG_FILE = DATA_DIR / "config.json"
HISTORY_FILE = DATA_DIR / "history.jsonl"
USAGE_FILE = DATA_DIR / "usage.jsonl"
META_FILE = DATA_DIR / "models_meta.json"
ROUTERS_FILE = DATA_DIR / "routers.json"
ANNOUNCEMENT_FILE = DATA_DIR / "announcement.json"

APP_VERSION = "1.5.2"

MAX_HISTORY_DAYS = 30
MAX_USAGE_DAYS = 30
HISTORY_CLEANUP_INTERVAL = 6 * 3600
ONE_MILLION = 1048576
POLL_INTERVAL = 300
CIRCUIT_FAIL_THRESHOLD = 3
CIRCUIT_RECOVERY_SECONDS = 60
STREAM_IDLE_TIMEOUT = 45.0  # 流空闲看门狗: 上游攥着连接不吐字超过此秒数立即掐断熔断换人
QUALITY_WINDOW = 20


# ============================================================
# 原子写入
# ============================================================
def atomic_write(path: Path, content: str):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


# ============================================================
# 配置加载
# ============================================================
def load_config():
    if CONFIG_FILE.exists():
        return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    data = {
        "local_api_key": "sk-local-" + secrets.token_hex(16),
    }
    atomic_write(CONFIG_FILE, json.dumps(data, indent=2))
    return data


def load_providers():
    if DATA_FILE.exists():
        return json.loads(DATA_FILE.read_text(encoding="utf-8"))
    return []


def save_providers(data):
    atomic_write(DATA_FILE, json.dumps(data, ensure_ascii=False, indent=2))
    global _providers_mtime
    try:
        _providers_mtime = DATA_FILE.stat().st_mtime
    except OSError:
        pass


_providers_mtime = 0.0


def maybe_reload_providers():
    """providers.json 真热重载: mtime 变了就地重载(手工编辑文件立即生效, 无需重启)。
    API 改动走 save_providers 会同步 mtime, 不会误触发。"""
    global providers, _providers_mtime
    try:
        m = DATA_FILE.stat().st_mtime
    except OSError:
        return
    if m == _providers_mtime:
        return
    _providers_mtime = m
    try:
        fresh = json.loads(DATA_FILE.read_text(encoding="utf-8"))
        if isinstance(fresh, list) and fresh:
            providers = fresh
            logger.info("providers.json hot-reloaded (%d channels)", len(fresh))
    except Exception:
        logger.exception("providers.json hot-reload failed; keep old")


def load_meta():
    default = {
        "aliases": {},
        "context_limits": {},
        "non_chat_keywords": [],
        "model_descriptions": {},
    }
    if META_FILE.exists():
        default.update(json.loads(META_FILE.read_text(encoding="utf-8")))
    return default


def load_routers():
    if ROUTERS_FILE.exists():
        try:
            return json.loads(ROUTERS_FILE.read_text(encoding="utf-8"))
        except:
            pass
    return {}

def save_routers():
    global _routers_mtime
    ROUTERS_FILE.write_text(json.dumps(ROUTERS, indent=2, ensure_ascii=False), encoding="utf-8")
    try:
        _routers_mtime = ROUTERS_FILE.stat().st_mtime
    except OSError:
        pass


_routers_mtime = 0.0
_meta_mtime = 0.0


def maybe_reload_routers():
    """routers.json 热重载: 手工改文件立即生效"""
    global ROUTERS, _routers_mtime
    try:
        m = ROUTERS_FILE.stat().st_mtime
    except OSError:
        return
    if m == _routers_mtime:
        return
    _routers_mtime = m
    try:
        fresh = load_routers()
        if isinstance(fresh, dict) and fresh:
            ROUTERS = fresh
            logger.info("routers.json hot-reloaded (%d groups)", len(fresh))
    except Exception:
        logger.exception("routers.json hot-reload failed")


def maybe_reload_meta():
    """models_meta.json 热重载: 别名/上下文/规格/视觉标记改文件立即生效"""
    global MODEL_ALIASES, CONTEXT_LIMITS, MODEL_DESCRIPTIONS, MODEL_SPECS, NON_CHAT_KEYWORDS, _meta_mtime
    try:
        m = META_FILE.stat().st_mtime
    except OSError:
        return
    if m == _meta_mtime:
        return
    _meta_mtime = m
    try:
        meta = load_meta()
        MODEL_ALIASES = meta.get("aliases", {})
        CONTEXT_LIMITS = meta.get("context_limits", {})
        NON_CHAT_KEYWORDS = meta.get("non_chat_keywords", [])
        MODEL_DESCRIPTIONS = meta.get("model_descriptions", {})
        MODEL_SPECS = meta.get("model_specs", {})
        _META_LC_CACHE.clear()
        logger.info("models_meta.json hot-reloaded")
    except Exception:
        logger.exception("models_meta.json hot-reload failed")


app_config = load_config()
LOCAL_API_KEY = app_config.get("local_api_key")
ROUTERS = load_routers()
for _f, _attr in ((ROUTERS_FILE, '_routers_mtime'), (META_FILE, '_meta_mtime'), (DATA_FILE, '_providers_mtime')):
    try:
        globals()[_attr] = _f.stat().st_mtime
    except OSError:
        pass

meta = load_meta()
MODEL_ALIASES = meta.get("aliases", {})
CONTEXT_LIMITS = meta.get("context_limits", {})
NON_CHAT_KEYWORDS = meta.get("non_chat_keywords", [])
MODEL_DESCRIPTIONS = meta.get("model_descriptions", {})
MODEL_SPECS = meta.get("model_specs", {})


def _next_anchor_in(hours):
    """上游官方固定锚点重置(如 MiniMax 5h 窗口 0/5/10/15/20 点), 返回距下一次的秒数; 无配置返回 -1"""
    if not hours:
        return -1
    lt = time.localtime()
    now_s = lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec
    best = None
    for h in hours:
        for add in (0, 86400):
            t = h * 3600 - now_s + add
            if t > 60 and (best is None or t < best):
                best = t
    return int(best) if best is not None else -1


def _meta_lookup(table, *keys):
    """元数据零留白查找：精确 → 基名(去斜杠前缀) → 大小写不敏感（表侧建小写索引）"""
    lc = _META_LC_CACHE.setdefault(id(table), {str(k).lower(): v for k, v in table.items()})
    cands = []
    for k in keys:
        if not k:
            continue
        cands.append(k)
        if "/" in k:
            cands.append(k.split("/")[-1])
    for k in cands:
        v = table.get(k) or lc.get(k.lower())
        if v:
            return v
    return None

_META_LC_CACHE = {}

# 窗口制渠道(拼车等)的额度查询缓存: name -> (ts, info)
_QUOTA_CACHE = {}


# ============================================================
# 鉴权
# ============================================================
security = HTTPBearer(auto_error=False)


def verify_client(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """客户端调用 /v1/* 的鉴权"""
    if not credentials or credentials.credentials != LOCAL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API Key")
    return credentials


def verify_admin(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """管理面板调用 /api/* 的鉴权，直接使用 local_api_key"""
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing credentials")
    if credentials.credentials != LOCAL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return credentials


# ============================================================
# 全局状态
# ============================================================
providers = load_providers()
try:
    _providers_mtime = DATA_FILE.stat().st_mtime
except OSError:
    pass
health_status: dict = {}
model_details: dict = {}
model_quality: dict = {}          # key -> {ok, fail, error, latencies: deque}
circuit_breaker: dict = {}        # key -> {fails, open_until}
providers_lock = asyncio.Lock()
history_lock = asyncio.Lock()
usage_lock = asyncio.Lock()
http_client: httpx.AsyncClient | None = None
poll_task = None
last_poll_time: float = 0
last_check_time: float = time.time()
last_history_cleanup: float = 0


def mark_full_check():
    """记录一次完整检测的时间，用于重置自动轮询计时"""
    global last_check_time
    last_check_time = time.time()


# ============================================================
# 历史记录（异步文件 IO）
# ============================================================
def _append_history_sync(snapshot: dict):
    line = json.dumps({"time": time.time(), "data": snapshot}, ensure_ascii=False) + "\n"
    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(line)


async def append_history(snapshot: dict):
    await asyncio.to_thread(_append_history_sync, snapshot)


def _read_history_sync(hours: int):
    if not HISTORY_FILE.exists():
        return []
    cutoff = time.time() - hours * 3600
    records = []
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                if rec["time"] >= cutoff:
                    records.append(rec)
            except Exception:
                pass
    return records


async def read_history(hours: int = 24):
    async with history_lock:
        return await asyncio.to_thread(_read_history_sync, hours)


def _cleanup_history_sync():
    if not HISTORY_FILE.exists():
        return 0
    cutoff = time.time() - MAX_HISTORY_DAYS * 86400
    kept = []
    removed = 0
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                if rec["time"] >= cutoff:
                    kept.append(line if line.endswith("\n") else line + "\n")
                else:
                    removed += 1
            except Exception:
                pass
    if removed > 0:
        atomic_write(HISTORY_FILE, "".join(kept))
    return removed


async def maybe_cleanup_history():
    global last_history_cleanup
    now = time.time()
    if now - last_history_cleanup < HISTORY_CLEANUP_INTERVAL:
        return
    last_history_cleanup = now
    n = await asyncio.to_thread(_cleanup_history_sync)
    if n:
        logger.info("history cleanup: removed %d expired records", n)
    un = await asyncio.to_thread(_cleanup_usage_sync)
    if un:
        logger.info("usage cleanup: removed %d expired records", un)


# ============================================================
# 消耗统计（异步文件 IO）
# ============================================================
def _append_usage_sync(record: dict):
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with open(USAGE_FILE, "a", encoding="utf-8") as f:
        f.write(line)


_req_t0 = contextvars.ContextVar("req_t0", default=None)
_inflight_prev = contextvars.ContextVar("inflight_prev", default=None)

# 进行中请求登记：GUI"正在调用"高亮以此为准（usage 落盘在请求结束, 看不到在途请求）。
# 结算时在 append_usage 清除; 兜底 TTL 900s 防崩溃泄漏。
INFLIGHT: dict = {}


def _inflight_mark(provider: str, model: str):
    """标记当前候选为进行中, 并摘掉本请求上一个候选的标记（回落时不留残影）"""
    try:
        prev = _inflight_prev.get()
        if prev:
            INFLIGHT.pop(prev, None)
        key = f"{provider}||{model}"
        INFLIGHT[key] = {"provider": provider, "model": model, "ts": time.time()}
        _inflight_prev.set(key)
    except Exception:
        pass


async def append_usage(record: dict):
    # 请求结束, 摘掉进行中标记
    try:
        INFLIGHT.pop(f"{record.get('provider')}||{record.get('model')}", None)
    except Exception:
        pass
    # 自动补调用延时: 请求入口记t0, 记账时算全程耗时(流式=到流结束)
    t0 = _req_t0.get()
    if t0 and "dur" not in record:
        try:
            record["dur"] = round(time.time() - t0, 1)
        except Exception:
            pass
    await asyncio.to_thread(_append_usage_sync, record)


def _read_usage_sync(days: int):
    if not USAGE_FILE.exists():
        return []
    cutoff = time.time() - days * 86400
    records = []
    with open(USAGE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                if rec.get("ts", 0) >= cutoff:
                    records.append(rec)
            except Exception:
                pass
    return records


async def read_usage(days: int = 1):
    async with usage_lock:
        return await asyncio.to_thread(_read_usage_sync, days)


def _cleanup_usage_sync():
    if not USAGE_FILE.exists():
        return 0
    cutoff = time.time() - MAX_USAGE_DAYS * 86400
    kept = []
    removed = 0
    with open(USAGE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                if rec.get("ts", 0) >= cutoff:
                    kept.append(line if line.endswith("\n") else line + "\n")
                else:
                    removed += 1
            except Exception:
                pass
    if removed > 0:
        atomic_write(USAGE_FILE, "".join(kept))
    return removed


# ============================================================
# 模型工具函数
# ============================================================
def is_chat_model(model_id: str) -> bool:
    lower = model_id.lower()
    return not any(kw in lower for kw in NON_CHAT_KEYWORDS)


def is_free_model(model_info: dict) -> bool:
    pricing = model_info.get("pricing", {})
    prompt_price = pricing.get("prompt", "")
    completion_price = pricing.get("completion", "")
    try:
        if float(prompt_price) == 0 and float(completion_price) == 0:
            return True
    except (ValueError, TypeError):
        pass
    return False


def is_free_by_name(model_id: str) -> bool:
    lower = model_id.lower()
    return ":free" in lower or "-free" in lower


def get_enabled_models(provider: dict) -> list[str]:
    """返回该 provider 未被禁用的模型列表"""
    disabled = set(provider.get("disabled_models", []))
    return [m for m in provider.get("models", []) if m not in disabled]


def get_context_length(model: str) -> int:
    actual = MODEL_ALIASES.get(model, model)
    ctx = _meta_lookup(CONTEXT_LIMITS, model, actual)
    if ctx:
        return ctx
    return model_details.get(actual, {}).get("context_length") or 32768


def is_1m_model(model: str) -> bool:
    ctx = get_context_length(model)
    return bool(ctx) and ctx >= ONE_MILLION


def _body_has_image(body: dict) -> bool:
    """检测请求是否带图: OpenAI 格式 image_url 数组content 或 Anthropic 格式 image block"""
    try:
        for msg in body.get("messages") or []:
            c = msg.get("content") if isinstance(msg, dict) else None
            if isinstance(c, list):
                for part in c:
                    if not isinstance(part, dict):
                        continue
                    if part.get("type") == "image_url" or part.get("type") == "image":
                        return True
                    src = part.get("source") or {}
                    if isinstance(src, dict) and src.get("type") in ("base64", "url"):
                        return True
    except Exception:
        pass
    return False


def _is_vision_model(model: str) -> bool:
    actual = MODEL_ALIASES.get(model, model)
    spec = _meta_lookup(MODEL_SPECS, model, actual)
    return bool(isinstance(spec, dict) and spec.get("vision"))


def filter_by_vision(candidates: list, body: dict) -> list:
    """带图请求只走视觉模型: 纯文本模型读图会被上游掐线/403, 卡死整个会话。
    全部候选都不支持视觉时保留原列表(让上游自己报错, 不无脑拒绝服务)。"""
    if not _body_has_image(body):
        return candidates
    keep = [(p, m) for p, m in candidates if _is_vision_model(m)]
    if len(keep) < len(candidates):
        dropped = [f"{p['name']}/{m}" for p, m in candidates if (p, m) not in keep]
        logger.info("vision-filter: 请求带图, 跳过纯文本候选: %s", ", ".join(dropped) or "-")
    return keep or candidates


def filter_by_context(candidates: list, body: dict, reserve: int = 4096) -> list:
    """失败回落守则：上下文装不下本次请求的候选直接跳过，避免长会话顺位落到小窗口垃圾模型。
    全部装不下时保留原列表（让上游自己报错，别无脑拒绝服务）。"""
    try:
        s = json.dumps(body, ensure_ascii=False)
        # 图片base64不算文本token(否则多图请求被高估8倍, ctx-filter误杀大窗口候选)
        s = re.sub(r'"data:image/[^;]+;base64,[A-Za-z0-9+/=]+"', '"img"', s)
        est = len(s) // 3 + reserve  # 粗估: ~3字符/token
    except Exception:
        return candidates
    if est <= 8192:
        return candidates
    keep = [(p, m) for p, m in candidates if get_context_length(m) >= est]
    if len(keep) < len(candidates):
        dropped = [f"{p['name']}/{m}({get_context_length(m)//1024}K)" for p, m in candidates if (p, m) not in keep]
        logger.info("ctx-filter: 请求约 %d tok, 跳过装不下的候选: %s", est, ", ".join(dropped) or "-")
    return keep or candidates


def mask_key(key: str) -> str:
    if not key:
        return ""
    if len(key) <= 12:
        return "****"
    return key[:6] + "****" + key[-4:]


# ============================================================
# 质量分（内存滑动窗口）
# ============================================================
def update_model_quality(key: str, info: dict):
    q = model_quality.get(key)
    if q is None:
        q = {"ok": 0, "fail": 0, "error": 0, "latencies": deque(maxlen=QUALITY_WINDOW)}
        model_quality[key] = q
    st = info.get("status", "unknown")
    if st == "ok":
        q["ok"] += 1
        lat = info.get("latency_ms")
        if lat:
            q["latencies"].append(lat)
    elif st == "fail":
        q["fail"] += 1
    elif st == "error":
        q["error"] += 1


def get_quality_score(key: str) -> float:
    """0~1 可用率，无数据返回 1.0（乐观）"""
    q = model_quality.get(key)
    if not q:
        return 1.0
    total = q["ok"] + q["fail"] + q["error"]
    if total == 0:
        return 1.0
    return q["ok"] / total


def get_avg_latency(key: str):
    q = model_quality.get(key)
    if not q or not q["latencies"]:
        return None
    return sum(q["latencies"]) / len(q["latencies"])


# ============================================================
# 熔断
# ============================================================

def _estimate_usage(body: dict, content_chars: int) -> tuple[int, int]:
    """上游不回usage时的粗估: pt按消息JSON字符/3, ct按正文/2(中文~1.5字/tok取保守)。
    content_chars 已含思维链字符(推理模型正文少思维多), 统一按2折算够用。
    仅用于记账展示, 宁可略偏差也不能记0(零蛋会误导调度面板)。"""
    try:
        pt = len(json.dumps(body.get("messages") or "", ensure_ascii=False)) // 3
    except Exception:
        pt = 0
    return pt, content_chars // 2

def is_circuit_open(key: str) -> bool:
    cb = circuit_breaker.get(key)
    if not cb:
        return False
    return bool(cb.get("open_until")) and time.time() < cb["open_until"]


FBLOG_FILE = DATA_DIR / "fallbacks.jsonl"


def log_fallback(key: str, err: str | None):
    """回落原因落盘：每次真实调用失败都记一条，面板据此显示回落原因（供其他面板参考）"""
    try:
        rec = {"ts": time.time(), "key": key, "err": (err or "")[:200]}
        with open(FBLOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass


def record_fail(key: str, err: str | None = None):
    if err:
        log_fallback(key, err)
    cb = circuit_breaker.setdefault(key, {"fails": 0, "open_until": 0})
    cb["fails"] += 1
    if cb["fails"] >= CIRCUIT_FAIL_THRESHOLD:
        cb["open_until"] = time.time() + CIRCUIT_RECOVERY_SECONDS
        logger.warning("circuit opened: %s", key)


def open_circuit_now(key: str, err: str):
    """立即熔断(不等阈值): 用于空流/静默拒绝等确定性失败, 下一请求立刻换人"""
    log_fallback(key, err)
    cb = circuit_breaker.setdefault(key, {"fails": 0, "open_until": 0})
    cb["fails"] = CIRCUIT_FAIL_THRESHOLD
    cb["open_until"] = time.time() + CIRCUIT_RECOVERY_SECONDS
    logger.warning("circuit force-opened: %s (%s)", key, err[:80])


def note_real_success(provider: str, model: str, latency_ms: int | None = None):
    """真实调用成功就地刷新健康: 探针可能被大单排队假红, 面板健康以实账为准"""
    k = f"{provider}||{model}"
    h = health_status.get(k) or {}
    h.update({"status": "ok", "code": 200, "last_status": "ok", "checked_at": time.time()})
    if latency_ms is not None:
        h["latency_ms"] = latency_ms
    health_status[k] = h
    cb = circuit_breaker.get(k)
    if cb:
        cb["fails"] = 0
        cb["open_until"] = 0


def record_success(key: str):
    cb = circuit_breaker.get(key)
    if cb:
        cb["fails"] = 0
        cb["open_until"] = 0


# ============================================================
# 探测
# ============================================================
async def check_model(base_url: str, api_key: str, model: str, protocol: str = "openai") -> dict:
    actual_model = MODEL_ALIASES.get(model, model)
    start = time.time()
    if protocol == "wenxiaobai":
        # 问小白：最小签名调用探测
        provider = {"api_key": api_key, "device_id": "", "user_id": 0}
        body_str = json.dumps({"query": "hi", "modelId": MODEL_ALIASES.get(model, model),
                               "isNewConversation": True, "conversationId": "", "agentId": "200006"},
                              ensure_ascii=False, separators=(",", ":"))
        try:
            resp = await http_client.post(
                base_url.rstrip("/") + "/api/v1.0/core/conversation/chat/v3",
                content=body_str.encode("utf-8"),
                headers=_wxb_sign_and_headers(provider, body_str), timeout=30)
            latency = round((time.time() - start) * 1000)
            if resp.status_code == 200:
                return {"status": "ok", "code": 200, "latency_ms": latency,
                        "prompt_tokens": 0, "completion_tokens": 0}
            return {"status": "fail", "code": resp.status_code, "latency_ms": latency,
                    "detail": resp.text[:200]}
        except Exception as e:
            return {"status": "error", "latency_ms": round((time.time() - start) * 1000),
                    "detail": str(e)[:200]}
    if protocol == "minimax-direct":
        try:
            p = {"api_key": api_key, "base_url": base_url, "user_id": ""}
            await _mm_agent_id(p)
            latency = round((time.time() - start) * 1000)
            return {"status": "ok", "code": 200, "latency_ms": latency,
                    "prompt_tokens": 0, "completion_tokens": 0}
        except Exception as e:
            return {"status": "fail", "code": 0, "error": str(e)[:100],
                    "latency_ms": round((time.time() - start) * 1000)}
    if protocol == "52who":
        try:
            import secrets as _sec
            r = await http_client.post(base_url.rstrip("/") + "/api/chatgpt/chat-process",
                json={"model": "gpt-5.5", "modelName": "GPT5.6（1普分）", "modelType": 1,
                      "prompt": "hi", "imageUrl": "", "videoUrl": "", "fileUrl": "",
                      "extraParam": {"size": "auto", "style": ""}, "appId": 0,
                      "options": {"groupId": 0, "fileParsing": "", "usingDeepThinking": False,
                                  "usingTool": False, "useKnowledgeBase": False},
                      "usingPluginId": 0, "drawId": ""},
                headers={"content-type": "application/json",
                         "fingerprint": str(_sec.randbelow(900000000) + 100000000),
                         "x-website-domain": base_url.rstrip("/")}, timeout=60)
            latency = round((time.time() - start) * 1000)
            if r.status_code == 200:
                return {"status": "ok", "code": 200, "latency_ms": latency,
                        "prompt_tokens": 0, "completion_tokens": 0}
            return {"status": "fail", "code": r.status_code, "latency_ms": latency}
        except Exception as e:
            return {"status": "fail", "code": 0, "error": str(e)[:100],
                    "latency_ms": round((time.time() - start) * 1000)}
    if protocol == "baidu":
        try:
            client = _baidu_client({"api_key": api_key} if api_key else {"api_key": ""})
            loop = asyncio.get_running_loop()

            def probe():
                client._ensure_cookies()
                client._get_base_data()
                return True
            await loop.run_in_executor(None, probe)
            latency = round((time.time() - start) * 1000)
            return {"status": "ok", "code": 200, "latency_ms": latency,
                    "prompt_tokens": 0, "completion_tokens": 0}
        except Exception as e:
            return {"status": "fail", "code": 0, "error": str(e)[:100],
                    "latency_ms": round((time.time() - start) * 1000)}
    if protocol == "allai":
        try:
            r = await http_client.post(base_url.rstrip("/") + "/api/chat",
                                       json={"tool": "problem_solver", "message": "hi", "history": []},
                                       headers={"Content-Type": "application/json"}, timeout=30)
            latency = round((time.time() - start) * 1000)
            if r.status_code == 200 and r.json().get("reply"):
                return {"status": "ok", "code": 200, "latency_ms": latency,
                        "prompt_tokens": 0, "completion_tokens": 0}
            return {"status": "fail", "code": r.status_code, "latency_ms": latency}
        except Exception as e:
            return {"status": "fail", "code": 0, "error": str(e)[:100],
                    "latency_ms": round((time.time() - start) * 1000)}
    if protocol == "metaso":
        try:
            ph = {"metaso-pc": "pc", "token": api_key, "Accept": "application/json"}
            r = await http_client.get(base_url.rstrip("/") + "/api/search-template",
                                      headers=ph, timeout=30)
            latency = round((time.time() - start) * 1000)
            if r.status_code == 200:
                return {"status": "ok", "code": 200, "latency_ms": latency,
                        "prompt_tokens": 0, "completion_tokens": 0}
            return {"status": "fail", "code": r.status_code, "latency_ms": latency}
        except Exception as e:
            return {"status": "fail", "code": 0, "error": str(e)[:100],
                    "latency_ms": round((time.time() - start) * 1000)}
    if protocol == "dangbei":
        provider = {"api_key": api_key, "base_url": base_url}
        p_create = {"botCode": "AI_SEARCH"}
        try:
            r = await http_client.post(base_url.rstrip("/") + "/ai-search/conversationApi/v1/create",
                                       json=p_create, headers=_db_headers(provider, p_create, uuid.uuid4().hex[:32]),
                                       timeout=30)
            latency = round((time.time() - start) * 1000)
            if r.status_code == 200 and (r.json().get("data") or {}).get("conversationId"):
                return {"status": "ok", "code": 200, "latency_ms": latency,
                        "prompt_tokens": 0, "completion_tokens": 0}
            return {"status": "fail", "code": r.status_code, "latency_ms": latency}
        except Exception as e:
            return {"status": "fail", "code": 0, "error": str(e)[:100],
                    "latency_ms": round((time.time() - start) * 1000)}
    if protocol == "anthropic":
        url = base_url.rstrip("/") + "/messages"
        payload = {"model": actual_model, "max_tokens": 8,
                   "messages": [{"role": "user", "content": "hi"}]}
        headers = {"x-api-key": api_key, "anthropic-version": "2023-06-01",
                   "Content-Type": "application/json"}
        try:
            resp = await http_client.post(url, json=payload, headers=headers, timeout=30)
            latency = round((time.time() - start) * 1000)
            if resp.status_code == 200 and isinstance(resp.json().get("content"), list):
                u = resp.json().get("usage", {})
                return {"status": "ok", "code": 200, "latency_ms": latency,
                        "prompt_tokens": u.get("input_tokens", 0),
                        "completion_tokens": u.get("output_tokens", 0)}
            return {"status": "fail", "code": resp.status_code, "latency_ms": latency,
                    "detail": resp.text[:200]}
        except Exception as e:
            return {"status": "error", "latency_ms": round((time.time() - start) * 1000),
                    "detail": str(e)[:200]}
    url = base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": actual_model,
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 5,
        "stream": False,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    start = time.time()
    try:
        resp = await http_client.post(url, json=payload, headers=headers, timeout=30)
        latency = round((time.time() - start) * 1000)
        if resp.status_code == 200:
            body = resp.json()
            # 静默拒绝(200但choices空/base_resp错误码, 如拼车池额度打穿)必须判败, 防巡检假绿
            _br = body.get("base_resp") or {}
            if _br.get("status_code") or not body.get("choices"):
                return {"status": "fail", "code": _br.get("status_code") or 200,
                        "latency_ms": latency, "detail": str(_br.get("status_msg") or "空choices")[:200]}
            usage = body.get("usage", {})
            return {
                "status": "ok",
                "code": resp.status_code,
                "latency_ms": latency,
                "prompt_tokens": usage.get("prompt_tokens", 0),
                "completion_tokens": usage.get("completion_tokens", 0),
            }
        return {
            "status": "fail",
            "code": resp.status_code,
            "latency_ms": latency,
            "detail": resp.text[:200],
        }
    except Exception as e:
        latency = round((time.time() - start) * 1000)
        return {"status": "error", "latency_ms": latency, "detail": str(e)[:200]}


async def fetch_model_details(base_url: str, api_key: str) -> dict:
    if not api_key:
        return {}
    url = base_url.rstrip("/") + "/models"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = await http_client.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            model_list = data.get("data", data) if isinstance(data, dict) else data
            details = {}
            for m in model_list:
                if isinstance(m, dict) and "id" in m:
                    pricing = m.get("pricing", {})
                    details[m["id"]] = {
                        "context_length": m.get("context_length"),
                        "prompt_price": pricing.get("prompt", ""),
                        "completion_price": pricing.get("completion", ""),
                    }
            return details
    except Exception:
        logger.exception("fetch_model_details failed for %s", base_url)
    return {}


async def fetch_models(base_url: str, api_key: str, free_only: bool = True) -> list[str]:
    url = base_url.rstrip("/") + "/models"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = await http_client.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            model_list = data.get("data", data) if isinstance(data, dict) else data
            if not isinstance(model_list, list):
                return []
            if free_only:
                has_pricing = any(isinstance(m, dict) and m.get("pricing") for m in model_list)
                if has_pricing:
                    free_by_api = [
                        m for m in model_list
                        if isinstance(m, dict) and "id" in m and is_free_model(m)
                    ]
                    if free_by_api:
                        return [m["id"] for m in free_by_api if is_chat_model(m["id"])]
                free_by_name = [
                    m["id"] for m in model_list
                    if isinstance(m, dict) and "id" in m
                    and is_free_by_name(m["id"]) and is_chat_model(m["id"])
                ]
                if free_by_name:
                    return free_by_name
            return [
                m["id"] for m in model_list
                if "id" in m and isinstance(m, dict) and is_chat_model(m["id"])
            ]
    except Exception:
        logger.exception("fetch_models failed for %s", base_url)
    return []


# ============================================================
# 轮询
# ============================================================
async def poll_all():
    global health_status, last_poll_time, last_check_time
    # 首次拉取 model details
    for p in list(providers):
        try:
            details = await fetch_model_details(p["base_url"], p["api_key"])
            if details:
                model_details.update(details)
        except Exception:
            logger.exception("initial model_details fetch failed: %s", p.get("name"))

    while True:
        try:
            tasks = []
            for p in list(providers):
                # 秘塔匿名配额紧张 / minimax-direct 探针缺 user_id 会误报,例行巡检跳过
                if p.get("protocol") in ("metaso", "minimax-direct", "ima", "lingxi"):
                    continue
                # no_probe: 上游嫌探针占并发(如 leehaha 拼车), 轮询跳过, 健康以真实调用为准
                if p.get("no_probe"):
                    continue
                for m in get_enabled_models(p):
                    tasks.append((p["name"], m, p["base_url"], p["api_key"], p.get("protocol", "openai")))
            sem = asyncio.Semaphore(10)

            async def limited_check(url, key, m, proto):
                async with sem:
                    return await check_model(url, key, m, proto)

            results = await asyncio.gather(
                *[limited_check(url, key, m, proto) for _, m, url, key, proto in tasks],
                return_exceptions=True,
            )
            new_status = {}
            for (name, m, _, _, _), result in zip(tasks, results):
                k = f"{name}||{m}"
                if isinstance(result, Exception):
                    new_status[k] = {
                        "status": "error",
                        "detail": str(result)[:200],
                        "checked_at": time.time(),
                    }
                else:
                    result["checked_at"] = time.time()
                    new_status[k] = result
                    if result.get("status") == "ok":
                        record_success(k)  # 巡检通过立即清熔断，复活渠道马上回排序
                update_model_quality(k, new_status[k])
            health_status = new_status
            last_poll_time = time.time()
            last_check_time = last_poll_time
            await append_history(new_status)
            await maybe_cleanup_history()
            ok_count = sum(1 for v in new_status.values() if v.get("status") == "ok")
            logger.info("poll done: %d/%d ok", ok_count, len(new_status))
        except Exception:
            logger.exception("poll_all loop error")
        # 等待到 last_check_time + POLL_INTERVAL；
        # 若手动检测更新了 last_check_time，则顺延，避免短时间内重复轮询
        while time.time() < last_check_time + POLL_INTERVAL:
            await asyncio.sleep(5)


# ============================================================
# lifespan
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client, poll_task
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(120.0, connect=10.0, read=180.0, pool=30.0),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        trust_env=False,  # 不走系统代理: 长流式响应会被本地代理掐断(502空body元凶)
    )
    app.state.http = http_client
    poll_task = asyncio.create_task(poll_all())
    yield
    if poll_task:
        poll_task.cancel()
    await http_client.aclose()


app = FastAPI(title="模型API网关", lifespan=lifespan)
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))
templates.env.auto_reload = True


# ============================================================
# Pydantic 模型
# ============================================================
class ProviderIn(BaseModel):
    name: str
    base_url: str
    api_key: str
    models: list[str] = []
    free_only: bool = True


class ProviderUpdate(BaseModel):
    name: str | None = None
    base_url: str | None = None
    api_key: str | None = None
    models: list[str] | None = None
    free_only: bool | None = None
    note: str | None = None
    billing: str | None = None


class ToggleModelIn(BaseModel):
    model: str
    enabled: bool


# ============================================================
# 模型选择
# ============================================================
def pick_available_models(model: str | None = None, force: bool = False) -> list[tuple[dict, str]]:
    maybe_reload_providers()
    maybe_reload_routers()
    maybe_reload_meta()
    """返回按质量排序的候选 (provider, model) 列表"""

    raw = []
    unhealthy_raw = []

    # 特殊组 "all"：全部渠道全部模型自动择优
    if model == "all":
        model = None

    # 1. 如果请求的是自定义路由组
    # 组成员支持两种写法："模型名"（所有渠道同名模型都入组）或 "渠道名/模型名"（只入指定渠道）
    if model in ROUTERS:
        target_models = set(ROUTERS[model])
        order = {}
        for i, entry in enumerate(ROUTERS[model]):
            order[entry] = i
            if "/" in entry:
                bare = entry.split("/", 1)[1]  # 限定写法时，裸模型名同序
                # 同名裸模型(如 leehaha/M3 与 openhub/M3)取最靠前的序号,
                # 否则后位条目会覆盖前位置顶 —— 置顶 silently 失效
                if bare not in order or order[bare] > i:
                    order[bare] = i

        # 池通配条目 "SenseNova-*/model": 只匹配该前缀渠道池(20key), 不吸其他同名渠道
        wild = [(i, e.split("/", 1)[0][:-1], e.split("/", 1)[1])
                for i, e in enumerate(ROUTERS[model]) if "/" in e and e.split("/", 1)[0].endswith("*")]

        def _match(p, m):
            if m in target_models:
                return m
            q = f"{p['name']}/{m}"
            if q in target_models:
                return q
            for wi, wbase, wm in wild:
                if wm == m and p["name"].startswith(wbase):
                    return ROUTERS[model][wi]
            return None

        for p in providers:
            for m in get_enabled_models(p):
                hit = _match(p, m)
                if hit is None:
                    continue
                k = f"{p['name']}||{m}"
                if force:
                    raw.append((p, m, k))
                    continue
                st = health_status.get(k, {}).get("status")
                if not is_circuit_open(k) and st in ("ok", None, "unknown"):
                    raw.append((p, m, k))
                else:
                    unhealthy_raw.append((p, m, k))
        if not raw:
            raw = unhealthy_raw
        # 路由组按组内模型列表顺序作为优先级（排前面的先用，失败依序后落）
        # 排序键优先精确匹配 "渠道/模型" 完整条目; 裸名只兜底 ——
        # 否则同名裸模型(如 leehaha/M3 与 openhub/M3)会互相沾光/覆盖顺位
        def _order_key(t):
            p, m = t[0], t[1]
            q = f"{p['name']}/{m}"
            if q in order:
                return order[q]
            if m in order:
                return order[m]
            for wi, wbase, wm in wild:
                if wm == m and p["name"].startswith(wbase):
                    return order[ROUTERS[model][wi]]
            return len(order)
        raw.sort(key=_order_key)
        return [(p, m) for p, m, _ in raw]
        
    # 3. 如果请求的是具体模型
    for p in providers:
        for m in get_enabled_models(p):
            prefixed = f"{p['name']}-{m}"
            slashed = f"{p['name']}/{m}"
            if model and model != m and model != prefixed and model != slashed:
                continue
            k = f"{p['name']}||{m}"
            if force:
                raw.append((p, m, k))
                continue
            st = health_status.get(k, {}).get("status")
            if not is_circuit_open(k) and st in ("ok", None, "unknown"):
                raw.append((p, m, k))
            else:
                unhealthy_raw.append((p, m, k))
    # 自动选择（未指定模型）时保留不健康回退，避免全挂时直接 503；
    # 显式指定模型时若该模型所有渠道均不健康，也放行尝试（上游可能已恢复）
    if not raw:
        raw = unhealthy_raw
    scored = [
        (get_quality_score(k), get_avg_latency(k) or 1e9, p, m)
        for p, m, k in raw
    ]
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [(p, m) for _, _, p, m in scored]


def pick_available_model(model: str | None = None, force: bool = False):
    cands = pick_available_models(model, force)
    return cands[0] if cands else (None, None)


# ============================================================
# Hermes 工具名压缩 / 还原
# ============================================================
HERMES_MAP = [
    ("mcp_hermes_studio_use_hermes_studio_use_", "mcp_hsu_"),
    ("mcp_hermes_studio_devices_hermes_studio_lan_", "mcp_hsd_"),
    ("mcp_hermes_studio_api_hermes_studio_api_", "mcp_hsa_"),
]


def compress_hermes(obj: dict) -> dict:
    s = json.dumps(obj, ensure_ascii=False)
    for long, short in HERMES_MAP:
        s = s.replace(long, short)
    return json.loads(s)


def restore_hermes_text(text: str) -> str:
    for long, short in HERMES_MAP:
        text = text.replace(short, long)
    return text


def merge_reasoning(obj: dict) -> dict:
    """将 reasoning_content 用 <think>...</think> 包裹后合并到 content"""
    choices = obj.get("choices")
    if not choices or not isinstance(choices, list):
        return obj
    for choice in choices:
        target = choice.get("delta") or choice.get("message")
        if not target or not isinstance(target, dict):
            continue
        rc = target.pop("reasoning_content", None)
        if rc is None:
            continue
        wrapped = f"<think>{rc}</think>"
        c = target.get("content")
        if isinstance(c, str) and c:
            target["content"] = c + wrapped
        else:
            target["content"] = wrapped
    return obj

# ============================================================
# 回复语言跟随：根据用户消息语言决定回复语言
# ============================================================
LANG_HINTS = {
    "zh": (
        "\n\n【重要】请始终使用简体中文回答用户。"
        "思考过程(reasoning)也请用中文。"
        "代码、命令、文件名、专有名词、标识符等保持原样即可，不要翻译。"
    ),
    "en": (
        "\n\n[Important] Please always respond to the user in English. "
        "The reasoning process should also be in English. "
        "Keep code, commands, file names, proper nouns, and identifiers as-is; do not translate them."
    ),
}


def detect_user_lang(msgs: list) -> str:
    """检测用户最后一条文本消息的语言，返回 'zh' 或 'en'。
    依据：CJK 字符数与拉丁字母数的比较，谁多跟随谁；
    两者都为 0 时继续向前找；都找不到则默认 'zh'。"""
    for m in reversed(msgs):
        if not isinstance(m, dict) or m.get("role") != "user":
            continue
        c = m.get("content")
        if isinstance(c, list):
            # 多模态：拼接其中的 text 段
            c = " ".join(
                seg.get("text", "")
                for seg in c
                if isinstance(seg, dict) and seg.get("type") == "text"
            )
        if not isinstance(c, str):
            continue
        cjk = sum(1 for ch in c if "\u4e00" <= ch <= "\u9fff")
        lat = sum(1 for ch in c if ch.isascii() and ch.isalpha())
        if cjk == 0 and lat == 0:
            continue
        return "zh" if cjk >= lat else "en"
    return "zh"


def ensure_lang_reply(body: dict) -> dict:
    """根据用户消息语言注入对应的回复语言提示。
    - 已有 system 且为纯文本：在末尾追加指令（带判重，幂等）。
    - 无 system：在最前面插入一条对应语言的 system。
    - 多模态(数组) system 不动，避免破坏结构。"""
    msgs = body.get("messages")
    if not isinstance(msgs, list) or not msgs:
        return body
    lang = detect_user_lang(msgs)
    hint = LANG_HINTS[lang]
    first = msgs[0]
    if isinstance(first, dict) and first.get("role") == "system":
        c = first.get("content")
        if isinstance(c, str) and "请始终使用简体中文" not in c and "always respond to the user in English" not in c:
            first["content"] = c.rstrip() + hint
        return body
    sys_text = ("请使用简体中文回答。" + hint) if lang == "zh" else ("Please respond in English. " + hint)
    msgs.insert(0, {"role": "system", "content": sys_text})
    return body


# ============================================================
# 页面
# ============================================================
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "local_api_key": LOCAL_API_KEY,
            "app_version": APP_VERSION,
        },
    )


# ============================================================
# 管理接口（admin 鉴权）
# ============================================================
@app.get("/api/poll-status")
async def poll_status(_=Depends(verify_admin)):
    return {
        "last_poll_time": last_poll_time,
        "total_models": sum(len(get_enabled_models(p)) for p in providers),
    }


@app.get("/api/history")
async def get_history(hours: int = 24, _=Depends(verify_admin)):
    return await read_history(hours)


@app.get("/api/stability")
async def get_stability(hours: int = 24, _=Depends(verify_admin)):
    records = await read_history(hours)
    model_stats: dict = {}
    for rec in records:
        for key, info in rec.get("data", {}).items():
            if key not in model_stats:
                model_stats[key] = {"ok": 0, "fail": 0, "error": 0, "total": 0, "latencies": []}
            model_stats[key]["total"] += 1
            st = info.get("status", "unknown")
            if st == "ok":
                model_stats[key]["ok"] += 1
                if info.get("latency_ms"):
                    model_stats[key]["latencies"].append(info["latency_ms"])
            elif st == "fail":
                model_stats[key]["fail"] += 1
            elif st == "error":
                model_stats[key]["error"] += 1
    allowed = set()

    for p in providers:
        for m in p.get("models", []):
            k = f"{p['name']}||{m}"
            allowed.add(k)
            if k not in model_stats:
                model_stats[k] = {"ok": 0, "fail": 0, "error": 0, "total": 0, "latencies": []}
    model_stats = {k: v for k, v in model_stats.items() if k in allowed}
    result = []
    for key, s in model_stats.items():
        name, model = key.split("||", 1)
        avg_lat = sum(s["latencies"]) / len(s["latencies"]) if s["latencies"] else None
        result.append({
            "provider": name,
            "model": model,
            "checks": s["total"],
            "ok": s["ok"],
            "fail": s["fail"],
            "error": s["error"],
            "availability": round(s["ok"] / s["total"] * 100, 1) if s["total"] else 0,
            "avg_latency_ms": round(avg_lat) if avg_lat else None,
            "min_latency_ms": min(s["latencies"]) if s["latencies"] else None,
            "max_latency_ms": max(s["latencies"]) if s["latencies"] else None,
            "last_status": health_status.get(key, {}).get("status", "unknown"),
        })
    result.sort(key=lambda x: (-x["availability"], x["avg_latency_ms"] or 99999))
    return result


@app.get("/api/logs")
async def get_logs(n: int = 50, _=Depends(verify_admin)):
    """最近 n 条调用日志（时间倒序），供面板运行日志模块使用"""
    n = max(1, min(n, 200))
    if not USAGE_FILE.exists():
        return {"ok": True, "logs": []}
    out = []
    try:
        with open(USAGE_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    out.append(json.loads(line))
                except Exception:
                    pass
    except Exception:
        pass
    out = out[-n:][::-1]
    return {"ok": True, "logs": out}


@app.get("/api/inflight", dependencies=[Depends(verify_admin)])
async def inflight_status():
    """进行中请求（TTL 900s 兜底），监控 GUI 正在调用高亮以此为准"""
    now = time.time()
    return [v for v in INFLIGHT.values() if now - v.get("ts", 0) < 900]


@app.get("/api/recent-serving")
async def recent_serving(minutes: int = 15, _=Depends(verify_admin)):
    """真实调用记录统计：各路由组最近实际由谁接单（面板'实际首选'以此为准，健康推测仅参考）"""
    records = await read_usage(days=1)
    cutoff = time.time() - max(1, min(minutes, 1440)) * 60
    agg = {}
    for r in records:
        if r.get("ts", 0) < cutoff:
            continue
        g = r.get("req") or "direct"
        key = f"{r.get('provider')}/{r.get('model')}"
        a = agg.setdefault(g, {})
        st = a.setdefault(key, {"calls": 0, "pt": 0, "ct": 0, "last_ts": 0})
        st["calls"] += 1
        st["pt"] += r.get("pt", 0) or 0
        st["ct"] += r.get("ct", 0) or 0
        st["last_ts"] = max(st["last_ts"], r.get("ts", 0))
    # 最近回落原因（真实调用失败落盘, 最近的在前）
    fbs = []
    fb_file = DATA_DIR / "fallbacks.jsonl"
    if fb_file.exists():
        try:
            lines = fb_file.read_text(encoding="utf-8").splitlines()
            for ln in reversed(lines[-300:]):
                try:
                    rec = json.loads(ln)
                except Exception:
                    continue
                if rec.get("ts", 0) >= cutoff:
                    fbs.append(rec)
                if len(fbs) >= 30:
                    break
        except Exception:
            pass
    return {"ok": True, "window_minutes": minutes, "groups": agg, "fallbacks": fbs}


@app.get("/api/usage")
async def get_usage(days: int = 1, _=Depends(verify_admin)):
    days = max(1, min(days, MAX_USAGE_DAYS))
    records = await read_usage(days)
    total = {"pt": 0, "ct": 0, "tt": 0, "requests": 0}
    by_day = {}
    by_model = {}
    for r in records:
        ts = r.get("ts", 0)
        day = time.strftime("%Y-%m-%d", time.localtime(ts))
        pt = r.get("pt", 0) or 0
        ct = r.get("ct", 0) or 0
        tt = r.get("tt", 0) or (pt + ct)
        m = r.get("model", "unknown")
        p = r.get("provider", "unknown")
        total["pt"] += pt
        total["ct"] += ct
        total["tt"] += tt
        total["requests"] += 1
        d = by_day.setdefault(day, {"pt": 0, "ct": 0, "tt": 0, "requests": 0})
        d["pt"] += pt
        d["ct"] += ct
        d["tt"] += tt
        d["requests"] += 1
        mk = f"{p} · {m}"
        mm = by_model.setdefault(mk, {"pt": 0, "ct": 0, "tt": 0, "requests": 0, "provider": p, "model": m})
        mm["pt"] += pt
        mm["ct"] += ct
        mm["tt"] += tt
        mm["requests"] += 1
    by_day_list = [{"date": d, **v} for d, v in sorted(by_day.items())]
    by_model_list = [
        {"provider": v["provider"], "model": v["model"], "pt": v["pt"], "ct": v["ct"], "tt": v["tt"], "requests": v["requests"]}
        for _, v in sorted(by_model.items(), key=lambda x: -x[1]["tt"])
    ]
    return {"days": days, "total": total, "by_day": by_day_list, "by_model": by_model_list}


@app.get("/api/model-details")
async def get_model_details(_=Depends(verify_admin)):
    maybe_reload_meta()
    merged = {}
    # 视觉能力标记: specs里的vision标志 与 vision_models名单 都认(单一来源会漂移)
    try:
        _raw = json.load(open(META_FILE, encoding="utf-8"))
        _meta_vision = {v.lower() for v in _raw.get("vision_models", [])}
        for _k, _v in (_raw.get("model_specs") or {}).items():
            if isinstance(_v, dict) and _v.get("vision"):
                _meta_vision.add(_k.lower())
    except Exception:
        _meta_vision = set()

    def _is_vision(name: str) -> bool:
        n = (name or "").lower()
        return any(n == v or n.endswith(v) or v.endswith("/" + n) for v in _meta_vision)
    # 1. 上游探测结果（键为上游模型 id / 原始名）
    for k, v in model_details.items():
        merged[k] = dict(v)
    # 2. 对 providers 里每个模型(原始名)，用别名归一化查 meta 兜底
    #    解决魔搭等 provider 用别名形式(如 ZhipuAI/GLM-5.2)而 meta 里
    #    只有规范化名(如 glm-5.2) 导致前端查不到上下文/描述的问题

    for p in providers:
        for m in p.get("models", []):
            entry = merged.setdefault(m, {})
            norm = MODEL_ALIASES.get(m, m)
            entry["vision"] = entry.get("vision") or _is_vision(m) or _is_vision(norm)
            meta_desc = _meta_lookup(MODEL_DESCRIPTIONS, m, norm) or {}
            spec = _meta_lookup(MODEL_SPECS, m, norm)
            if spec:
                entry["total_params_b"] = spec.get("total") or spec.get("total_params_b")
                entry["active_params_b"] = spec.get("active") or spec.get("active_params_b")
                if spec.get("note"):
                    entry["spec_note"] = spec["note"]
            if not entry.get("context_length"):
                ctx = meta_desc.get("ctx") or _meta_lookup(CONTEXT_LIMITS, m, norm)
                if ctx:
                    entry["context_length"] = ctx
            if not entry.get("desc"):
                desc = meta_desc.get("desc", "")
                if desc:
                    entry["desc"] = desc
    # 3. 对 meta 里规范化名也建条目（兼容以规范化名查询）
    for k, v in MODEL_DESCRIPTIONS.items():
        merged.setdefault(k, {})
        merged[k]["vision"] = merged[k].get("vision") or _is_vision(k)
        if not merged[k].get("context_length"):
            merged[k]["context_length"] = v.get("ctx")
        merged[k]["desc"] = v.get("desc", "")
    return merged


@app.get("/api/context-limits")
async def get_context_limits(_=Depends(verify_admin)):
    return {"ok": True, "data": CONTEXT_LIMITS}

class ContextLimitUpdate(BaseModel):
    model: str
    context_length: int

@app.put("/api/context-limits")
async def update_context_limit(req: ContextLimitUpdate, _=Depends(verify_admin)):
    global CONTEXT_LIMITS, meta
    meta = load_meta()
    if "context_limits" not in meta:
        meta["context_limits"] = {}
    meta["context_limits"][req.model] = req.context_length
    CONTEXT_LIMITS[req.model] = req.context_length
    META_FILE.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    return {"ok": True}


@app.delete("/api/context-limits/{model}")
async def delete_context_limit(model: str, _=Depends(verify_admin)):
    """删除某条自定义上下文长度配置"""
    global CONTEXT_LIMITS, meta
    meta = load_meta()
    if "context_limits" in meta and model in meta["context_limits"]:
        del meta["context_limits"][model]
    CONTEXT_LIMITS.pop(model, None)
    META_FILE.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    return {"ok": True}


@app.get("/api/routers")
async def get_routers_api(_=Depends(verify_admin)):
    maybe_reload_routers()
    return {"ok": True, "data": ROUTERS}

@app.post("/api/routers")
async def save_routers_api(request: Request, _=Depends(verify_admin)):
    global ROUTERS
    body = await request.json()
    # 调整审计: 每次保存记录差异(谁动了哪组、新顺位), 供面板/追溯
    try:
        changes = {}
        for g, new_list in body.items():
            old_list = ROUTERS.get(g, []) if isinstance(ROUTERS, dict) else []
            if old_list != new_list:
                changes[g] = {"from": old_list, "to": new_list}
        if changes:
            await append_history({"adjustment": {"ts": time.time(), "changes": changes}})
    except Exception:
        logger.exception("adjustment log failed")
    ROUTERS = body
    save_routers()
    return {"ok": True}


# ---------- 系统公告（Gitee 远程，本地兜底） ----------
DEFAULT_ANNOUNCEMENT_URL = "https://gitee.com/ywtc000/dongye/raw/master/announcement.md"
ANNOUNCEMENT_CACHE_FILE = DATA_DIR / "announcement_cache.json"
_announcement_cache = {"content": None, "ts": 0}
ANNOUNCEMENT_TTL = 300


@app.get("/api/announcement")
async def get_announcement(_=Depends(verify_admin)):
    """公告弹窗已移除：不再远程抓取，固定返回空。"""
    return {"ok": False, "content": ""}


@app.get("/api/providers")
async def list_providers(_=Depends(verify_admin)):
    maybe_reload_providers()
    result = []
    # 日额度: 今日用量(本地零点起) + 估算总量; 真实用量超出估算时自学习上调并落盘(每日滚动修正)
    meta_path = str(META_FILE)
    try:
        meta = json.load(open(meta_path, encoding="utf-8"))
    except Exception:
        meta = {}
    quota_est = meta.setdefault("daily_quota_est", {})
    lt = time.localtime()
    day_start = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, 0, 0, 0))
    daily_used = {}
    try:
        for r in await read_usage(days=1):
            if r.get("ts", 0) >= day_start:
                p = r.get("provider")
                if p:
                    daily_used[p] = daily_used.get(p, 0) + (r.get("pt", 0) or 0) + (r.get("ct", 0) or 0)
    except Exception:
        pass
    quota_dirty = False
    for pr in providers:
        if str(pr.get("billing", "")).startswith("日限额"):
            pn = pr["name"]
            used = daily_used.get(pn, 0)
            est = quota_est.get(pn, 0)
            if used > est:
                quota_est[pn] = used  # 估算总量至少不低于历史最高日耗
                quota_dirty = True
        elif pr["name"] in quota_est and not str(pr.get("billing", "")).startswith("日限额"):
            quota_est.pop(pr["name"], None)  # 渠道改性质后清掉旧估算
    if quota_dirty:
        try:
            meta["daily_quota_est"] = quota_est
            json.dump(meta, open(meta_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        except Exception:
            pass

    # 窗口制渠道(配置 quota_url): 实时拉上游额度查询端口, 5 分钟缓存
    quota_live = {}
    for pr in providers:
        qurl = pr.get("quota_url")
        if not qurl:
            continue
        now_t = time.time()
        c = _QUOTA_CACHE.get(pr["name"])
        if c and now_t - c[0] < 300:
            quota_live[pr["name"]] = c[1]
            continue
        try:
            qr = await http_client.get(qurl, headers={"Authorization": f"Bearer {pr.get('api_key', '')}"}, timeout=10)
            qd = qr.json()
            # new-api 系计费口(/v1/dashboard/billing/*): total_usage=已用美元, hard_limit=额度
            if qd.get("object") == "billing_subscription":
                try:
                    ur = await http_client.get(str(qurl).replace("/subscription", "/usage"),
                                               headers={"Authorization": f"Bearer {pr.get('api_key', '')}"}, timeout=10)
                    used_usd = (ur.json() or {}).get("total_usage", 0) or 0
                except Exception:
                    used_usd = 0
                info = {"used_usd": round(used_usd, 4), "granted_usd": qd.get("hard_limit_usd", 0)}
                quota_live[pr["name"]] = info
                _QUOTA_CACHE[pr["name"]] = (now_t, info)
                continue
            if qd.get("code") and qd.get("data"):
                dd = qd["data"]
                granted = dd.get("total_granted", 0)
                # 本窗口真实用量 = 总量 - 剩余 (total_used 是跨窗口累计值, 直接用会显示 >100%)
                used = max(0, granted - dd.get("total_available", 0))
                info = {"used": used, "granted": granted,
                        "reset_in": max(0, int(dd.get("quota_reset_next_at", 0) - now_t))}
                quota_live[pr["name"]] = info
                _QUOTA_CACHE[pr["name"]] = (now_t, info)
        except Exception:
            pass

    for p in providers:
        item = {
            "name": p["name"],
            "base_url": p["base_url"],
            "api_key_masked": mask_key(p.get("api_key", "")),
            "models": p.get("models", []),
            "disabled_models": p.get("disabled_models", []),
            "_archived_models": p.get("_archived_models", []),
            "free_only": p.get("free_only", True),
            "note": p.get("note", ""),
            "billing": p.get("billing", ""),
            "daily_used": daily_used.get(p["name"], 0),
            # 显示估算 = 存储天花板×1.25 余量(向上取整): 渠道活着时用量永远追不上估算, 不显"打穿"假象;
            # 真打穿后自学习上调天花板, 显示随之水涨船高
            "daily_quota": (lambda est: int((est * 1.25) // 1) + 1 if est else 0)(
                max(quota_est.get(p["name"], 0), int(daily_used.get(p["name"], 0) * 1.25))
            ) if str(p.get("billing", "")).startswith("日限额") else 0,
            "quota_reset_in": quota_live.get(p["name"], {}).get("reset_in", -1),
            "upstream_reset_in": _next_anchor_in(p.get("upstream_reset_hours") or []),
            "expire_date": p.get("expire_date", ""),
            "health": {},
        }
        for m in p.get("models", []):
            k = f"{p['name']}||{m}"
            h = dict(health_status.get(k, {"status": "unknown"}))
            # 真实调用的熔断状态并入：面板据此反映实际回落，而不是只看巡检
            h["circuit_open"] = is_circuit_open(k)
            if h["circuit_open"]:
                cb = circuit_breaker.get(k) or {}
                remain = int((cb.get("open_until") or 0) - time.time())
                h["status"] = "fail"
                h["circuit_remain_s"] = max(remain, 0)
                h["last_status"] = "circuit"
            item["health"][m] = h
        ql = quota_live.get(p["name"])
        if ql:  # 窗口制渠道用上游实账覆盖估算
            item["daily_used"] = ql.get("used", 0)
            item["daily_quota"] = ql.get("granted", 0)
            if "used_usd" in ql:  # new-api 系美元计费(如 whya 按次折算)
                item["used_usd"] = ql["used_usd"]
        result.append(item)
    return result


@app.post("/api/providers")
async def add_provider(data: ProviderIn, _=Depends(verify_admin)):
    async with providers_lock:
        for p in providers:
            if p["name"] == data.name:
                raise HTTPException(400, "名称已存在")
        if not data.models:
            data.models = await fetch_models(data.base_url, data.api_key, data.free_only)
        providers.append(data.model_dump())
        save_providers(providers)
    return {"ok": True}


@app.post("/api/providers/{name}/fetch-models")
async def refresh_models(name: str, _=Depends(verify_admin)):
    async with providers_lock:
        for p in providers:
            if p["name"] == name:
                models = await fetch_models(p["base_url"], p["api_key"], p.get("free_only", True))
                if models:
                    p["models"] = models
                    save_providers(providers)
                return {"ok": True, "models": models}
    raise HTTPException(404, "未找到")


@app.get("/api/providers/{name}/available-models")
async def get_available_models(name: str, _=Depends(verify_admin)):

    for p in providers:
        if p["name"] == name:
            models = await fetch_models(p["base_url"], p["api_key"], free_only=False)
            return {"ok": True, "models": models}
    raise HTTPException(404, "未找到")


@app.put("/api/providers/{name}")
async def update_provider(name: str, data: ProviderUpdate, _=Depends(verify_admin)):
    async with providers_lock:
        for i, p in enumerate(providers):
            if p["name"] == name:
                providers[i].update(data.model_dump(exclude_unset=True))
                save_providers(providers)
                return {"ok": True}
    raise HTTPException(404, "未找到")


@app.delete("/api/providers/{name}")
async def delete_provider(name: str, _=Depends(verify_admin)):
    global providers
    async with providers_lock:
        providers = [p for p in providers if p["name"] != name]
        save_providers(providers)
    return {"ok": True}


@app.post("/api/providers/{name}/toggle-model")
async def toggle_model(name: str, data: ToggleModelIn, _=Depends(verify_admin)):
    async with providers_lock:
        for p in providers:
            if p["name"] == name:
                disabled = p.get("disabled_models", [])
                if data.enabled:
                    if data.model in disabled:
                        disabled.remove(data.model)
                else:
                    if data.model not in disabled:
                        disabled.append(data.model)
                p["disabled_models"] = disabled
                save_providers(providers)
                return {"ok": True, "disabled_models": disabled}
    raise HTTPException(404, "未找到")


class BulkToggleIn(BaseModel):
    """整组(池)批量切换: 一次请求落盘, 替代前端 N×M 次串行请求(曾把主线程打死)"""
    patterns: list[str]  # 通配或精确渠道名, 如 ["SenseNova-*"] / ["SenseNova-1"]
    enabled: bool


@app.post("/api/providers/toggle-bulk")
async def toggle_bulk(d: BulkToggleIn, _=Depends(verify_admin)):
    # 注意路由顺序: 该路径含静态段, 必须注册在 /toggle-model 同级, FastAPI 按注册顺序匹配
    touched = 0
    async with providers_lock:
        for p in providers:
            hit = any(
                (pat.endswith("*") and p["name"].startswith(pat[:-1])) or p["name"] == pat
                for pat in d.patterns
            )
            if not hit:
                continue
            models = p.get("models", [])
            if not models:
                continue
            disabled = set(p.get("disabled_models", []))
            if d.enabled:
                disabled -= set(models)
            else:
                disabled |= set(models)
            p["disabled_models"] = sorted(disabled)
            touched += 1
        if touched:
            save_providers(providers)
    return {"ok": True, "touched": touched}


@app.post("/api/check/{name}/{model}")
async def manual_check(name: str, model: str, _=Depends(verify_admin)):

    for p in providers:
        if p["name"] == name:
            result = await check_model(p["base_url"], p["api_key"], model, p.get("protocol", "openai"))
            k = f"{name}||{model}"
            result["checked_at"] = time.time()
            health_status[k] = result
            update_model_quality(k, result)
            await append_history({k: result})
            return result
    raise HTTPException(404, "未找到")


@app.post("/api/check/all")
async def check_all(_=Depends(verify_admin)):
    results = {}
    tasks = []

    for p in providers:
        if p.get("no_probe"):
            continue
        for m in get_enabled_models(p):
            tasks.append((p["name"], m, p["base_url"], p["api_key"], p.get("protocol", "openai")))
    sem = asyncio.Semaphore(10)

    async def limited_check(url, key, m, proto):
        async with sem:
            return await check_model(url, key, m, proto)

    check_results = await asyncio.gather(
        *[limited_check(url, key, m, proto) for _, m, url, key, proto in tasks],
        return_exceptions=True,
    )
    for (name, m, _, _), result in zip(tasks, check_results):
        k = f"{name}||{m}"
        if isinstance(result, Exception):
            results[k] = {"status": "error", "detail": str(result)[:200], "checked_at": time.time()}
        else:
            result["checked_at"] = time.time()
            results[k] = result
        update_model_quality(k, results[k])
    health_status.update(results)
    await append_history(results)
    mark_full_check()
    return results


# ============================================================
# 代理（客户端鉴权）
# ============================================================
async def _try_stream_forward(url, body, headers, key, provider_name, model):
    """尝试建立流式上游连接。成功返回 (StreamingResponse, None)，失败返回 (None, error)"""
    t0 = time.time()
    req = http_client.build_request("POST", url, json=body, headers=headers)
    try:
        resp = await http_client.send(req, stream=True)
    except httpx.RequestError as e:
        logger.warning("stream connect error to %s: %s", provider_name, repr(e))
        record_fail(key)
        return None, str(e)

    if resp.status_code != 200:
        try:
            err_bytes = await resp.aread()
            err_msg = err_bytes.decode("utf-8", errors="ignore")
        except Exception:
            err_msg = ""
        await resp.aclose()
        logger.warning("upstream stream error %d from %s", resp.status_code, provider_name)
        record_fail(key)
        return None, f"upstream {resp.status_code}"

    record_success(key)

    async def gen():
        usage_obj = None
        content_chars = 0
        got_any = False
        try:
            ait = resp.aiter_lines()
            idle = 120.0  # 首行放宽: 巨型prompt上游TTFT本就可达1-2分钟
            while True:
                # 流空闲看门狗: 迟迟不吐下一行(上游接单不干活)→掐断+立即熔断, 不陪它干等读超时
                try:
                    line = await asyncio.wait_for(ait.__anext__(), timeout=idle)
                except StopAsyncIteration:
                    break
                except asyncio.TimeoutError:
                    raise TimeoutError(f"stream idle >{idle:.0f}s (上游接单不吐字)")
                idle = STREAM_IDLE_TIMEOUT  # 首行之后, 行间空闲45秒即掐
                got_any = True
                if not line:
                    continue
                if not line.startswith("data: "):
                    yield line + "\n"
                    continue
                data_str = line[6:]
                if data_str.strip() == "[DONE]":
                    yield "data: [DONE]\n\n"
                    break
                try:
                    obj = json.loads(data_str)
                    if obj.get("usage"):
                        usage_obj = obj["usage"]
                    if "model" in obj and isinstance(obj["model"], str):
                        obj["model"] = f"{provider_name} · {model}"
                    choices = obj.get("choices") or []
                    if choices:
                        delta = choices[0].get("delta") or {}
                        c = delta.get("content")
                        if isinstance(c, str):
                            content_chars += len(c)
                        # reasoning_content 保留原生字段透传，不再包装 <think> 混入正文
                        # （正文里的 think 残留会污染会话历史，导致模型复读循环）
                        rc = delta.get("reasoning_content")
                        if rc is not None:
                            if isinstance(rc, str):
                                # 思维链也算输出字符——glm-5.2 这类推理模型正文全在 reasoning 里,
                                # 不算则 usage 记零蛋(面板输入/输出全是-)
                                content_chars += len(rc)
                            delta.pop("content", None)
                    out = json.dumps(obj, ensure_ascii=False)
                    out = restore_hermes_text(out)
                    yield "data: " + out + "\n\n"
                except json.JSONDecodeError:
                    yield line + "\n"
            if content_chars == 0:
                # 200 但零输出=静默拒绝/空流: 立即熔断, 下一请求立刻换人
                open_circuit_now(f"{provider_name}||{model}", "空流(200但零输出)")
        except Exception as e:
            # 流中断/空闲超时: 强制开闸(确定性失败), 下个请求立刻换人
            logger.warning("stream aborted from %s: %r", provider_name, e)
            open_circuit_now(f"{provider_name}||{model}", f"stream aborted: {repr(e)[:120]}")
            raise
        finally:
            await resp.aclose()
            if content_chars > 0:
                note_real_success(provider_name, model)
            try:
                pt = (usage_obj or {}).get("prompt_tokens", 0) or 0
                ct = (usage_obj or {}).get("completion_tokens", 0) or 0
                if not (ct or pt) and content_chars > 0:
                    # 推理模型(如kimi-k3/glm-5.2)流式常不回usage: 粗估记账, 不留零蛋。
                    # 思维链比正文更密, 按 2.5 字符/tok 折(正文仍按 2), 别把思考过程虚高成输出。
                    pt, ct = _estimate_usage(body, content_chars)
                if ct or pt:
                    await append_usage({
                        "ts": time.time(), "model": model,
                        "provider": provider_name,
                        "pt": pt, "ct": ct, "tt": pt + ct,
                        "dur": round(time.time() - t0, 1),
                    })
            except Exception:
                logger.exception("append_usage(stream) failed")

    return StreamingResponse(gen(), media_type="text/event-stream"), None


# ===== 问小白 (wenxiaobai) Web 协议适配层 =====
# 原理：复刻前端 signFn —— x-date + body SHA256 digest → HMAC-SHA1（密钥硬编码于前端 JS）
WXB_HMAC_KEY = "TkoWuEN8cpDJubb7Zfwxln16NQDZIc8z"


def _wxb_sign_and_headers(provider: dict, body_str: str) -> dict:
    digest = "SHA-256=" + base64.b64encode(hashlib.sha256(body_str.encode("utf-8")).digest()).decode()
    xdate = formatdate(usegmt=True)
    sign_str = f"x-date: {xdate}\ndigest: {digest}"
    sig = base64.b64encode(hmac.new(WXB_HMAC_KEY.encode(), sign_str.encode("utf-8"), hashlib.sha1).digest()).decode()
    auth = (f'hmac username="web.1.0.beta", algorithm="hmac-sha1", '
            f'headers="x-date digest", signature="{sig}"')
    return {
        "Accept": "text/event-stream, application/json",
        "Content-type": "application/json",
        "Authorization": auth,
        "x-date": xdate,
        "Digest": digest,
        "X-Yuanshi-AppName": "wenxiaobai",
        "X-Yuanshi-AppVersionName": "6.2.0",
        "X-Yuanshi-Channel": "browser",
        "X-Yuanshi-DeviceId": provider.get("device_id", ""),
        "X-Yuanshi-DeviceMode": "Chrome",
        "X-Yuanshi-DeviceOS": "146",
        "X-Yuanshi-Locale": "zh",
        "X-Yuanshi-Platform": "web",
        "X-Yuanshi-TimeZone": "Asia/Shanghai",
        "X-Yuanshi-Authorization": f"Bearer {provider.get('api_key', '')}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/140.0 Safari/537.36",
    }


# 问小白网页端混入正文的状态文本，转发时丢弃
_WXB_ARTIFACTS = {"全网收集资料中", "正在思考", "深度思考中"}


# ===== MiniMax (agent.minimaxi.com) 原生适配层 =====
# 协议(minimax2api 项目逆向):三头签名
#   x-timestamp = unix秒; x-signature = MD5(ts + "I*7Cf%WZ#S&%1RlZJ&C2" + body)
#   yy = MD5(encodeURIComponent(full_url) + "_" + body + MD5(毫秒) + "ooui")
# 查询串顺序固定: token, uuid, device_id, user_id, screen_width, screen_height
# 流程: GET /minimax-cloud/api/v1/agent 发现 agent_id(缓存) →
#       POST /minimax-cloud/api/v1/agent/{agent_id}/session 建会话 →
#       POST /archon/api/v1/session/{sid}/message {content,turn_id,worktreeMode[,mode=think]}
# SSE 帧深层字段:正文 msg_content/msgContent/content/text/delta/answer,思考 reasoning_content 等
_MM_SALT = "I*7Cf%WZ#S&%1RlZJ&C2"
_MM_THINK_KEYS = ("reasoning_content", "thinking_content", "reason_content",
                  "think_content", "reasoning", "thinking")
_MM_CONTENT_KEYS = ("msg_content", "msgContent", "content", "text", "delta", "answer")


def _mm_md5(s: str) -> str:
    import hashlib as _h
    return _h.md5(s.encode("utf-8")).hexdigest()


def _mm_enc_uri(s: str) -> str:
    # 等价 JS encodeURIComponent:空格->%20,不转 ~!*'()
    import urllib.parse as _up
    return _up.quote(s, safe="-_.!~*'()")


def _mm_deep_first(obj, keys):
    """深度优先在嵌套结构里找第一个命中 key 的字符串值"""
    if isinstance(obj, dict):
        for k in keys:
            v = obj.get(k)
            if isinstance(v, str) and v:
                return v
        for v in obj.values():
            r = _mm_deep_first(v, keys)
            if r:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = _mm_deep_first(v, keys)
            if r:
                return r
    return ""


def _mm_headers(provider: dict, url: str, body: str) -> dict:
    import time as _t
    ts = int(_t.time())
    ms = int(_t.time() * 1000)
    base = provider["base_url"].rstrip("/")
    return {
        "accept": "text/event-stream, application/json, */*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "content-type": "application/json",
        "origin": base, "referer": base + "/",
        "user-agent": _DANGBEI_UA,
        "token": provider["api_key"],
        "x-timestamp": str(ts),
        "x-signature": _mm_md5(f"{ts}{_MM_SALT}{body}"),
        "yy": _mm_md5(f"{_mm_enc_uri(url)}_{body}{_mm_md5(str(ms))}ooui"),
    }


def _mm_query(provider: dict) -> str:
    import uuid as _u
    if not provider.get("_mm_uuid"):
        provider["_mm_uuid"] = str(_u.uuid4())
        provider["_mm_device"] = str(_u.uuid4().int)[:8]
    parts = [f"token={_mm_enc_uri(provider['api_key'])}",
             f"uuid={provider['_mm_uuid']}",
             f"device_id={provider['_mm_device']}",
             f"user_id={provider.get('user_id','')}",
             "screen_width=1920", "screen_height=1080"]
    return "&".join(parts)


async def _mm_agent_id(provider: dict) -> str:
    if provider.get("agent_id"):
        return provider["agent_id"]
    base = provider["base_url"].rstrip("/")
    url = f"{base}/minimax-cloud/api/v1/agent?{_mm_query(provider)}"
    body = ""
    r = await http_client.get(url, headers=_mm_headers(provider, url, body), timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"agent list HTTP {r.status_code}: {r.text[:150]}")
    jd = r.json()
    agents = jd.get("agents") or (jd.get("data") or {}).get("agents") or []
    for a in agents:
        if str(a.get("agent_role") or a.get("role") or "").lower() == "general":
            provider["agent_id"] = str(a.get("name") or a.get("id") or "")
            return provider["agent_id"]
    if agents:
        provider["agent_id"] = str(agents[0].get("name") or agents[0].get("id") or "")
        return provider["agent_id"]
    raise RuntimeError("no minimax agent found")


async def _forward_minimax(provider: dict, model: str, openai_body: dict, stream: bool, k: str):
    """MiniMax 原生转发。返回 (Response 或 None, last_err)。"""
    import uuid as _u
    base = provider["base_url"].rstrip("/")
    label = f"{provider['name']} · {model}"
    msgs = []
    for m in openai_body.get("messages") or []:
        c = m.get("content")
        if isinstance(c, list):
            c = "".join(x.get("text", "") for x in c if isinstance(x, dict))
        c = (c or "").strip()
        if c:
            msgs.append(c if m.get("role") == "user" else f"{m.get('role', 'user')}: {c}")
    prompt = "\n".join(msgs) or "你好"
    prompt, __tools_ctx = inject_tools(openai_body, prompt)
    _ev_types, _first_frames = {}, []
    try:
        # 1. 建会话
        aid = await _mm_agent_id(provider)
        s_url = f"{base}/minimax-cloud/api/v1/agent/{aid}/session?{_mm_query(provider)}"
        s_body = json.dumps({"agent_id": aid, "worktreeMode": False},
                            separators=(",", ":"), ensure_ascii=False)
        r = await http_client.post(s_url, content=s_body.encode("utf-8"),
                                   headers=_mm_headers(provider, s_url, s_body), timeout=60)
        if r.status_code in (401, 403):
            record_fail(k, str("minimax token invalid")[:200])
            return None, "minimax token invalid"
        sid = ""
        try:
            d = r.json()
            sid = str((d.get("data") or {}).get("session_id") or d.get("session_id") or "")
        except Exception:
            pass
        if not sid:
            record_fail(k, str(f"minimax session HTTP {r.status_code}: {r.text[:150]}")[:200])
            return None, f"minimax session HTTP {r.status_code}: {r.text[:150]}"
        # 2. 发消息
        m_body = {"content": prompt, "turn_id": str(_u.uuid4()), "worktreeMode": False}
        if "thinking" in model:
            m_body["mode"] = "think"
        m_url = f"{base}/archon/api/v1/session/{sid}/message?{_mm_query(provider)}"
        m_json = json.dumps(m_body, separators=(",", ":"), ensure_ascii=False)
        req = http_client.build_request("POST", m_url, content=m_json.encode("utf-8"),
                                        headers=_mm_headers(provider, m_url, m_json))
        resp = await http_client.send(req, stream=True)
        if resp.status_code != 200:
            detail = (await resp.aread())[:200]
            await resp.aclose()
            record_fail(k, str(f"minimax message HTTP {resp.status_code}: {detail}")[:200])
            return None, f"minimax message HTTP {resp.status_code}: {detail}"

        async def mm_gen(rp=resp, kk=k, pn=provider["name"], ml=label, md=model, note_on_empty=False):
            chars = 0
            err_text = ""
            buf = ""
            try:
                async for chunk in rp.aiter_bytes():
                    buf += chunk.decode("utf-8", errors="replace").replace("\r\n", "\n")
                    while "\n\n" in buf:
                        frame, buf = buf.split("\n\n", 1)
                        data = ""
                        for line in frame.split("\n"):
                            if line.startswith("data:"):
                                data = line[5:].strip()
                        if not data or data == "[DONE]":
                            continue
                        try:
                            ev = json.loads(data)
                        except Exception:
                            continue
                        try:
                            for _ek in ev:
                                _ev_types[_ek] = _ev_types.get(_ek, 0) + 1
                            if len(_first_frames) < 8:
                                _first_frames.append(data[:200])
                        except Exception:
                            pass
                        # 错误终止帧: 配额上限(42212)/风控等以 finish_reason:"error" 送达
                        am = ev.get("agent_message") or {}
                        if (ev.get("agent_message_chunk") or {}).get("finish_reason") == "error" \
                           or (am.get("role") == "assistant" and am.get("finish_reason") == "error"):
                            t = str(am.get("msg_content") or "").strip()
                            if t and not err_text:
                                err_text = t[:120]
                        # 只取 assistant 的增量 chunk;agent_message(用户回显/完整汇总)跳过避免重复
                        chunk = ev.get("agent_message_chunk") or {}
                        if chunk.get("role") == "assistant" and chunk.get("msg_content"):
                            txt = chunk["msg_content"]
                            chars += len(txt)
                            yield _wxb_sse_to_openai_chunk(ml, txt)
                        elif chunk.get("role") == "assistant":
                            # agent 内部工具执行期的 thinking/进度帧: 记为有效活动
                            think = _mm_deep_first(chunk, _MM_THINK_KEYS)
                            if think:
                                chars += len(think)
                if chars == 0:
                    # 200 但零正文 = 上游拒绝（配额/风控），记失败而非假成功
                    record_fail(kk, f"minimax 拒绝: {err_text}" if err_text else "minimax 空回复(200但零正文)")
                    if note_on_empty:
                        warn = "⚠️ minimax " + (err_text or "返回空回复")
                        yield _wxb_sse_to_openai_chunk(ml, warn)
                yield ("data: " + json.dumps({"id": "mm_gateway", "object": "chat.completion.chunk",
                       "model": ml, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
                       ensure_ascii=False) + "\n\n")
                yield "data: [DONE]\n\n"
                if chars:
                    record_success(kk)  # 零正文已记失败，不能又清熔断
            finally:
                await rp.aclose()
                try:
                    await append_usage({"ts": time.time(), "model": md, "provider": pn,
                                        "pt": 0, "ct": chars, "tt": chars})
                except Exception:
                    pass

        if stream:
            record_success(k)
            return StreamingResponse(mm_gen(note_on_empty=True), media_type="text/event-stream"), None
        chunks = []

        async def collect():
            async for item in mm_gen():
                chunks.append(item)
            texts = []
            for item in chunks:
                try:
                    texts.append(json.loads(item[6:])["choices"][0]["delta"].get("content") or "")
                except Exception:
                    pass
            full = "".join(texts)
            if not full.strip():
                return None  # 失败已由生成器记录（含上游错误原因）
            tc, text_out = _ts_extract(full, __tools_ctx)
            msg = {"role": "assistant", "content": text_out}
            finish = "stop"
            if tc:
                msg["tool_calls"] = tc
                msg["content"] = text_out or None
                finish = "tool_calls"
            return JSONResponse(content={
                "id": "mm_gateway", "object": "chat.completion", "created": int(time.time()),
                "model": label,
                "choices": [{"index": 0, "message": msg, "finish_reason": finish}],
                "usage": {"prompt_tokens": 0, "completion_tokens": len(full), "total_tokens": len(full)},
            })
        collected = await collect()
        if collected is None:
            logger.warning("minimax empty reply | ev_types=%s first_frames=%s",
                           _ev_types, _first_frames[:3])
            return None, "minimax empty reply"
        return collected, None
    except httpx.RequestError as e:
        logger.warning("minimax forward error: %r", e)
        record_fail(k, str(str(e))[:200])
        return None, str(e)
    except Exception as e:
        logger.warning("minimax forward error: %r", e)
        record_fail(k, str(str(e))[:200])
        return None, str(e)


# ===== 优AI (chat.52who.top) 适配层 =====
# 协议要点（2026-09 逆向）：POST {base}/api/chatgpt/chat-process
# 头：fingerprint(随机数字即可,游客身份) + x-website-domain
# 体：{model:"gpt-5.5", prompt, options:{groupId:0,...}}
# 响应：NDJSON 流，行如 {"content":[{"type":"text","text":"素"}],"text":"素"}
# 游客按 fingerprint 计积分，随机指纹=新额度，无鉴权


async def _forward_52who(provider: dict, model: str, openai_body: dict, stream: bool, k: str):
    """优AI转发。返回 (Response 或 None, last_err)。"""
    import secrets as _sec
    base = provider["base_url"].rstrip("/")
    label = f"{provider['name']} · {model}"
    msgs = []
    for m in openai_body.get("messages") or []:
        c = m.get("content")
        if isinstance(c, list):
            c = "".join(x.get("text", "") for x in c if isinstance(x, dict))
        c = (c or "").strip()
        if c:
            role = m.get("role", "user")
            msgs.append(c if role == "user" else f"{role}: {c}")
    prompt = "\n".join(msgs) or "你好"
    prompt, __tools_ctx = inject_tools(openai_body, prompt)
    __tools_ctx = None
    # 固定指纹(缓存在 provider 上):新指纹会收到"问候语"模板回复,须先"养"住一个身份
    if not provider.get("fingerprint"):
        provider["fingerprint"] = str(_sec.randbelow(900000000) + 100000000)
    headers = {"content-type": "application/json", "fingerprint": provider["fingerprint"],
               "x-website-domain": base, "Origin": base, "Referer": base + "/",
               "User-Agent": _DANGBEI_UA}
    if provider.get("api_key"):
        headers["Authorization"] = f"Bearer {provider['api_key']}"
    # groupId=0 会触发"问候语";且同 group 服务端保留会话历史(跨请求串扰),
    # 因此每次请求新建独立 group,保证上下文隔离
    group_id = 0
    try:
        gh = dict(headers)
        gh.pop("content-type", None)
        gc = await http_client.post(f"{base}/api/group/create", json={"title": "gw"},
                                    headers=gh, timeout=30)
        group_id = ((gc.json().get("data") or {}).get("id")) or 0
    except Exception:
        group_id = provider.get("group_id", 0)
    display_names = {"gpt-5.5": "GPT5.6（1普分）", "claude-opus-4-8": "Claude-4.8（1高分）",
                     "claude-opus-4-7": "claude-opus-4-7（1高分）"}
    payload = {"model": model, "modelName": display_names.get(model, "GPT5.6（1普分）"), "modelType": 1,
               "prompt": prompt, "imageUrl": "", "videoUrl": "", "fileUrl": "",
               "extraParam": {"size": "auto", "style": ""}, "appId": 0,
               "options": {"groupId": group_id, "fileParsing": "", "usingDeepThinking": False,
                           "usingTool": False, "useKnowledgeBase": False},
               "usingPluginId": 0, "drawId": ""}
    try:
        req = http_client.build_request("POST", f"{base}/api/chatgpt/chat-process",
                                        json=payload, headers=headers)
        resp = await http_client.send(req, stream=True)
        if resp.status_code != 200:
            detail = (await resp.aread())[:200]
            await resp.aclose()
            logger.warning("52who upstream %d: %s", resp.status_code, detail)
            record_fail(k, str(f"upstream {resp.status_code}")[:200])
            return None, f"upstream {resp.status_code}"

        async def yw_gen(rp=resp, kk=k, pn=provider["name"], ml=label, note_on_empty=False):
            chars = 0
            try:
                async for line in rp.aiter_lines():
                    if not line.startswith("{"):
                        continue
                    try:
                        ev = json.loads(line)
                    except Exception:
                        continue
                    txt = ev.get("text")
                    if isinstance(txt, str) and txt and ev.get("content"):
                        chars += len(txt)
                        yield _wxb_sse_to_openai_chunk(ml, txt)
                if chars == 0:
                    record_fail(kk, "52who 空回复(200但零正文,疑问候语模板/限流)")
                    if note_on_empty:
                        yield _wxb_sse_to_openai_chunk(ml, "⚠️ 52who 返回空回复")
                yield ("data: " + json.dumps({"id": "yw_gateway", "object": "chat.completion.chunk",
                       "model": ml, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
                       ensure_ascii=False) + "\n\n")
                yield "data: [DONE]\n\n"
                if chars:
                    record_success(kk)
            finally:
                await rp.aclose()
                try:
                    await append_usage({"ts": time.time(), "model": ml.split(" · ")[1] if " · " in ml else ml,
                                        "provider": pn, "pt": 0, "ct": chars, "tt": chars})
                except Exception:
                    pass

        if stream:
            record_success(k)
            return StreamingResponse(yw_gen(note_on_empty=True), media_type="text/event-stream"), None
        chunks = []

        async def collect():
            async for item in yw_gen():
                chunks.append(item)
            texts = []
            for item in chunks:
                try:
                    texts.append(json.loads(item[6:])["choices"][0]["delta"].get("content") or "")
                except Exception:
                    pass
            full = "".join(texts)
            if not full.strip():
                return None  # 失败已由生成器记录（含上游错误原因）
            tc, text_out = _ts_extract(full, __tools_ctx)
            msg = {"role": "assistant", "content": text_out}
            finish = "stop"
            if tc:
                msg["tool_calls"] = tc
                msg["content"] = text_out or None
                finish = "tool_calls"
            return JSONResponse(content={
                "id": "yw_gateway", "object": "chat.completion", "created": int(time.time()),
                "model": label,
                "choices": [{"index": 0, "message": msg, "finish_reason": finish}],
                "usage": {"prompt_tokens": 0, "completion_tokens": len(full), "total_tokens": len(full)},
            })
        collected = await collect()
        if collected is None:
            return None, "52who empty reply"
        return collected, None
    except httpx.RequestError as e:
        logger.warning("52who forward error: %r", e)
        record_fail(k, str(str(e))[:200])
        return None, str(e)
    except Exception as e:
        logger.exception("52who unexpected error")
        record_fail(k, str(str(e))[:200])
        return None, str(e)


# ===== 百度文心 (chat.baidu.com) 适配层 =====
# 复用 vendors 的 baidu_client.py(LLM-Red-Team 风格逆向实现,requests 同步),
# 线程池包装成异步。鉴权:providers.json api_key 存完整 Cookie 串(含 BDUSS,小号)。
# 模型:deepseek-v4-flash / deepseek-v4-pro / deepseek-r1 / ernie-5.1 / smartmode 等。

_BAIDU_CLIENTS: dict[str, "BaiduChatClient"] = {}


def _baidu_client(provider: dict):
    """按 cookie 缓存客户端实例(内部持有 token/lid 会话状态)"""
    key = provider.get("api_key", "")
    if key not in _BAIDU_CLIENTS:
        import baidu_client as _bc
        _BAIDU_CLIENTS[key] = _bc.BaiduChatClient(
            cookies=key,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")
    return _BAIDU_CLIENTS[key]


async def _forward_baidu(provider: dict, model: str, openai_body: dict, stream: bool, k: str):
    """百度文心转发。返回 (Response 或 None, last_err)。"""
    label = f"{provider['name']} · {model}"
    msgs = []
    for m in openai_body.get("messages") or []:
        c = m.get("content")
        if isinstance(c, list):
            c = "".join(x.get("text", "") for x in c if isinstance(x, dict))
        c = (c or "").strip()
        if c:
            msgs.append(f"{m.get('role', 'user').capitalize()}: {c}" if m.get("role") != "user" else c)
    query = "\n".join(msgs) or "你好"
    loop = asyncio.get_running_loop()
    try:
        client = _baidu_client(provider)

        def run():
            texts, thinks = [], []
            try:
                for ch in client.chat_to_openai_chunks(query, model=model):
                    if ch["type"] == "content":
                        texts.append(ch["content"])
                    elif ch["type"] == "reasoning_content":
                        thinks.append(ch["content"])
            except Exception as e:
                raise e
            return "".join(texts), "".join(thinks)

        text, think = await loop.run_in_executor(None, run)
        if not text.strip() and not think.strip():
            record_fail(k, str("baidu empty reply")[:200])
            return None, "baidu empty reply"
        record_success(k)
        n = len(text) + len(think)
        try:
            await append_usage({"ts": time.time(), "model": model, "provider": provider["name"],
                                "pt": 0, "ct": n, "tt": n})
        except Exception:
            pass
        if stream:
            async def bd_gen():
                if think:
                    yield _wxb_sse_to_openai_chunk(label, f"<think>{think}</think>\n\n")
                # 整段拆小块输出,保持流式观感
                step = 120
                for i in range(0, len(text), step):
                    yield _wxb_sse_to_openai_chunk(label, text[i:i + step])
                    await asyncio.sleep(0.02)
                yield ("data: " + json.dumps({"id": "bd_gateway", "object": "chat.completion.chunk",
                       "model": label, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
                       ensure_ascii=False) + "\n\n")
                yield "data: [DONE]\n\n"
            return StreamingResponse(bd_gen(), media_type="text/event-stream"), None
        msg = {"role": "assistant", "content": text}
        if think:
            msg["reasoning_content"] = think
        return JSONResponse(content={
            "id": "bd_gateway", "object": "chat.completion", "created": int(time.time()),
            "model": label,
            "choices": [{"index": 0, "message": msg, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 0, "completion_tokens": n, "total_tokens": n},
        }), None
    except Exception as e:
        logger.warning("baidu forward error: %r", e)
        record_fail(k, str(str(e))[:200])
        return None, str(e)


# ===== 有趣的人类 (all-ai.cc) Web 协议适配层 =====
# 协议要点（2026-09 逆向）：POST {base}/api/chat
# 体：{"tool":"problem_solver","message":"...","history":[...]}
# 无任何鉴权；history 客户端维护，可注入完整上下文。
# 响应：JSON {"reply":"...","provider":"cf"}（Cloudflare Workers AI，模型不强，应急用）


async def _forward_allai(provider: dict, model: str, openai_body: dict, stream: bool, k: str):
    """all-ai.cc 转发。返回 (Response 或 None, last_err)。"""
    base = provider["base_url"].rstrip("/")
    label = f"{provider['name']} · {model}"
    history = []
    for m in openai_body.get("messages") or []:
        c = m.get("content")
        if isinstance(c, list):
            c = "".join(x.get("text", "") for x in c if isinstance(x, dict))
        c = (c or "").strip()
        if c:
            history.append({"role": m.get("role", "user"), "content": c})
    if not history:
        history = [{"role": "user", "content": "你好"}]
    # 最后一条 user 作为 message，其余进 history
    message = history.pop()["content"] if history[-1]["role"] == "user" else "你好"
    payload = {"tool": "problem_solver", "message": message, "history": history}
    headers = {"Content-Type": "application/json", "Origin": base, "Referer": base + "/tools/problem-solver",
               "User-Agent": _DANGBEI_UA}
    try:
        resp = await http_client.post(f"{base}/api/chat", json=payload, headers=headers, timeout=90)
        if resp.status_code != 200:
            logger.warning("allai upstream %d: %s", resp.status_code, resp.text[:200])
            record_fail(k, str(f"upstream {resp.status_code}")[:200])
            return None, f"upstream {resp.status_code}"
        data = resp.json()
        reply = data.get("reply") or ""
        if not isinstance(reply, str):
            reply = str(reply)
        if not reply.strip():
            record_fail(k, str("allai empty reply")[:200])
            return None, "allai empty reply"
        record_success(k)
        try:
            await append_usage({"ts": time.time(), "model": model, "provider": provider["name"],
                                "pt": 0, "ct": len(reply), "tt": len(reply)})
        except Exception:
            pass
        if stream:
            import asyncio as _aio

            async def aa_gen():
                yield _wxb_sse_to_openai_chunk(label, reply)
                yield ("data: " + json.dumps({"id": "aa_gateway", "object": "chat.completion.chunk",
                       "model": label, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
                       ensure_ascii=False) + "\n\n")
                yield "data: [DONE]\n\n"
            return StreamingResponse(aa_gen(), media_type="text/event-stream"), None
        return JSONResponse(content={
            "id": "aa_gateway", "object": "chat.completion", "created": int(time.time()),
            "model": label,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": reply},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 0, "completion_tokens": len(reply), "total_tokens": len(reply)},
        }), None
    except httpx.RequestError as e:
        logger.warning("allai forward error: %r", e)
        record_fail(k, str(str(e))[:200])
        return None, str(e)
    except Exception as e:
        logger.exception("allai unexpected error")
        record_fail(k, str(str(e))[:200])
        return None, str(e)


# ===== 秘塔 (metaso) Web 协议适配层 =====
# 协议要点（2026-09 逆向）：POST {base}/api/search/chat
# 头：metaso-pc: pc + token（匿名 token，存 providers.json 的 api_key）
# 体：OpenAI 风格 messages（多 temp-uuid 字段）+ mode/model/outputLanguage 等
# SSE：data:{choices:[{delta:{content/reasoning_content}}]}，夹杂 conversation_init/heartbeat 等


_metaso_token_cache = {"token": "", "ts": 0}


async def _metaso_fresh_token() -> str:
    """从秘塔首页 <meta id="meta-token"> 自动获取匿名 token（每次页面访问都会发放）"""
    import re as _re
    now = time.time()
    if _metaso_token_cache["token"] and now - _metaso_token_cache["ts"] < 600:
        return _metaso_token_cache["token"]
    try:
        r = await http_client.get("https://metaso.cn/", headers={
            "User-Agent": _DANGBEI_UA,
            "Accept": "text/html,application/xhtml+xml",
            "Sec-Fetch-Mode": "navigate", "Upgrade-Insecure-Requests": "1"}, timeout=20)
        m = re.search(r'<meta id="meta-token" content="([^"]*)"', r.text)
        if m:
            _metaso_token_cache.update(token=m.group(1), ts=now)
            return m.group(1)
    except Exception as e:
        logger.warning("metaso token fetch failed: %r", e)
    return _metaso_token_cache["token"]


async def _forward_metaso(provider: dict, model: str, openai_body: dict, stream: bool, k: str):
    """秘塔 Web 协议转发。返回 (Response 或 None, last_err)。"""
    token = provider.get("api_key", "") or await _metaso_fresh_token()
    base = provider["base_url"].rstrip("/")
    label = f"{provider['name']} · {model}"
    cid = f"temp-{uuid.uuid4()}"

    def build_msg(role: str, content: str) -> dict:
        return {"id": f"temp-{uuid.uuid4()}", "key": f"temp-{uuid.uuid4()}", "conversationId": cid,
                "role": role, "content": content, "markdownContent": content, "engineType": "",
                "filter": "all", "contentType": 0, "outputHtml": False, "mode": "detail",
                "model": model, "outputStyle": "正常"}

    msgs = []
    sys_texts, ua_texts = [], []
    for m in openai_body.get("messages") or []:
        c = m.get("content")
        if isinstance(c, list):
            c = "".join(x.get("text", "") for x in c if isinstance(x, dict))
        c = (c or "").strip()
        if not c:
            continue
        role = m.get("role", "user")
        if role == "system":
            sys_texts.append(c)
        else:
            ua_texts.append(f"{role}: {c}" if role == "assistant" else c)
    text = "\n".join(sys_texts + ua_texts)
    msgs = [build_msg("user", text or "你好")]

    payload = {"model": model, "stream": True, "messages": msgs, "engineType": "", "mode": "detail",
               "filter": "all", "outputHtml": False, "outputStyle": "正常", "darkMode": False,
               "outputLanguage": "中文", "htmlNoDisplayEnable": True, "metaso-pc": "pc", "token": token}
    headers = {"Content-Type": "application/json", "metaso-pc": "pc", "token": token,
               "accept": "text/event-stream", "Origin": base, "Referer": base + "/"}
    try:
        req = http_client.build_request("POST", f"{base}/api/search/chat", json=payload, headers=headers)
        resp = await http_client.send(req, stream=True)
        if resp.status_code != 200:
            detail = (await resp.aread())[:200]
            await resp.aclose()
            logger.warning("metaso upstream %d: %s", resp.status_code, detail)
            record_fail(k, str(f"upstream {resp.status_code}")[:200])
            return None, f"upstream {resp.status_code}"
        # 秒回 200 但流内 429 属于限流:不在这里重试,由熔断跳过、客户端稍后再试

        async def ms_gen(rp=resp, kk=k, pn=provider["name"], ml=label):
            chars = 0
            try:
                async for line in rp.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    ds = line[5:]
                    try:
                        ev = json.loads(ds)
                    except Exception:
                        continue
                    if not isinstance(ev.get("choices"), list):
                        continue
                    delta = (ev["choices"][0] or {}).get("delta") or {}
                    txt = delta.get("content")
                    if isinstance(txt, str) and txt:
                        chars += len(txt)
                        yield _wxb_sse_to_openai_chunk(ml, txt)
                yield ("data: " + json.dumps({"id": "ms_gateway", "object": "chat.completion.chunk",
                       "model": ml, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
                       ensure_ascii=False) + "\n\n")
                yield "data: [DONE]\n\n"
                record_success(kk)
            finally:
                await rp.aclose()
                try:
                    await append_usage({"ts": time.time(), "model": ml.split(" · ")[1] if " · " in ml else ml,
                                        "provider": pn, "pt": 0, "ct": chars, "tt": chars})
                except Exception:
                    pass

        if stream:
            record_success(k)
            return StreamingResponse(ms_gen(), media_type="text/event-stream"), None
        chunks = []

        async def collect():
            async for item in ms_gen():
                chunks.append(item)
            texts = []
            for item in chunks:
                try:
                    texts.append(json.loads(item[6:])["choices"][0]["delta"].get("content") or "")
                except Exception:
                    pass
            full = "".join(texts)
            if not full.strip():
                # 流内 429 限流或空响应:视为失败,让网关 fallback 到下一候选
                record_fail(k, "empty reply")
                return None
            return JSONResponse(content={
                "id": "ms_gateway", "object": "chat.completion", "created": int(time.time()),
                "model": label,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": full},
                             "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 0, "completion_tokens": len(full), "total_tokens": len(full)},
            })
        collected = await collect()
        if collected is None:
            return None, "metaso empty (rate-limited?)"
        return collected, None
    except httpx.RequestError as e:
        logger.warning("metaso forward error: %r", e)
        record_fail(k, str(str(e))[:200])
        return None, str(e)
    except Exception as e:
        logger.exception("metaso unexpected error")
        record_fail(k, str(str(e))[:200])
        return None, str(e)


# ===== 当贝AI (dangbei) Web 协议适配层 =====
# 协议要点（2026-09 逆向）：POST {base}/ai-search/chatApi/v2/chat
# 头：apptype/appversion/client-ver/lang + deviceid/nonce/sign/timestamp/token
# sign = MD5(timestamp + JSON(payload, 无空格) + nonce).upper()
# 前置：conversationApi/v1/create 建会话；commonApi/v1/generateId 取消息ID（POST）
# SSE：event:conversation.message.delta / data:{content_type: text|thinking|card, content}

_DANGBEI_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
_DANGBEI_MODEL_ACTIONS = {
    "deepseek-v3": "", "deepseek-v3-search": "online",
    "deepseek-r1": "deep", "deepseek-r1-search": "deep,online",
}


def _db_nanoid(size: int = 21) -> str:
    import secrets as _secrets
    alphabet = "abcdefgh0ijkl1mno2pqrs3tuv4wxyz5ABCDEFGH6IJKL7MNO8PQRS9TUV-WXYZ_"
    return "".join(_secrets.choice(alphabet) for _ in range(size))


def _db_sign(timestamp: str, payload: dict, nonce: str) -> str:
    import hashlib as _hl
    s = timestamp + json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + nonce
    return _hl.md5(s.encode("utf-8")).hexdigest().upper()


def _db_headers(provider: dict, payload: dict, device_id: str) -> dict:
    ts = str(int(time.time()))
    nonce = _db_nanoid(21)
    base = provider["base_url"].rstrip("/")
    origin = base if "ai-api" not in base else "https://ai.dangbei.com"
    return {
        "Origin": "https://ai.dangbei.com", "Referer": "https://ai.dangbei.com/",
        "User-Agent": _DANGBEI_UA,
        "apptype": "6", "appversion": "1.3.9", "client-ver": "1.0.2", "lang": "zh",
        "deviceid": device_id, "nonce": nonce,
        "sign": _db_sign(ts, payload, nonce), "timestamp": ts,
        "token": provider.get("api_key", ""),
        "content-type": "application/json",
    }


async def _forward_dangbei(provider: dict, model: str, openai_body: dict, stream: bool, k: str):
    """当贝 Web 协议转发（OpenAI 请求 -> 签名调用 -> OpenAI 响应/SSE）。
    返回 (Response 或 None, last_err)。"""
    base = provider["base_url"].rstrip("/") + "/ai-search"
    device_id = uuid.uuid4().hex[:32]
    label = f"{provider['name']} · {model}"
    try:
        # 1. 建会话（每次新会话，历史拼进 question，无状态）
        p_create = {"botCode": "AI_SEARCH"}
        r = await http_client.post(f"{base}/conversationApi/v1/create", json=p_create,
                                   headers=_db_headers(provider, p_create, device_id), timeout=30)
        cid = (r.json().get("data") or {}).get("conversationId")
        if r.status_code != 200 or not cid:
            logger.warning("dangbei create failed %d from %s: %s", r.status_code, provider["name"], r.text[:200])
            record_fail(k, str(f"create {r.status_code}")[:200])
            return None, f"create {r.status_code}"
        # 2. 消息ID
        r = await http_client.post(f"{base}/commonApi/v1/generateId", json={},
                                   headers=_db_headers(provider, {}, device_id), timeout=30)
        gid = str((r.json() or {}).get("data") or "")
        if not gid:
            gid = cid
        # 3. 拼历史
        msgs = []
        for m in openai_body.get("messages") or []:
            c = m.get("content")
            if isinstance(c, list):
                c = "".join(x.get("text", "") for x in c if isinstance(x, dict))
            c = (c or "").strip()
            if c:
                msgs.append(f"{str(m.get('role', 'user')).capitalize()}: {c}")
        question = "\n".join(msgs) or "你好"
        question, __tools_ctx = inject_tools(openai_body, question)
        p_chat = {
            "stream": True, "botCode": "AI_SEARCH", "conversationId": cid,
            "question": question, "model": "deepseek-v3",  # r1 也用 v3 标识,思考模式由 userAction=deep 控制
            "chatOption": {"searchKnowledge": False, "searchAllKnowledge": False, "searchSharedKnowledge": False},
            "knowledgeList": [], "anonymousKey": "", "uuid": gid, "chatId": gid,
            "files": [], "reference": [], "role": "user", "status": "local",
            "content": question[-2000:], "userAction": _DANGBEI_MODEL_ACTIONS.get(model, ""),
            "agentId": "",
        }
        req = http_client.build_request("POST", f"{base}/chatApi/v2/chat", json=p_chat,
                                        headers=_db_headers(provider, p_chat, device_id))
        resp = await http_client.send(req, stream=True)
        if resp.status_code != 200:
            detail = (await resp.aread())[:200]
            await resp.aclose()
            logger.warning("dangbei upstream %d: %s", resp.status_code, detail)
            record_fail(k, str(f"upstream {resp.status_code}")[:200])
            return None, f"upstream {resp.status_code}"

        async def db_gen(rp=resp, kk=k, pn=provider["name"], ml=label):
            chars = 0
            try:
                async for line in rp.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    ds = line[5:]
                    try:
                        ev = json.loads(ds)
                    except Exception:
                        continue
                    c = ev.get("content")
                    if isinstance(c, str) and c and ev.get("content_type") == "text":
                        chars += len(c)
                        yield _wxb_sse_to_openai_chunk(ml, c)
                yield ("data: " + json.dumps({"id": "db_gateway", "object": "chat.completion.chunk",
                       "model": ml, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
                       ensure_ascii=False) + "\n\n")
                yield "data: [DONE]\n\n"
                record_success(kk)
            finally:
                await rp.aclose()
                try:
                    await append_usage({"ts": time.time(), "model": ml.split(" · ")[1] if " · " in ml else ml,
                                        "provider": pn, "pt": 0, "ct": chars, "tt": chars})
                except Exception:
                    pass

        if stream:
            record_success(k)
            return StreamingResponse(db_gen(), media_type="text/event-stream"), None
        # 非流式：收集完整正文
        chunks = []

        async def collect():
            async for item in db_gen():
                chunks.append(item)
            texts = []
            for item in chunks:
                try:
                    texts.append(json.loads(item[6:])["choices"][0]["delta"].get("content") or "")
                except Exception:
                    pass
            full = "".join(texts)
            tc, text_out = _ts_extract(full, __tools_ctx)
            msg = {"role": "assistant", "content": text_out}
            finish = "stop"
            if tc:
                msg["tool_calls"] = tc
                msg["content"] = text_out or None
                finish = "tool_calls"
            return JSONResponse(content={
                "id": "db_gateway", "object": "chat.completion", "created": int(time.time()),
                "model": label,
                "choices": [{"index": 0, "message": msg, "finish_reason": finish}],
                "usage": {"prompt_tokens": 0, "completion_tokens": len(full), "total_tokens": len(full)},
            })
        return await collect(), None
    except httpx.RequestError as e:
        logger.warning("dangbei forward error: %r", e)
        record_fail(k, str(e)[:200])
        return None, str(e)
    except Exception as e:
        logger.exception("dangbei unexpected error")
        record_fail(k, str(str(e))[:200])
        return None, str(e)


def _wxb_build_body(provider: dict, model: str, openai_body: dict) -> str:
    """OpenAI messages -> 问小白 chat/v3 请求体（无状态：历史拼进 query）。
    若请求带 tools，注入 TOOL_CALL 文本协议。"""
    # 工具定义 -> 文本协议说明
    tool_prompt = ""
    tools = openai_body.get("tools") or []
    if tools:
        defs = []
        for t in tools:
            fn = t.get("function") or t
            params = fn.get("parameters") or {}
            props = params.get("properties") or {}
            args = {k: v.get("type", "string") for k, v in props.items()}
            defs.append({"name": fn.get("name", ""), "description": fn.get("description", ""), "args": args})
        tool_prompt = (
            "【可用工具】" + json.dumps(defs, ensure_ascii=False) + "\n"
            "当且仅当需要工具时，在回复中输出一行：\n"
            'TOOL_CALL: {"name":"工具名","arguments":{参数对象}}\n'
            "系统会执行该工具并把结果以【工具返回】标签回传给你，届时继续任务。\n"
            "不需要工具时正常回答，禁止在正常回答中使用 TOOL_CALL 字样。\n\n"
        )

    parts = []
    for m in openai_body.get("messages", []):
        role = m.get("role", "user")
        c = m.get("content")
        if isinstance(c, list):
            c = "".join(x.get("text", "") for x in c if isinstance(x, dict))
        # assistant 的历史工具调用
        if role == "assistant" and m.get("tool_calls"):
            tc_texts = []
            for tc in m["tool_calls"]:
                fn = tc.get("function") or {}
                tc_texts.append(f'TOOL_CALL: {{"name":"{fn.get("name","")}","arguments":{fn.get("arguments","{}")}}}')
            parts.append(f"【上一步助手动作】{c or ''} {' '.join(tc_texts)}".strip())
            continue
        # 工具结果回传
        if role == "tool":
            parts.append(f"【工具返回({m.get('tool_call_id','')})】{c or ''}")
            continue
        if not c:
            continue
        tag = {"system": "系统要求", "assistant": "之前的助手回复", "user": "用户"}.get(role, role)
        parts.append(f"【{tag}】{c}")
    if not tools:
        parts.append("【本次指令】只处理最后一条用户消息，不要重复历史内容。直接输出结果。")
    query = tool_prompt + "\n".join(parts)
    body = {
        "userId": provider.get("user_id"),
        "botAlias": "custom",
        "query": query[-30000:],
        "isRetry": False,
        "breakingStrategy": 0,
        "isNewConversation": True,
        "isAnonymousChat": False,
        "mediaInfos": [],
        "turnIndex": 0,
        "rewriteQuery": "",
        "conversationId": "",
        "attachmentInfo": {"url": {"infoList": []}},
        "inputWay": "proactive",
        "agentId": "200006",
        "modelId": MODEL_ALIASES.get(model, model),
        "aiAbility": {"id": "", "extraInfo": ""},
        "abilities": [],
        "pureQuery": "",
        "isPublic": 0,
    }
    return json.dumps(body, ensure_ascii=False, separators=(",", ":"))



_TOOL_CALL_RE = re.compile(r'TOOL_CALL\s*[:：]\s*(\{.*?\})\s*$', re.S)


def _wxb_parse_tool_call(text: str):
    """从回复文本中解析 TOOL_CALL 标记。返回 (tool_calls, cleaned_text) 或 None"""
    for line in text.splitlines():
        m = _TOOL_CALL_RE.search(line.strip())
        if not m:
            continue
        try:
            obj = json.loads(m.group(1))
        except Exception:
            continue
        name = obj.get("name")
        if not name:
            continue
        return [{
            "id": "wxbcall_" + secrets.token_hex(8),
            "type": "function",
            "function": {"name": name,
                         "arguments": json.dumps(obj.get("arguments") or {}, ensure_ascii=False)},
        }], None
    return None


def _wxb_sse_to_openai_chunk(model_label: str, content: str) -> str:
    return "data: " + json.dumps({
        "id": "wxb_gateway", "object": "chat.completion.chunk", "model": model_label,
        "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": None}],
    }, ensure_ascii=False) + "\n\n"


async def _forward_wxb(provider: dict, model: str, openai_body: dict, stream: bool, k: str):
    """问小白 Web 协议转发（OpenAI 请求 -> 签名调用 -> OpenAI 响应/SSE）。
    返回 (Response 或 None, last_err)；None 表示失败应 fallback 下一候选。"""
    url = provider["base_url"].rstrip("/") + "/api/v1.0/core/conversation/chat/v3"
    body_str = _wxb_build_body(provider, model, openai_body)
    headers = _wxb_sign_and_headers(provider, body_str)
    label = f"{provider['name']} · {model}"
    try:
        req = http_client.build_request("POST", url, content=body_str.encode("utf-8"),
                                        headers=headers)
        resp = await http_client.send(req, stream=True)
        if resp.status_code != 200:
            detail = (await resp.aread())[:200]
            await resp.aclose()
            logger.warning("wxb upstream %d: %s", resp.status_code, detail)
            record_fail(k, f"upstream {resp.status_code} {detail!r}"[:200])
            return None, f"upstream {resp.status_code}"
        record_success(k)

        async def wxb_gen(rp=resp, kk=k, pn=provider["name"], ml=label, note_on_empty=stream):
            chars = 0
            paywall = False
            try:
                async for line in rp.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    ds = line[5:].strip()
                    if not ds or ds == "[DONE]":
                        continue
                    try:
                        ev = json.loads(ds)
                    except Exception:
                        continue
                    if "suanli" in ev:
                        paywall = True  # 蒜粒扣费事件：免费额度耗尽的信号
                    c = ev.get("content")
                    if isinstance(c, str) and c:
                        # 过滤网页端状态文本（如“全网收集资料中”），避免污染正文
                        if c.strip() in _WXB_ARTIFACTS:
                            continue
                        chars += len(c)
                        yield _wxb_sse_to_openai_chunk(ml, c)
                if chars == 0:
                    # 200 但零正文 = 上游静默失败（蒜粒付费墙/被软限流），记失败而非假成功
                    record_fail(kk, "wxb 蒜粒付费墙(免费额度耗尽)" if paywall else "wxb 空回复(200但零正文)")
                    if note_on_empty:
                        warn = "⚠️ wenxiaobai " + ("免费额度耗尽（蒜粒付费墙）" if paywall else "返回空回复")
                        yield _wxb_sse_to_openai_chunk(ml, warn)
                yield ("data: " + json.dumps({"id": "wxb_gateway", "object": "chat.completion.chunk",
                       "model": ml, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
                       ensure_ascii=False) + "\n\n")
                yield "data: [DONE]\n\n"
                record_success(kk)
            finally:
                await rp.aclose()
                try:
                    await append_usage({"ts": time.time(), "model": ml.split(" · ")[1] if " · " in ml else ml,
                                        "provider": pn, "pt": 0, "ct": chars, "tt": chars})
                except Exception:
                    pass

        if stream and not openai_body.get("tools"):
            return StreamingResponse(wxb_gen(), media_type="text/event-stream"), None
        # 非流式 / 带工具：收集完整回复，检测 TOOL_CALL 标记
        chunks = []
        collect_err = []

        async def collect():
            async for item in wxb_gen():
                chunks.append(item)
            texts = []
            for item in chunks:
                try:
                    texts.append(json.loads(item[6:])["choices"][0]["delta"].get("content") or "")
                except Exception:
                    pass
            full = "".join(texts)
            if not full.strip():
                # 空回复已在 wxb_gen 记过失败；返回 None 让调用方回落下一候选
                collect_err.append("空回复(蒜粒付费墙/软限流)")
                return None
            parsed_tc = _wxb_parse_tool_call(full)
            if parsed_tc:
                tc = parsed_tc[0]
                return JSONResponse(content={
                    "id": "wxb_gateway", "object": "chat.completion", "created": int(time.time()),
                    "model": label,
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": None,
                                 "tool_calls": tc}, "finish_reason": "tool_calls"}],
                    "usage": {"prompt_tokens": 0, "completion_tokens": len(full), "total_tokens": len(full)},
                })
            return JSONResponse(content={
                "id": "wxb_gateway", "object": "chat.completion", "created": int(time.time()),
                "model": label,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": full},
                             "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 0, "completion_tokens": len(full), "total_tokens": len(full)},
            })
        return await collect(), (collect_err[0] if collect_err else None)
    except httpx.RequestError as e:
        logger.warning("wxb forward error: %r", e)
        record_fail(k, str(e)[:200])
        return None, str(e)
    except Exception as e:
        logger.exception("wxb unexpected error")
        record_fail(k, str(e)[:200])
        return None, str(e)


# ===== 腾讯 ima 官方 OpenAPI 工具代理 (/v1/ima/*) =====
# 凭证存 config.json: ima_client_id / ima_api_key（agent-interface 页申请，首次需 ima 客户端）
# 端点: POST /v1/ima/note/{list_notebook|list_note|search_note|import_doc|append_doc|get_doc_content}
#       POST /v1/ima/kb/{search_knowledge|search_knowledge_base|add_knowledge|get_knowledge_list|...}
# 透传 POST JSON 到 https://ima.qq.com/openapi/note/v1/* 或 /openapi/wiki/v1/*

IMA_OPENAPI_BASE = "https://ima.qq.com/openapi"


async def _ima_forward(kind: str, action: str, body: dict):
    cid = str(app_config.get("ima_client_id") or "")
    key = str(app_config.get("ima_api_key") or "")
    if not cid or not key:
        return JSONResponse(status_code=503, content={"detail": "ima 未配置：config.json 缺 ima_client_id / ima_api_key"})
    if kind == "note":
        path = f"note/v1/{action}"
    elif kind == "kb":
        path = f"wiki/v1/{action}"
    else:
        return JSONResponse(status_code=404, content={"detail": f"未知 ima 类别 {kind}，用 note 或 kb"})
    if not action or not all(c.isalnum() or c == "_" for c in action):
        return JSONResponse(status_code=400, content={"detail": "非法 ima 接口名"})
    try:
        r = await http_client.post(
            f"{IMA_OPENAPI_BASE}/{path}",
            json=body,
            headers={"Content-Type": "application/json",
                     "ima-openapi-clientid": cid, "ima-openapi-apikey": key},
            timeout=60,
        )
        return JSONResponse(status_code=r.status_code, content=r.json() if r.text else {})
    except Exception as e:
        logger.warning("ima forward error: %r", e)
        return JSONResponse(status_code=502, content={"detail": f"ima upstream error: {e!r}"})


@app.post("/v1/ima/note/{action}", dependencies=[Depends(verify_client)])
async def ima_note(action: str, request: Request):
    return await _ima_forward("note", action, await request.json())


@app.post("/v1/ima/kb/{action}", dependencies=[Depends(verify_client)])
async def ima_kb(action: str, request: Request):
    return await _ima_forward("kb", action, await request.json())


# ===== WPS 灵犀 (lingxi.kdocs.cn) 适配层 =====
# 协议(2026-09-20 逆向): ①POST /api/aioffice/v1/cowork/sessions {"session_type":"cowork_web"} 建会话(Cookie鉴权)
# ②WSS /api/aioffice/v1/cowork/sessions/{sid}/sandbox/completions/ws
# ③发 {"event":"user.input","data":{"agent_input":{"text",...}}} ④收事件流: component.content(c_text.delta 增量)→run.end
# 鉴权: kdocs Cookie(kso_sid/wps_sid/csrf), 存 providers.json 灵犀条目 api_key; csrf 存 extra 字段 lingxi_csrf
# 计费: 1 智点/次

_LINGXI_BASE = "https://lingxi.kdocs.cn"


async def _forward_lingxi(provider: dict, model: str, openai_body: dict, stream: bool, k: str):
    """灵犀转发。返回 (Response 或 None, last_err)。"""
    import websockets
    label = f"{provider['name']} · {model}"
    cookie = provider.get("api_key") or ""
    csrf = provider.get("lingxi_csrf") or ""
    headers = {"cookie": cookie, "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/134.0.0.0",
               "origin": _LINGXI_BASE, "referer": _LINGXI_BASE + "/",
               "x-requested-with": "XMLHttpRequest"}
    msgs = openai_body.get("messages") or []
    parts = []
    for m in msgs:
        c = m.get("content")
        if isinstance(c, list):
            c = "".join(x.get("text", "") for x in c if isinstance(x, dict))
        c = (c or "").strip()
        if c and m.get("role") != "system":
            parts.append(c)
            question, __tools_ctx = inject_tools(openai_body, question)
    question = "\n".join(parts) or "你好"
    try:
        async with httpx.AsyncClient(headers=headers, timeout=30) as hc:
            r = await hc.post(f"{_LINGXI_BASE}/api/aioffice/v1/cowork/sessions",
                              json={"session_type": "cowork_web"}, headers={"x-csrf-token": csrf})
        if r.status_code != 200:
            record_fail(k, str(f"lingxi session {r.status_code}")[:200])
            return None, f"lingxi session {r.status_code}"
        sid = (r.json().get("data") or {}).get("session_id")
        if not sid:
            record_fail(k, str("lingxi no session_id (cookie 过期?)")[:200])
            return None, "lingxi no session_id (cookie 过期?)"

        ws_url = f"wss://lingxi.kdocs.cn/api/aioffice/v1/cowork/sessions/{sid}/sandbox/completions/ws"
        async with websockets.connect(
                ws_url, extra_headers={"cookie": cookie, "origin": _LINGXI_BASE}) as ws:
            payload = {"event": "user.input", "data": {"agent_input": {
                "text": question, "files": [], "refs": [],
                "expect_model": "plan_pro", "current_mode": "agent",
                "permission_mode": "default", "browser_plugin_enabled": False,
                "file_ids": [], "upload_ids": []},
                "headers": {"client_type": "pc_web", "open_position": "copilotchat_web"}}}
            await ws.send(json.dumps(payload, ensure_ascii=False))
            full = []
            try:
                async for msg in ws:
                    if isinstance(msg, bytes):
                        msg = msg.decode("utf-8", errors="replace")
                    for line in msg.split("\n"):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            ev = json.loads(line)
                        except Exception:
                            continue
                        e, d = ev.get("event"), ev.get("data") or {}
                        if e == "component.content" and d.get("type") == "c_text":
                            full.append(d.get("delta", ""))
                        elif e == "run.end":
                            raise StopAsyncIteration
                        elif e == "error":
                            raise RuntimeError(f"lingxi error: {json.dumps(d, ensure_ascii=False)[:150]}")
            except (StopAsyncIteration, websockets.exceptions.ConnectionClosed):
                pass
        full_text = "".join(full).strip()
        if not full_text:
            record_fail(k, str("lingxi empty reply")[:200])
            return None, "lingxi empty reply"
        record_success(k)
        try:
            await append_usage({"ts": time.time(), "model": model, "provider": provider["name"],
                                "pt": 0, "ct": len(full_text), "tt": len(full_text)})
        except Exception:
            pass
        if stream:
            import asyncio as _aio

            async def lx_gen():
                step = max(20, len(full_text) // 60)
                for i in range(0, len(full_text), step):
                    yield _wxb_sse_to_openai_chunk(label, full_text[i:i + step])
                    await _aio.sleep(0.02)
                yield ("data: " + json.dumps({"id": "lingxi_gateway", "object": "chat.completion.chunk",
                       "model": label, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
                       ensure_ascii=False) + "\n\n")
                yield "data: [DONE]\n\n"
            return StreamingResponse(lx_gen(), media_type="text/event-stream"), None
        tc_l, text_l = _ts_extract(full_text, __tools_ctx)
        msg_l = {"role": "assistant", "content": text_l}
        finish_l = "stop"
        if tc_l:
            msg_l["tool_calls"] = tc_l
            msg_l["content"] = text_l or None
            finish_l = "tool_calls"
        return JSONResponse(content={
            "id": "lingxi_gateway", "object": "chat.completion", "created": int(time.time()),
            "model": label,
            "choices": [{"index": 0, "message": msg_l, "finish_reason": finish_l}],
            "usage": {"prompt_tokens": 0, "completion_tokens": len(full_text), "total_tokens": len(full_text)},
        }), None
    except Exception as e:
        logger.warning("lingxi forward error: %r", e)
        record_fail(k, str(str(e))[:200])
        return None, str(e)


# ===== 腾讯 ima 网页版 (ima.qq.com) 适配层 =====


# ===== 腾讯 ima 网页版 (ima.qq.com) 适配层 =====
# 协议(2026-09-20 逆向)：POST {base}/cgi-bin/assistant/qa，SSE 响应
# 头：x-ima-bkn(静态，存 provider.ima_bkn) + x-ima-cookie(存 api_key，需随请求带 Cookie 头)
# 体：session_id/client_id/device_info(uskey) 必须来自浏览器创建过的会话模板——
#     uskey 设备指纹与会话绑定，凭空伪造的 session 返回 1402/1000。
#     模板存 provider.templates[模型名]，把 question 换掉即可无限复用。
# SSE：MESSAGE 帧 {"Text":"增量"}；SEARCH_MEDIAS(联网搜索提示)/HEARTBEAT/SUGGEST_QUESTION 忽略；
#      COMPLETED {"Code":0} 成功，非 0 视为失败。
# 已见错误码: 0=成功; 3=账号临时风控("提问太快",≥8s间隔低频使用,数小时后自行解封);
# 1402=会话失效(session_id/client_id/uskey 不匹配,须用浏览器实抓模板); 1000=上游内部错误; 200002=skill 鉴权失败(OpenAPI 凭证错)
_IMA_MODEL_MAP = {
    "ima-deepseek-v4-flash": {"model_id": "official_3", "model_type": 3},
    "ima-hy4-preview": {"model_id": "official_1001", "model_type": 1001},
    "ima-hy3": {"model_id": "official_2", "model_type": 2},
    "ima-glm-5.3-flash": {"model_id": "official_3000", "model_type": 3000},
}


async def _forward_ima(provider: dict, model: str, openai_body: dict, stream: bool, k: str):
    """ima 网页版转发。返回 (Response 或 None, last_err)。"""
    label = f"{provider['name']} · {model}"
    tpls = provider.get("templates") or {}
    tpl = tpls.get(model)
    if not tpl:
        return None, f"ima 无会话模板: {model}"
    msgs = openai_body.get("messages") or []
    parts = []
    for m in msgs:
        c = m.get("content")
        if isinstance(c, list):
            c = "".join(x.get("text", "") for x in c if isinstance(x, dict))
        c = (c or "").strip()
        if c and m.get("role") != "system":
            parts.append(c)
            question, __tools_ctx = inject_tools(openai_body, question)
    question = "\n".join(parts) or "你好"
    cookie = provider.get("api_key") or ""
    bkn = provider.get("ima_bkn") or ""
    headers = {
        "Content-Type": "application/json", "accept": "text/event-stream",
        "from_browser_ima": "1", "x-ima-bkn": bkn, "x-ima-cookie": cookie,
        "Cookie": cookie, "Origin": "https://ima.qq.com", "Referer": "https://ima.qq.com/",
        "User-Agent": _DANGBEI_UA,
    }
    body = dict(tpl)
    body["question"] = question
    mi = dict(tpl.get("model_info") or {})
    if model in _IMA_MODEL_MAP:
        mi.update(_IMA_MODEL_MAP[model])
    body["model_info"] = mi
    try:
        resp = await http_client.post(f"{provider['base_url']}/cgi-bin/assistant/qa",
                                      json=body, headers=headers, timeout=180)
        if resp.status_code != 200:
            record_fail(k, str(f"ima upstream {resp.status_code}")[:200])
            return None, f"ima upstream {resp.status_code}"
        # 解析 SSE
        text_out, err_code = [], 0
        for frame in resp.text.replace("\r\n", "\n").split("\n\n"):
            if not frame.startswith("event:"):
                continue
            ev, _, data = frame.partition("\n")
            ev = ev.replace("event: ", "").strip()
            if ev == "MESSAGE" and data.startswith("data: "):
                try:
                    payload = data[6:].split(chr(10), 1)[0]  # data行后还跟id:行,只取首行
                    text_out.append(json.loads(payload).get("Text", ""))
                except Exception:
                    pass
            elif ev == "COMPLETED" and data.startswith("data: "):
                try:
                    err_code = json.loads(data[6:]).get("Code", 0) or 0
                except Exception:
                    pass
        full = "".join(text_out).strip()
        if err_code != 0 or not full:
            record_fail(k, str(f"ima code={err_code} empty={not full}")[:200])
            return None, f"ima code={err_code} empty={not full}"
        record_success(k)
        try:
            await append_usage({"ts": time.time(), "model": model, "provider": provider["name"],
                                "pt": 0, "ct": len(full), "tt": len(full)})
        except Exception:
            pass
        if stream:
            import asyncio as _aio

            async def ima_gen():
                step = max(20, len(full) // 60)
                for i in range(0, len(full), step):
                    yield _wxb_sse_to_openai_chunk(label, full[i:i + step])
                    await _aio.sleep(0.02)
                yield ("data: " + json.dumps({"id": "ima_gateway", "object": "chat.completion.chunk",
                       "model": label, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
                       ensure_ascii=False) + "\n\n")
                yield "data: [DONE]\n\n"
            return StreamingResponse(ima_gen(), media_type="text/event-stream"), None
        tc_i, text_i = _ts_extract(full, __tools_ctx)
        msg_i = {"role": "assistant", "content": text_i}
        finish_i = "stop"
        if tc_i:
            msg_i["tool_calls"] = tc_i
            msg_i["content"] = text_i or None
            finish_i = "tool_calls"
        return JSONResponse(content={
            "id": "ima_gateway", "object": "chat.completion", "created": int(time.time()),
            "model": label,
            "choices": [{"index": 0, "message": msg_i, "finish_reason": finish_i}],
            "usage": {"prompt_tokens": 0, "completion_tokens": len(full), "total_tokens": len(full)},
        }), None
    except httpx.RequestError as e:
        logger.warning("ima forward error: %r", e)
        record_fail(k, str(str(e))[:200])
        return None, str(e)
    except Exception as e:
        logger.exception("ima unexpected error")
        record_fail(k, str(str(e))[:200])
        return None, str(e)


@app.api_route("/v1/chat/completions", methods=["POST"], dependencies=[Depends(verify_client)])
async def proxy_chat(request: Request, force: bool = False):
    _req_t0.set(time.time())
    body = await request.json()
    body = compress_hermes(body)
    requested_model = body.get("model")
    # MiniMax M3 会把注入的语言指令复读进正文,该渠道跳过
    if not str(requested_model or "").startswith("minimax"):
        body = ensure_lang_reply(body)

    candidates = pick_available_models(requested_model, force=force)
    candidates = filter_by_vision(candidates, body)
    candidates = filter_by_context(candidates, body)
    if not candidates:
        raise HTTPException(503, f"无可用的模型: {requested_model or '任意'}")

    stream = body.get("stream", False)
    last_err = None

    for provider, model in candidates:
        k = f"{provider['name']}||{model}"
        _inflight_mark(provider['name'], model)
        # ===== Anthropic 协议上游：OpenAI 请求转换后转发，响应转回 OpenAI 格式 =====
        if provider.get("protocol") == "anthropic":
            url = provider["base_url"].rstrip("/") + "/messages"
            headers = {"x-api-key": provider["api_key"], "anthropic-version": "2023-06-01",
                       "Content-Type": "application/json"}
            ab = _openai_to_anth_request(body)
            ab["model"] = MODEL_ALIASES.get(model, model)
            ab["stream"] = stream
            # 响应 model 字段显示实际渠道模型（与其他适配层一致）；usage 记录实际模型，
            # 请求的组名单独存 req 字段——组名绝不冒充模型名
            label = f"{provider['name']} · {model}"
            try:
                if stream:
                    req = http_client.build_request("POST", url, json=ab, headers=headers)
                    resp = await http_client.send(req, stream=True)
                    if resp.status_code >= 400:
                        detail = (await resp.aread())[:200]
                        await resp.aclose()
                        logger.warning("anth->openai stream upstream %d from %s: %s",
                                       resp.status_code, provider["name"], detail)
                        last_err = f"upstream {resp.status_code}"
                        record_fail(k, str(last_err)[:200])
                        continue
                    record_success(k)
                    return StreamingResponse(_openai_stream_from_anth(resp, label, k, provider["name"],
                                                                     usage_model=model, req_model=requested_model),
                                             media_type="text/event-stream")
                resp = await http_client.post(url, json=ab, headers=headers, timeout=180)
                if resp.status_code >= 400:
                    logger.warning("anth->openai upstream %d from %s: %s",
                                   resp.status_code, provider["name"], resp.text[:200])
                    last_err = f"upstream {resp.status_code}"
                    record_fail(k, str(last_err)[:200])
                    continue
                parsed = json.loads(resp.text)
                if not isinstance(parsed.get("content"), list):
                    last_err = "upstream non-anthropic body"
                    record_fail(k, str(last_err)[:200])
                    continue
                record_success(k)
                u = parsed.get("usage") or {}
                try:
                    await append_usage({"ts": time.time(), "model": model, "req": requested_model,
                                        "provider": provider["name"],
                                        "pt": u.get("input_tokens", 0) or 0,
                                        "ct": u.get("output_tokens", 0) or 0,
                                        "tt": (u.get("input_tokens", 0) or 0) + (u.get("output_tokens", 0) or 0)})
                except Exception:
                    pass
                return JSONResponse(_anth_to_openai_response(parsed, label))
            except httpx.RequestError as e:
                logger.warning("anth->openai error to %s: %s", provider["name"], repr(e))
                last_err = str(e)
                record_fail(k, str(last_err)[:200])
            except Exception as e:
                logger.exception("anth->openai unexpected error to %s", provider["name"])
                last_err = str(e)
                record_fail(k, str(last_err)[:200])
            continue
        # ===== MiniMax 原生转发 =====
        if provider.get("protocol") == "minimax-direct":
            mm_resp, mm_err = await _forward_minimax(provider, model, body, stream, k)
            if mm_resp is not None:
                return mm_resp
            last_err = mm_err
            continue
        # ===== 优AI转发 =====
        if provider.get("protocol") == "52who":
            yw_resp, yw_err = await _forward_52who(provider, model, body, stream, k)
            if yw_resp is not None:
                return yw_resp
            last_err = yw_err
            continue
        # ===== 百度文心转发 =====
        if provider.get("protocol") == "baidu":
            bd_resp, bd_err = await _forward_baidu(provider, model, body, stream, k)
            if bd_resp is not None:
                return bd_resp
            last_err = bd_err
            continue
        # ===== all-ai.cc Web 协议转发 =====
        if provider.get("protocol") == "allai":
            aa_resp, aa_err = await _forward_allai(provider, model, body, stream, k)
            if aa_resp is not None:
                return aa_resp
            last_err = aa_err
            continue
        # ===== WPS 灵犀 (WebSocket) 转发 =====
        if provider.get("protocol") == "lingxi":
            lx_resp, lx_err = await _forward_lingxi(provider, model, body, stream, k)
            if lx_resp is not None:
                return lx_resp
            last_err = lx_err
            continue
        # ===== 腾讯 ima 网页版转发 =====
        if provider.get("protocol") == "ima":
            im_resp, im_err = await _forward_ima(provider, model, body, stream, k)
            if im_resp is not None:
                return im_resp
            last_err = im_err
            continue
        # ===== 秘塔 Web 协议转发 =====
        if provider.get("protocol") == "metaso":
            ms_resp, ms_err = await _forward_metaso(provider, model, body, stream, k)
            if ms_resp is not None:
                return ms_resp
            last_err = ms_err
            continue
        # ===== 当贝 Web 协议：签名调用 + SSE 转换 =====
        if provider.get("protocol") == "dangbei":
            db_resp, db_err = await _forward_dangbei(provider, model, body, stream, k)
            if db_resp is not None:
                return db_resp
            last_err = db_err
            continue
        # ===== 问小白 Web 协议：签名调用 + SSE 转换（公共转发函数） =====
        if provider.get("protocol") == "wenxiaobai":
            wxb_resp, wxb_err = await _forward_wxb(provider, model, body, stream, k)
            if wxb_resp is not None:
                return wxb_resp
            last_err = wxb_err
            continue
        
        req_body = json.loads(json.dumps(body))
        req_body["model"] = MODEL_ALIASES.get(model, model)
        url = provider["base_url"].rstrip("/") + "/chat/completions"
        headers = {
            "Authorization": f"Bearer {provider['api_key']}",
            "Content-Type": "application/json",
        }
        headers.update(provider.get("extra_headers") or {})

        if stream:
            resp, err = await _try_stream_forward(url, req_body, headers, k, provider["name"], model)
            if resp is not None:
                return resp
            last_err = err
            continue

        try:
            t_iter = time.time()
            resp = await http_client.post(url, json=req_body, headers=headers, timeout=120)
            # 非 2xx（429 限流 / 5xx 等）视为失败，fallback 到下一候选
            if resp.status_code >= 400:
                logger.warning("upstream %d from %s: %s", resp.status_code, provider["name"], resp.text[:200])
                last_err = f"upstream {resp.status_code}"
                record_fail(k, str(last_err)[:200])
                continue
            try:
                parsed = json.loads(resp.text)
                # 静默拒绝检测: 200 但 choices 为空 / base_resp 带错误码(如 minimax 拼车 2067 额度上限)
                # ——必须判败回落, 否则客户端拿到空回复、巡检假绿
                _br = parsed.get("base_resp") or {}
                _chs = parsed.get("choices")
                if _br.get("status_code") or not _chs:
                    _why = _br.get("status_msg") or f"choices={_chs!r}"
                    logger.warning("upstream silent-reject from %s: %s", provider["name"], _why)
                    last_err = f"静默拒绝: {_why}"
                    open_circuit_now(k, last_err)
                    continue
                # 200 但正文空且无工具调用 = 空回复, 立即熔断换人
                try:
                    _msg = parsed["choices"][0].get("message") or {}
                    _c = _msg.get("content")
                    if not (_c or _msg.get("tool_calls") or _msg.get("reasoning_content")):
                        last_err = "空回复(200但零输出)"
                        open_circuit_now(k, last_err)
                        continue
                except (KeyError, IndexError, TypeError):
                    pass
                # reasoning_content 原生透传，不再 <think> 包装混入正文
                parsed_str = json.dumps(parsed, ensure_ascii=False)
                parsed_str = restore_hermes_text(parsed_str)
                parsed = json.loads(parsed_str)
                record_success(k)
                parsed["model"] = f"{provider['name']} · {model}"
                try:
                    u = parsed.get("usage") or {}
                    pt = u.get("prompt_tokens", 0) or 0
                    ct = u.get("completion_tokens", 0) or 0
                    if not (pt or ct):
                        _c = ((parsed.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
                        if _c:
                            pt, ct = _estimate_usage(body, len(_c) if isinstance(_c, str) else len(str(_c)))
                    await append_usage({
                        "ts": time.time(), "model": model,
                        "provider": provider["name"],
                        "pt": pt, "ct": ct, "tt": pt + ct,
                        "dur": round(time.time() - t_iter, 1),
                    })
                except Exception:
                    logger.exception("append_usage(non-stream) failed")
                try:
                    msg = parsed["choices"][0]["message"]
                    c = msg.get("content")
                    if isinstance(c, str) and c:
                        # 去掉网页端状态文本前缀，保持与标准 OpenAI 响应一致
                        for art in _WXB_ARTIFACTS:
                            if c.startswith(art):
                                c = c[len(art):]
                        msg["content"] = c
                except (KeyError, IndexError, TypeError, AttributeError):
                    pass
                note_real_success(provider["name"], model)
                return JSONResponse(content=parsed, status_code=resp.status_code)
            except json.JSONDecodeError:
                logger.warning("upstream non-json from %s: %s", provider["name"], resp.text[:200])
                last_err = f"upstream non-json ({resp.status_code})"
                record_fail(k, str(last_err)[:200])
                continue
        except httpx.RequestError as e:
            logger.warning("forward error to %s: %s", provider["name"], repr(e))
            last_err = str(e)
            record_fail(k, str(last_err)[:200])
            continue
        except Exception as e:
            logger.exception("unexpected forward error to %s", provider["name"])
            last_err = str(e)
            record_fail(k, str(last_err)[:200])
            continue

    raise HTTPException(502, f"所有候选模型均失败: {last_err}")


# ===== Anthropic Messages 兼容层：/v1/messages <-> OpenAI chat/completions =====

def _sse_event(ev: str, data: dict) -> str:
    return f"event: {ev}\ndata: " + json.dumps(data, ensure_ascii=False) + "\n\n"


def _anth_to_openai(body: dict) -> dict:
    """Anthropic Messages 请求体 -> OpenAI chat/completions 请求体"""
    msgs = []
    sysc = body.get("system")
    if isinstance(sysc, list):
        sysc = " ".join(b.get("text", "") for b in sysc if isinstance(b, dict))
    if isinstance(sysc, str) and sysc.strip():
        msgs.append({"role": "system", "content": sysc})

    for m in body.get("messages", []):
        role = m.get("role", "user")
        content = m.get("content")
        if isinstance(content, str):
            msgs.append({"role": role, "content": content})
            continue
        if role == "assistant":
            texts, tool_calls = [], []
            for b in content or []:
                if not isinstance(b, dict):
                    continue
                if b.get("type") == "text":
                    texts.append(b.get("text", ""))
                elif b.get("type") == "tool_use":
                    tool_calls.append({
                        "id": b.get("id", ""), "type": "function",
                        "function": {"name": b.get("name", ""),
                                     "arguments": json.dumps(b.get("input") or {}, ensure_ascii=False)},
                    })
            # 部分上游拒绝 content=null，空文本用 "" 兜底
            msg = {"role": "assistant", "content": "\n".join(texts)}
            if tool_calls:
                msg["tool_calls"] = tool_calls
            msgs.append(msg)
        else:  # user
            texts, parts = [], []
            deferred_images = []  # tool_result 里的图片，OpenAI tool 消息不能带，挪到本条 user 消息
            for b in content or []:
                if not isinstance(b, dict):
                    if isinstance(b, str):
                        texts.append(b)
                    continue
                if b.get("type") == "tool_result":
                    c = b.get("content")
                    if isinstance(c, list):
                        tx = []
                        for x in c:
                            if not isinstance(x, dict):
                                continue
                            if x.get("type") == "text":
                                tx.append(x.get("text", ""))
                            elif x.get("type") == "image":
                                src = x.get("source") or {}
                                if src.get("type") == "base64":
                                    deferred_images.append(
                                        f"data:{src.get('media_type', 'image/png')};base64,{src.get('data', '')}")
                        c = "".join(tx)
                    msgs.append({"role": "tool", "tool_call_id": b.get("tool_use_id", ""),
                                 "content": str(c or "")})
                elif b.get("type") == "text":
                    texts.append(b.get("text", ""))
                elif b.get("type") == "image":
                    src = b.get("source") or {}
                    if src.get("type") == "base64":
                        parts.append({"type": "image_url",
                                      "image_url": {"url": f"data:{src.get('media_type', 'image/png')};base64,{src.get('data', '')}"}})
                    elif src.get("type") == "url":
                        parts.append({"type": "image_url", "image_url": {"url": src.get("url", "")}})
            text = "\n".join(texts)
            if parts or deferred_images:
                mc = []
                if text:
                    mc.append({"type": "text", "text": text})
                mc.extend(parts)
                for u in deferred_images:
                    mc.append({"type": "image_url", "image_url": {"url": u}})
                msgs.append({"role": "user", "content": mc})
            elif text:
                msgs.append({"role": "user", "content": text})

    ob = {"model": body.get("model"), "messages": msgs,
          "max_tokens": min(body.get("max_tokens") or 16384, 65535)}
    if body.get("temperature") is not None:
        ob["temperature"] = body["temperature"]
    tools = body.get("tools")
    if tools:
        ob["tools"] = [{
            "type": "function",
            "function": {"name": t["name"], "description": t.get("description") or "",
                         "parameters": t.get("input_schema") or {"type": "object"}},
        } for t in tools]
        tc = body.get("tool_choice")
        if isinstance(tc, dict):
            if tc.get("type") == "any":
                ob["tool_choice"] = "required"
            elif tc.get("type") == "tool":
                ob["tool_choice"] = {"type": "function", "function": {"name": tc.get("name")}}
            else:
                ob["tool_choice"] = "auto"
    return ob


def _openai_to_anthropic(obj: dict, model_label: str) -> dict:
    """OpenAI 非流式响应 -> Anthropic message 响应"""
    choice = (obj.get("choices") or [{}])[0]
    msg = choice.get("message") or {}
    blocks, has_tool = [], False
    c = msg.get("content")
    if isinstance(c, list):  # 多段内容
        c = "".join(x.get("text", "") for x in c if isinstance(x, dict))
    if isinstance(c, str) and c:
        blocks.append({"type": "text", "text": c})
    for i, tc in enumerate(msg.get("tool_calls") or []):
        fn = tc.get("function") or {}
        try:
            args = json.loads(fn.get("arguments") or "{}")
        except Exception:
            args = {"_raw": fn.get("arguments")}
        blocks.append({"type": "tool_use", "id": tc.get("id") or f"toolu_{i}",
                       "name": fn.get("name"), "input": args})
        has_tool = True
    if not blocks:
        blocks.append({"type": "text", "text": ""})
    stop = "tool_use" if has_tool else ("max_tokens" if choice.get("finish_reason") == "length" else "end_turn")
    u = obj.get("usage") or {}
    return {"id": obj.get("id") or "msg_gateway", "type": "message", "role": "assistant",
            "model": model_label, "content": blocks, "stop_reason": stop, "stop_sequence": None,
            "usage": {"input_tokens": u.get("prompt_tokens", 0), "output_tokens": u.get("completion_tokens", 0)}}


def _openai_to_anth_request(body: dict) -> dict:
    """OpenAI chat/completions 请求体 -> Anthropic Messages 请求体"""
    msgs, sys_text = [], []
    for m in body.get("messages") or []:
        role, content = m.get("role", "user"), m.get("content")
        if role == "system":
            if isinstance(content, str):
                sys_text.append(content)
            elif isinstance(content, list):
                sys_text.append(" ".join(p.get("text", "") for p in content if isinstance(p, dict)))
            continue
        if role == "tool":
            msgs.append({"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": m.get("tool_call_id", ""),
                 "content": str(m.get("content") or "")}]})
            continue
        if role == "assistant" and m.get("tool_calls"):
            blocks = []
            if isinstance(content, str) and content:
                blocks.append({"type": "text", "text": content})
            for i, tc in enumerate(m["tool_calls"]):
                fn = tc.get("function") or {}
                try:
                    args = json.loads(fn.get("arguments") or "{}")
                except Exception:
                    args = {"_raw": fn.get("arguments")}
                blocks.append({"type": "tool_use", "id": tc.get("id") or f"toolu_{i}",
                               "name": fn.get("name", ""), "input": args})
            msgs.append({"role": "assistant", "content": blocks})
            continue
        if isinstance(content, list):
            blocks = []
            for p in content:
                if not isinstance(p, dict):
                    continue
                if p.get("type") == "text":
                    blocks.append({"type": "text", "text": p.get("text", "")})
                elif p.get("type") == "image_url":
                    url = (p.get("image_url") or {}).get("url", "")
                    if url.startswith("data:"):
                        head, _, b64 = url[5:].partition(",")
                        blocks.append({"type": "image", "source": {
                            "type": "base64", "media_type": head.split(";")[0] or "image/png", "data": b64}})
                    elif url:
                        blocks.append({"type": "image", "source": {"type": "url", "url": url}})
            msgs.append({"role": role, "content": blocks or [{"type": "text", "text": ""}]})
        else:
            msgs.append({"role": role, "content": str(content or "")})
    ab = {"model": body.get("model"), "max_tokens": min(body.get("max_tokens") or 16384, 65535),
          "messages": msgs}
    if sys_text:
        ab["system"] = "\n".join(sys_text)
    if body.get("temperature") is not None:
        ab["temperature"] = body["temperature"]
    tools = body.get("tools")
    if tools:
        ab["tools"] = [{
            "name": (t.get("function") or {}).get("name", ""),
            "description": (t.get("function") or {}).get("description") or "",
            "input_schema": (t.get("function") or {}).get("parameters") or {"type": "object"},
        } for t in tools if isinstance(t, dict) and t.get("type") == "function"]
        tc = body.get("tool_choice")
        if tc == "required":
            ab["tool_choice"] = {"type": "any"}
        elif isinstance(tc, dict) and tc.get("type") == "function":
            ab["tool_choice"] = {"type": "tool", "name": (tc.get("function") or {}).get("name")}
    return ab


def _anth_to_openai_response(obj: dict, model_label: str) -> dict:
    """Anthropic Messages 响应 -> OpenAI chat/completions 响应"""
    blocks = obj.get("content") or []
    text = "".join(b.get("text", "") for b in blocks if isinstance(b, dict) and b.get("type") == "text")
    tool_calls = []
    for b in blocks:
        if isinstance(b, dict) and b.get("type") == "tool_use":
            tool_calls.append({
                "id": b.get("id") or f"call_{len(tool_calls)}", "type": "function",
                "function": {"name": b.get("name", ""),
                             "arguments": json.dumps(b.get("input") or {}, ensure_ascii=False)}})
    msg = {"role": "assistant", "content": text}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    sr = obj.get("stop_reason")
    finish = "tool_calls" if sr == "tool_use" else ("length" if sr == "max_tokens" else "stop")
    u = obj.get("usage") or {}
    it, ot = u.get("input_tokens", 0) or 0, u.get("output_tokens", 0) or 0
    return {"id": obj.get("id") or "chatcmpl_gateway", "object": "chat.completion",
            "created": int(time.time()), "model": model_label,
            "choices": [{"index": 0, "message": msg, "finish_reason": finish}],
            "usage": {"prompt_tokens": it, "completion_tokens": ot, "total_tokens": it + ot}}


async def _openai_stream_from_anth(resp, model_label: str, k: str, provider_name: str,
                                   usage_model: str | None = None, req_model: str | None = None):
    """把上游 Anthropic SSE 流转换为 OpenAI chat.completion.chunk 流"""
    cid = f"chatcmpl-gw-{int(time.time() * 1000)}"

    def chunk(delta: dict, fin=None) -> str:
        obj = {"id": cid, "object": "chat.completion.chunk", "created": int(time.time()),
               "model": model_label, "choices": [{"index": 0, "delta": delta, "finish_reason": fin}]}
        return "data: " + json.dumps(obj, ensure_ascii=False) + "\n\n"

    in_tok = out_tok = 0
    try:
        yield chunk({"role": "assistant", "content": ""})
        async for line in resp.aiter_lines():
            if not line.startswith("data: "):
                continue
            try:
                ev = json.loads(line[6:])
            except Exception:
                continue
            t = ev.get("type")
            u = ev.get("usage") or (ev.get("message") or {}).get("usage") or {}
            if u.get("output_tokens"):
                out_tok = u["output_tokens"]
            if u.get("input_tokens"):
                in_tok = u["input_tokens"]
            if t == "content_block_start":
                b = ev.get("content_block") or {}
                if b.get("type") == "tool_use":
                    yield chunk({"tool_calls": [{"index": ev.get("index", 0), "id": b.get("id"),
                                                 "type": "function",
                                                 "function": {"name": b.get("name", ""), "arguments": ""}}]})
            elif t == "content_block_delta":
                d = ev.get("delta") or {}
                if d.get("type") == "text_delta" and d.get("text"):
                    yield chunk({"content": d["text"]})
                elif d.get("type") == "input_json_delta" and d.get("partial_json"):
                    yield chunk({"tool_calls": [{"index": ev.get("index", 0), "type": "function",
                                                 "function": {"arguments": d["partial_json"]}}]})
            elif t == "message_delta":
                sr = (ev.get("delta") or {}).get("stop_reason")
                fin = "tool_calls" if sr == "tool_use" else ("length" if sr == "max_tokens" else "stop")
                yield chunk({}, fin)
        yield "data: [DONE]\n\n"
        record_success(k)
    finally:
        await resp.aclose()
        try:
            await append_usage({"ts": time.time(), "model": usage_model or model_label, "req": req_model,
                                "provider": provider_name,
                                "pt": in_tok, "ct": out_tok, "tt": in_tok + out_tok})
        except Exception:
            pass


async def _anth_stream_gen(resp, model_label: str, k: str, provider_name: str,
                           usage_model: str | None = None, req_model: str | None = None):
    """把上游 OpenAI SSE 流转换为 Anthropic SSE 事件流"""
    yield _sse_event("message_start", {"type": "message_start", "message": {
        "id": "msg_gateway", "type": "message", "role": "assistant", "model": model_label,
        "content": [], "stop_reason": None, "stop_sequence": None,
        "usage": {"input_tokens": 0, "output_tokens": 0}}})
    yield _sse_event("ping", {"type": "ping"})

    text_open = False
    cur = 0
    tools = {}
    in_tok = out_tok = 0
    try:
        async for line in resp.aiter_lines():
            if not line.startswith("data: "):
                continue
            ds = line[6:]
            if ds.strip() == "[DONE]":
                break
            try:
                obj = json.loads(ds)
            except Exception:
                continue
            if isinstance(obj.get("usage"), dict):
                in_tok = obj["usage"].get("prompt_tokens", in_tok) or in_tok
                out_tok = obj["usage"].get("completion_tokens", out_tok) or out_tok
            choice = (obj.get("choices") or [{}])[0]
            delta = choice.get("delta") or {}
            txt = delta.get("content")
            if isinstance(txt, str) and txt:
                if not text_open:
                    yield _sse_event("content_block_start", {"type": "content_block_start", "index": cur,
                                                             "content_block": {"type": "text", "text": ""}})
                    text_open = True
                yield _sse_event("content_block_delta", {"type": "content_block_delta", "index": cur,
                                                         "delta": {"type": "text_delta", "text": txt}})
            for tcd in delta.get("tool_calls") or []:
                i = tcd.get("index", 0)
                t = tools.setdefault(i, {"id": "", "name": "", "args": ""})
                fn = tcd.get("function") or {}
                if tcd.get("id"):
                    t["id"] = tcd["id"]
                if fn.get("name"):
                    t["name"] = fn["name"]
                if fn.get("arguments"):
                    t["args"] += fn["arguments"]
        if text_open:
            yield _sse_event("content_block_stop", {"type": "content_block_stop", "index": cur})
            cur += 1
        for i in sorted(tools):
            t = tools[i]
            try:
                args = json.loads(t["args"] or "{}")
            except Exception:
                args = {}
            yield _sse_event("content_block_start", {"type": "content_block_start", "index": cur,
                                                     "content_block": {"type": "tool_use", "id": t["id"] or f"toolu_{i}",
                                                                       "name": t["name"], "input": {}}})
            yield _sse_event("content_block_delta", {"type": "content_block_delta", "index": cur,
                                                     "delta": {"type": "input_json_delta",
                                                               "partial_json": json.dumps(args, ensure_ascii=False)}})
            yield _sse_event("content_block_stop", {"type": "content_block_stop", "index": cur})
            cur += 1
        stop = "tool_use" if tools else "end_turn"
        yield _sse_event("message_delta", {"type": "message_delta",
                                           "delta": {"stop_reason": stop, "stop_sequence": None},
                                           "usage": {"output_tokens": out_tok}})
        yield _sse_event("message_stop", {"type": "message_stop"})
        record_success(k)
    except Exception as e:
        logger.exception("anthropic stream convert failed")
        record_fail(k, f"stream convert exception: {repr(e)[:150]}")
        raise
    finally:
        await resp.aclose()
        try:
            await append_usage({"ts": time.time(), "model": usage_model or model_label, "req": req_model,
                                "provider": provider_name,
                                "pt": in_tok, "ct": out_tok, "tt": in_tok + out_tok})
        except Exception:
            logger.exception("append_usage(anth-stream) failed")


@app.api_route("/v1/messages", methods=["POST"], dependencies=[Depends(verify_client)])
async def proxy_messages(request: Request):
    return await _handle_messages(request)


@app.api_route("/v1/v1/messages", methods=["POST"], dependencies=[Depends(verify_client)])
async def proxy_messages_dup(request: Request):
    # 兼容客户端把 /v1 重复拼进 base_url 的情况
    return await _handle_messages(request)


async def _handle_messages(request: Request):
    _req_t0.set(time.time())
    body = await request.json()
    stream = bool(body.get("stream"))
    model_label = body.get("model") or "gateway"
    ob = _anth_to_openai(body)
    candidates = pick_available_models(body.get("model"))
    candidates = filter_by_vision(candidates, body)
    candidates = filter_by_context(candidates, body)
    if not candidates:
        raise HTTPException(503, f"无可用的模型: {body.get('model') or '任意'}")
    last_err = None
    for provider, model in candidates:
        k = f"{provider['name']}||{model}"
        _inflight_mark(provider['name'], model)
        # ===== MiniMax：非流式转 Anthropic 格式 =====
        if provider.get("protocol") == "minimax-direct":
            mm_resp, mm_err = await _forward_minimax(provider, model, ob, False, k)
            if mm_resp is None:
                last_err = mm_err
                continue
            openai_obj = json.loads(mm_resp.body.decode("utf-8"))
            anth = _openai_to_anthropic(openai_obj, model_label)
            if not stream:
                return JSONResponse(anth)

            async def mmx_anth_gen(anth_obj=anth):
                yield _sse_event("message_start", {"type": "message_start", "message": {
                    "id": anth_obj.get("id") or "msg_gateway", "type": "message", "role": "assistant",
                    "model": model_label, "content": [], "stop_reason": None, "stop_sequence": None,
                    "usage": {"input_tokens": 0, "output_tokens": 0}}})
                yield _sse_event("ping", {"type": "ping"})
                cur = 0
                for b in anth_obj.get("content") or []:
                    yield _sse_event("content_block_start", {"type": "content_block_start", "index": cur,
                                                             "content_block": {"type": "text", "text": ""}})
                    if b.get("text"):
                        yield _sse_event("content_block_delta", {"type": "content_block_delta", "index": cur,
                                                                 "delta": {"type": "text_delta", "text": b["text"]}})
                    yield _sse_event("content_block_stop", {"type": "content_block_stop", "index": cur})
                    cur += 1
                yield _sse_event("message_delta", {"type": "message_delta",
                                                   "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                                                   "usage": {"output_tokens": (anth_obj.get("usage") or {}).get("output_tokens", 0)}})
                yield _sse_event("message_stop", {"type": "message_stop"})

            return StreamingResponse(mmx_anth_gen(), media_type="text/event-stream",
                                     headers={"anthropic-version": "1.0"})
        # ===== 优AI：非流式转 Anthropic 格式 =====
        if provider.get("protocol") == "52who":
            yw_resp, yw_err = await _forward_52who(provider, model, ob, False, k)
            if yw_resp is None:
                last_err = yw_err
                continue
            openai_obj = json.loads(yw_resp.body.decode("utf-8"))
            anth = _openai_to_anthropic(openai_obj, model_label)
            if not stream:
                return JSONResponse(anth)

            async def yw_anth_gen(anth_obj=anth):
                yield _sse_event("message_start", {"type": "message_start", "message": {
                    "id": anth_obj.get("id") or "msg_gateway", "type": "message", "role": "assistant",
                    "model": model_label, "content": [], "stop_reason": None, "stop_sequence": None,
                    "usage": {"input_tokens": 0, "output_tokens": 0}}})
                yield _sse_event("ping", {"type": "ping"})
                cur = 0
                for b in anth_obj.get("content") or []:
                    yield _sse_event("content_block_start", {"type": "content_block_start", "index": cur,
                                                             "content_block": {"type": "text", "text": ""}})
                    if b.get("text"):
                        yield _sse_event("content_block_delta", {"type": "content_block_delta", "index": cur,
                                                                 "delta": {"type": "text_delta", "text": b["text"]}})
                    yield _sse_event("content_block_stop", {"type": "content_block_stop", "index": cur})
                    cur += 1
                yield _sse_event("message_delta", {"type": "message_delta",
                                                   "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                                                   "usage": {"output_tokens": (anth_obj.get("usage") or {}).get("output_tokens", 0)}})
                yield _sse_event("message_stop", {"type": "message_stop"})

            return StreamingResponse(yw_anth_gen(), media_type="text/event-stream",
                                     headers={"anthropic-version": "1.0"})
        # ===== 百度文心：非流式转 Anthropic 格式 =====
        if provider.get("protocol") == "baidu":
            bd_resp, bd_err = await _forward_baidu(provider, model, ob, False, k)
            if bd_resp is None:
                last_err = bd_err
                continue
            openai_obj = json.loads(bd_resp.body.decode("utf-8"))
            anth = _openai_to_anthropic(openai_obj, model_label)
            if not stream:
                return JSONResponse(anth)
            u = anth.get("usage") or {}

            async def bdu_anth_gen(anth_obj=anth):
                yield _sse_event("message_start", {"type": "message_start", "message": {
                    "id": anth_obj.get("id") or "msg_gateway", "type": "message", "role": "assistant",
                    "model": model_label, "content": [], "stop_reason": None, "stop_sequence": None,
                    "usage": {"input_tokens": 0, "output_tokens": 0}}})
                yield _sse_event("ping", {"type": "ping"})
                cur = 0
                for b in anth_obj.get("content") or []:
                    yield _sse_event("content_block_start", {"type": "content_block_start", "index": cur,
                                                             "content_block": {"type": "text", "text": ""}})
                    if b.get("text"):
                        yield _sse_event("content_block_delta", {"type": "content_block_delta", "index": cur,
                                                                 "delta": {"type": "text_delta", "text": b["text"]}})
                    yield _sse_event("content_block_stop", {"type": "content_block_stop", "index": cur})
                    cur += 1
                yield _sse_event("message_delta", {"type": "message_delta",
                                                   "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                                                   "usage": {"output_tokens": (anth_obj.get("usage") or {}).get("output_tokens", 0)}})
                yield _sse_event("message_stop", {"type": "message_stop"})

            return StreamingResponse(bdu_anth_gen(), media_type="text/event-stream",
                                     headers={"anthropic-version": "1.0"})
        # ===== all-ai.cc：非流式转 Anthropic 格式 =====
        if provider.get("protocol") == "allai":
            aa_resp, aa_err = await _forward_allai(provider, model, ob, False, k)
            if aa_resp is None:
                last_err = aa_err
                continue
            openai_obj = json.loads(aa_resp.body.decode("utf-8"))
        # ===== WPS 灵犀：非流式转 Anthropic 格式 =====
        elif provider.get("protocol") == "lingxi":
            lx_resp, lx_err = await _forward_lingxi(provider, model, ob, False, k)
            if lx_resp is None:
                last_err = lx_err
                continue
            openai_obj = json.loads(lx_resp.body.decode("utf-8"))
        # ===== 腾讯 ima：非流式转 Anthropic 格式 =====
        elif provider.get("protocol") == "ima":
            im_resp, im_err = await _forward_ima(provider, model, ob, False, k)
            if im_resp is None:
                last_err = im_err
                continue
            openai_obj = json.loads(im_resp.body.decode("utf-8"))
            anth = _openai_to_anthropic(openai_obj, model_label)
            if not stream:
                return JSONResponse(anth)
            u = anth.get("usage") or {}

            async def aa_anth_gen(anth_obj=anth):
                yield _sse_event("message_start", {"type": "message_start", "message": {
                    "id": anth_obj.get("id") or "msg_gateway", "type": "message", "role": "assistant",
                    "model": model_label, "content": [], "stop_reason": None, "stop_sequence": None,
                    "usage": {"input_tokens": 0, "output_tokens": 0}}})
                yield _sse_event("ping", {"type": "ping"})
                cur = 0
                for b in anth_obj.get("content") or []:
                    yield _sse_event("content_block_start", {"type": "content_block_start", "index": cur,
                                                             "content_block": {"type": "text", "text": ""}})
                    if b.get("text"):
                        yield _sse_event("content_block_delta", {"type": "content_block_delta", "index": cur,
                                                                 "delta": {"type": "text_delta", "text": b["text"]}})
                    yield _sse_event("content_block_stop", {"type": "content_block_stop", "index": cur})
                    cur += 1
                yield _sse_event("message_delta", {"type": "message_delta",
                                                   "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                                                   "usage": {"output_tokens": (anth_obj.get("usage") or {}).get("output_tokens", 0)}})
                yield _sse_event("message_stop", {"type": "message_stop"})

            return StreamingResponse(aa_anth_gen(), media_type="text/event-stream",
                                     headers={"anthropic-version": "1.0"})
        # ===== 秘塔 Web 协议：内部按非流式收集，再转成 Anthropic 格式 =====
        if provider.get("protocol") == "metaso":
            ms_resp, ms_err = await _forward_metaso(provider, model, ob, False, k)
            if ms_resp is None:
                last_err = ms_err
                continue
            openai_obj = json.loads(ms_resp.body.decode("utf-8"))
            anth = _openai_to_anthropic(openai_obj, model_label)
            if not stream:
                return JSONResponse(anth)
            u = anth.get("usage") or {}
            try:
                await append_usage({"ts": time.time(), "model": model, "req": model_label,
                                    "provider": provider["name"], "pt": u.get("input_tokens", 0),
                                    "ct": u.get("output_tokens", 0),
                                    "tt": u.get("input_tokens", 0) + u.get("output_tokens", 0)})
            except Exception:
                pass

            async def ms_anth_gen(anth_obj=anth):
                yield _sse_event("message_start", {"type": "message_start", "message": {
                    "id": anth_obj.get("id") or "msg_gateway", "type": "message", "role": "assistant",
                    "model": model_label, "content": [], "stop_reason": None, "stop_sequence": None,
                    "usage": {"input_tokens": 0, "output_tokens": 0}}})
                yield _sse_event("ping", {"type": "ping"})
                cur = 0
                for b in anth_obj.get("content") or []:
                    yield _sse_event("content_block_start", {"type": "content_block_start", "index": cur,
                                                             "content_block": {"type": "text", "text": ""}})
                    if b.get("text"):
                        yield _sse_event("content_block_delta", {"type": "content_block_delta", "index": cur,
                                                                 "delta": {"type": "text_delta", "text": b["text"]}})
                    yield _sse_event("content_block_stop", {"type": "content_block_stop", "index": cur})
                    cur += 1
                yield _sse_event("message_delta", {"type": "message_delta",
                                                   "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                                                   "usage": {"output_tokens": (anth_obj.get("usage") or {}).get("output_tokens", 0)}})
                yield _sse_event("message_stop", {"type": "message_stop"})

            return StreamingResponse(ms_anth_gen(), media_type="text/event-stream",
                                     headers={"anthropic-version": "1.0"})
        # ===== 当贝 Web 协议：内部按非流式收集，再转成 Anthropic 格式 =====
        if provider.get("protocol") == "dangbei":
            db_resp, db_err = await _forward_dangbei(provider, model, ob, False, k)
            if db_resp is None:
                last_err = db_err
                continue
            openai_obj = json.loads(db_resp.body.decode("utf-8"))
            anth = _openai_to_anthropic(openai_obj, model_label)
            if not stream:
                return JSONResponse(anth)
            u = anth.get("usage") or {}
            try:
                await append_usage({"ts": time.time(), "model": model, "req": model_label,
                                    "provider": provider["name"], "pt": u.get("input_tokens", 0),
                                    "ct": u.get("output_tokens", 0),
                                    "tt": u.get("input_tokens", 0) + u.get("output_tokens", 0)})
            except Exception:
                pass

            async def db_anth_gen(anth_obj=anth):
                yield _sse_event("message_start", {"type": "message_start", "message": {
                    "id": anth_obj.get("id") or "msg_gateway", "type": "message", "role": "assistant",
                    "model": model_label, "content": [], "stop_reason": None, "stop_sequence": None,
                    "usage": {"input_tokens": (anth_obj.get("usage") or {}).get("input_tokens", 0),
                              "output_tokens": 0}}})
                yield _sse_event("ping", {"type": "ping"})
                cur = 0
                for b in anth_obj.get("content") or []:
                    yield _sse_event("content_block_start", {"type": "content_block_start", "index": cur,
                                                             "content_block": {"type": "text", "text": ""}})
                    if b.get("text"):
                        yield _sse_event("content_block_delta", {"type": "content_block_delta", "index": cur,
                                                                 "delta": {"type": "text_delta", "text": b["text"]}})
                    yield _sse_event("content_block_stop", {"type": "content_block_stop", "index": cur})
                    cur += 1
                yield _sse_event("message_delta", {"type": "message_delta",
                                                   "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                                                   "usage": {"output_tokens": (anth_obj.get("usage") or {}).get("output_tokens", 0)}})
                yield _sse_event("message_stop", {"type": "message_stop"})

            return StreamingResponse(db_anth_gen(), media_type="text/event-stream",
                                     headers={"anthropic-version": "1.0"})
        # ===== 问小白 Web 协议：内部按非流式收集，再转成 Anthropic 格式 =====
        if provider.get("protocol") == "wenxiaobai":
            wxb_resp, wxb_err = await _forward_wxb(provider, model, ob, False, k)
            if wxb_resp is None:
                last_err = wxb_err
                continue
            openai_obj = json.loads(wxb_resp.body.decode("utf-8"))
            anth = _openai_to_anthropic(openai_obj, model_label)
            if not stream:
                return JSONResponse(anth)
            u = anth.get("usage") or {}
            try:
                await append_usage({"ts": time.time(), "model": model, "req": model_label,
                                    "provider": provider["name"], "pt": u.get("input_tokens", 0),
                                    "ct": u.get("output_tokens", 0),
                                    "tt": u.get("input_tokens", 0) + u.get("output_tokens", 0)})
            except Exception:
                pass

            async def wxb_anth_gen(anth_obj=anth):
                yield _sse_event("message_start", {"type": "message_start", "message": {
                    "id": anth_obj.get("id") or "msg_gateway", "type": "message", "role": "assistant",
                    "model": model_label, "content": [], "stop_reason": None, "stop_sequence": None,
                    "usage": {"input_tokens": (anth_obj.get("usage") or {}).get("input_tokens", 0),
                              "output_tokens": 0}}})
                yield _sse_event("ping", {"type": "ping"})
                cur = 0
                for b in anth_obj.get("content") or []:
                    cb = {"type": b.get("type", "text")}
                    if b.get("type") == "tool_use":
                        cb.update({"id": b.get("id"), "name": b.get("name"), "input": {}})
                    else:
                        cb.update({"text": b.get("text", "")})
                    yield _sse_event("content_block_start", {"type": "content_block_start", "index": cur,
                                                             "content_block": cb})
                    if b.get("type") == "text" and b.get("text"):
                        yield _sse_event("content_block_delta", {"type": "content_block_delta", "index": cur,
                                                                 "delta": {"type": "text_delta", "text": b["text"]}})
                    elif b.get("type") == "tool_use":
                        yield _sse_event("content_block_delta", {"type": "content_block_delta", "index": cur,
                                                                 "delta": {"type": "input_json_delta",
                                                                           "partial_json": json.dumps(b.get("input") or {}, ensure_ascii=False)}})
                    yield _sse_event("content_block_stop", {"type": "content_block_stop", "index": cur})
                    cur += 1
                yield _sse_event("message_delta", {"type": "message_delta",
                                                   "delta": {"stop_reason": anth_obj.get("stop_reason") or "end_turn",
                                                             "stop_sequence": None},
                                                   "usage": {"output_tokens": (anth_obj.get("usage") or {}).get("output_tokens", 0)}})
                yield _sse_event("message_stop", {"type": "message_stop"})

            return StreamingResponse(wxb_anth_gen(), media_type="text/event-stream",
                                     headers={"anthropic-version": "1.0"})
        # ===== Anthropic 协议上游：同协议直接透传 =====
        if provider.get("protocol") == "anthropic":
            url = provider["base_url"].rstrip("/") + "/messages"
            headers = {"x-api-key": provider["api_key"], "anthropic-version": "2023-06-01",
                       "Content-Type": "application/json"}
            req_body = dict(body)
            req_body["model"] = MODEL_ALIASES.get(model, model)
            req_body["stream"] = stream
            try:
                if stream:
                    req = http_client.build_request("POST", url, json=req_body, headers=headers)
                    resp = await http_client.send(req, stream=True)
                    if resp.status_code >= 400:
                        detail = (await resp.aread())[:200]
                        await resp.aclose()
                        logger.warning("anth-passthrough upstream %d from %s: %s", resp.status_code, provider["name"], detail)
                        last_err = f"upstream {resp.status_code}"
                        record_fail(k, str(last_err)[:200])
                        continue
                    record_success(k)

                    async def pass_gen(rp=resp, kk=k, pn=provider["name"]):
                        out_tok = 0
                        try:
                            async for line in rp.aiter_lines():
                                if line.startswith("data: "):
                                    try:
                                        ev = json.loads(line[6:])
                                        u = ev.get("usage") or (ev.get("message") or {}).get("usage") or {}
                                        out_tok = u.get("output_tokens", out_tok) or out_tok
                                        in_tok = u.get("input_tokens", 0)
                                    except Exception:
                                        in_tok = 0
                                    yield line + "\n\n"
                                else:
                                    yield line + "\n"
                            record_success(kk)
                        finally:
                            await rp.aclose()
                            try:
                                await append_usage({"ts": time.time(), "model": model, "req": model_label,
                                                    "provider": pn, "pt": 0, "ct": out_tok, "tt": out_tok})
                            except Exception:
                                pass

                    return StreamingResponse(pass_gen(), media_type="text/event-stream")
                resp = await http_client.post(url, json=req_body, headers=headers, timeout=180)
                if resp.status_code >= 400:
                    logger.warning("anth-passthrough upstream %d from %s: %s", resp.status_code, provider["name"], resp.text[:200])
                    last_err = f"upstream {resp.status_code}"
                    record_fail(k, str(last_err)[:200])
                    continue
                parsed = json.loads(resp.text)
                if not isinstance(parsed.get("content"), list):
                    last_err = "upstream non-anthropic body"
                    record_fail(k, str(last_err)[:200])
                    continue
                record_success(k)
                u = parsed.get("usage") or {}
                try:
                    await append_usage({"ts": time.time(), "model": model, "req": model_label, "provider": provider["name"],
                                        "pt": u.get("input_tokens", 0) or 0, "ct": u.get("output_tokens", 0) or 0,
                                        "tt": (u.get("input_tokens", 0) or 0) + (u.get("output_tokens", 0) or 0)})
                except Exception:
                    pass
                return JSONResponse(parsed)
            except httpx.RequestError as e:
                logger.warning("anth-passthrough error to %s: %s", provider["name"], repr(e))
                last_err = str(e)
                record_fail(k, str(last_err)[:200])
            except Exception as e:
                logger.exception("anth-passthrough unexpected error to %s", provider["name"])
                last_err = str(e)
                record_fail(k, str(last_err)[:200])
            continue
        # ===== OpenAI 协议上游：协议转换 =====
        req_body = dict(ob)
        req_body["model"] = MODEL_ALIASES.get(model, model)
        req_body["stream"] = stream
        url = provider["base_url"].rstrip("/") + "/chat/completions"
        headers = {"Authorization": f"Bearer {provider['api_key']}", "Content-Type": "application/json"}
        try:
            if stream:
                req = http_client.build_request("POST", url, json=req_body, headers=headers)
                resp = await http_client.send(req, stream=True)
                if resp.status_code >= 400:
                    detail = (await resp.aread())[:200]
                    await resp.aclose()
                    logger.warning("anth upstream %d from %s: %s", resp.status_code, provider["name"], detail)
                    last_err = f"upstream {resp.status_code}"
                    record_fail(k, str(last_err)[:200])
                    continue
                return StreamingResponse(_anth_stream_gen(resp, model_label, k, provider["name"],
                                                          usage_model=model, req_model=model_label),
                                          media_type="text/event-stream",
                                          headers={"anthropic-version": "1.0"})
            resp = await http_client.post(url, json=req_body, headers=headers, timeout=180)
            if resp.status_code >= 400:
                logger.warning("anth upstream %d from %s: %s", resp.status_code, provider["name"], resp.text[:200])
                last_err = f"upstream {resp.status_code}"
                record_fail(k, str(last_err)[:200])
                continue
            parsed = json.loads(resp.text)
            record_success(k)
            u = parsed.get("usage") or {}
            try:
                await append_usage({"ts": time.time(), "model": model, "req": model_label, "provider": provider["name"],
                                    "pt": u.get("prompt_tokens", 0) or 0, "ct": u.get("completion_tokens", 0) or 0,
                                    "tt": (u.get("prompt_tokens", 0) or 0) + (u.get("completion_tokens", 0) or 0)})
            except Exception:
                pass
            return JSONResponse(_openai_to_anthropic(parsed, model_label))
        except httpx.RequestError as e:
            logger.warning("anth forward error to %s: %s", provider["name"], repr(e))
            last_err = str(e)
            record_fail(k, str(last_err)[:200])
        except Exception as e:
            logger.exception("anth unexpected error to %s", provider["name"])
            last_err = str(e)
            record_fail(k, str(last_err)[:200])
    raise HTTPException(502, f"所有候选模型均失败: {last_err}")


@app.api_route("/v1/models", methods=["GET"], dependencies=[Depends(verify_client)])
async def proxy_models():
    models_list = []

    # 自定义路由组作为可输出的模型
    for router_name in ROUTERS:
        models_list.append({
            "id": router_name,
            "object": "model",
            "owned_by": "Router",
            "available": True,
        })

    for p in providers:
        disabled = set(p.get("disabled_models", []))
        for m in p.get("models", []):
            if m in disabled:
                continue
            k = f"{p['name']}||{m}"
            st = health_status.get(k, {}).get("status")
            # 三态：unknown/None -> True（乐观），ok -> True，fail/error -> False
            available = st in (None, "unknown", "ok")
            ctx_len = get_context_length(m)
            models_list.append({
                "id": f"{p['name']}-{m}",
                "object": "model",
                "owned_by": p["name"],
                "available": available,
                "context_length": ctx_len,
                "max_position_embeddings": ctx_len,
                "max_model_len": ctx_len,
            })
    return {"object": "list", "data": models_list}


if __name__ == "__main__":
    import sys as _sys
    import uvicorn
    if "--gui" not in _sys.argv:
        # 默认无窗口模式，只起 API 服务（要监控窗口用 --gui）
        uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")
        raise SystemExit(0)
    import threading
    import webview
    import time
    from PIL import Image, ImageDraw
    import pystray
    import msvcrt

    # ---- 单实例限制：防止重复启动 ----
    LOCK_FILE = str(DATA_DIR / ".gateway.lock")
    try:
        _lock_fd = open(LOCK_FILE, "w")
        msvcrt.locking(_lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
    except (OSError, IOError):
        import ctypes
        ctypes.windll.user32.MessageBoxW(
            0, "网关客户端已在运行中，请勿重复启动。", "提示", 0x30
        )
        sys.exit(0)

    # ---- 生成托盘图标（纯几何图形，不依赖外部图片文件） ----
    def create_tray_icon():
        img = Image.new('RGBA', (64, 64), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        # 蓝色圆角底
        draw.rounded_rectangle([4, 4, 60, 60], radius=14, fill=(30, 144, 255))
        # 白色右箭头，代表"网关/转发"
        draw.polygon([(22, 20), (44, 32), (22, 44)], fill="white")
        return img

    # ---- 全局状态：quitting 用于区分"点X隐藏"与"托盘退出" ----
    state = {"window": None, "quitting": False}

    # ---- 托盘菜单回调 ----
    def on_show(icon, item):
        w = state["window"]
        if w:
            w.show()

    def on_quit(icon, item):
        state["quitting"] = True
        icon.stop()
        w = state["window"]
        if w:
            w.destroy()

    tray_icon = pystray.Icon(
        "model-gateway",
        create_tray_icon(),
        "有限额度监控网关",
        menu=pystray.Menu(
            pystray.MenuItem("显示窗口", on_show, default=True),
            pystray.MenuItem("退出", on_quit),
        ),
    )

    # ---- FastAPI 服务器（daemon 线程，主进程退出时自动结束） ----
    def start_server():
        uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")

    t = threading.Thread(target=start_server, daemon=True)
    t.start()

    # 给一点点时间让 FastAPI 绑定端口
    time.sleep(1)

    # ---- 创建原生的桌面窗口 ----
    window = webview.create_window(
        '有限额度监控网关', 'http://127.0.0.1:8000/', width=1200, height=800
    )
    state["window"] = window

    # ---- 拦截关闭：点 X 时隐藏到托盘，而非退出程序 ----
    def on_closing():
        if state["quitting"]:
            return  # 退出流程：放行，允许真正关闭
        window.hide()
        return False  # 阻止关闭，仅隐藏窗口

    window.events.closing += on_closing

    # ---- 启动系统托盘（独立 daemon 线程） ----
    threading.Thread(target=tray_icon.run, daemon=True).start()

    # ---- 启动 webview（主线程阻塞，窗口全部关闭后返回） ----
    webview.start()

