# -*- coding: utf-8 -*-
"""网关控制台监控 v2：只显示最佳前5渠道 + 最近调用日志。
热重载：providers.json / config.json 变更后无需重启，下一轮刷新自动生效。
用法: python monitor.py [刷新间隔秒数, 默认5]
"""
import json
import os
import sys
import time
import unicodedata
import urllib.request


def dw(s):
    """显示宽度：中文/全角按 2 列计"""
    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in s)


def pad(s, w, align="l"):
    """按显示宽度对齐补空格（f-string 的 :<w 按字符数算，中文一列占两格会错位）"""
    s = str(s)
    fill = max(0, w - dw(s))
    return (" " * fill + s) if align == "r" else (s + " " * fill)

BASE = "http://127.0.0.1:8000"
HERE = os.path.dirname(os.path.abspath(__file__))
USAGE_FILE = os.path.join(HERE, "usage.jsonl")


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def api(path, key):
    try:
        req = urllib.request.Request(BASE + path, headers={"Authorization": "Bearer " + key})
        with urllib.request.urlopen(req, timeout=4) as r:
            return json.load(r)
    except Exception as e:
        return {"__error__": str(e)}


def fmt_ctx(n):
    if not n:
        return "-"
    if n >= 1048576:
        return f"{round(n/1048576, 1):g}M"
    if n >= 1024:
        return f"{n//1024}K"
    return str(n)


def tail_usage(n=12):
    """读取 usage.jsonl 末尾 n 条调用记录"""
    if not os.path.exists(USAGE_FILE):
        return []
    try:
        with open(USAGE_FILE, encoding="utf-8") as f:
            lines = f.readlines()[-n:]
        out = []
        for ln in lines:
            try:
                d = json.loads(ln)
                out.append(d)
            except Exception:
                pass
        return out
    except Exception:
        return []


def build_group_map():
    """渠道·模型 -> 所属路由组列表（all 是全池不单列；分组类型只写组名，绝不与模型名混写）。
    某模型只要存在 渠道/模型 限定写法，裸模型名匹配即失效（防同名跨渠道串组，ccvibe 事件同款坑）。"""
    routers = load_json(os.path.join(HERE, "routers.json"), {})
    qmap, bmap = {}, {}
    for grp, entries in routers.items():
        if grp == "all":
            continue
        for e in entries or []:
            if "/" in e:
                qmap.setdefault(e, set()).add(grp)
            else:
                bmap.setdefault(e, set()).add(grp)
    return qmap, {m: g for m, g in bmap.items() if not any(q.startswith(m + "/") or q.endswith("/" + m) for q in qmap)}


def groups_of(qmap, bmap, provider, model):
    """单一溯源：命中多个组时只归一个组（coding > transcribe > proxy，取最专属用途），绝不混合显示。"""
    hits = qmap.get(f"{provider}/{model}", set()) | bmap.get(model, set())
    for g in ("coding", "transcribe", "proxy"):
        if g in hits:
            return [g]
    return []


# 网页反代渠道（按渠道实质判别，qwen-web 虽写 openai 协议也是 sidecar 反代）——不进"最佳前5"
REVERSE_PROVIDERS = {"wenxiaobai", "dangbei", "metaso", "allai", "baidu", "youai",
                     "minimax", "ima", "lingxi", "qwen-web"}


def meta_specs():
    return load_json(os.path.join(HERE, "models_meta.json"), {}).get("model_specs", {})


def usage_stats():
    """统计 usage.jsonl 最近 7 天各 渠道·模型 的总 token（输入+输出）与调用次数"""
    stats = {}
    cutoff = time.time() - 7 * 86400
    if os.path.exists(USAGE_FILE):
        try:
            with open(USAGE_FILE, encoding="utf-8") as f:
                for ln in f:
                    try:
                        d = json.loads(ln)
                    except Exception:
                        continue
                    if d.get("ts", 0) < cutoff:
                        continue
                    key = (d.get("provider"), d.get("model"))
                    if not key[1]:
                        continue
                    st = stats.setdefault(key, {"tokens": 0, "calls": 0})
                    st["tokens"] += (d.get("pt", 0) or 0) + (d.get("ct", 0) or 0)
                    st["calls"] += 1
        except Exception:
            pass
    return stats


def fmt_tokens(n):
    if not n:
        return "-"
    if n >= 1000000:
        return f"{n/1000000:.1f}M"
    if n >= 1000:
        return f"{n/1000:.0f}K"
    return str(n)


def render(key, providers):
    details = api("/api/model-details", key)
    stab = api("/api/stability", key)
    poll = api("/api/poll-status", key)

    os.system("cls" if os.name == "nt" else "clear")
    print("=" * 88)
    print(f"  模型网关监控   {time.strftime('%Y-%m-%d %H:%M:%S')}   渠道数: {len(providers)}   Ctrl+C 退出")
    print("=" * 88)

    if "__error__" in details:
        print(f"  [!] 网关不可达: {details['__error__']}")
        print(f"      启动: python -m uvicorn app:app --host 127.0.0.1 --port 8000")
        return

    stab_map = {(s.get("provider"), s.get("model")): s for s in (stab if isinstance(stab, list) else [])}

    # ---- 收集所有渠道，按 可用率降序 + 均延迟升序 排序，取前5 ----
    ctx_default = {"sensenova-6.8-flash-lite": 262144, "deepseek-ai/DeepSeek-V4-Pro": 1048576}
    rows = []
    qmap, bmap = build_group_map()
    for p in providers:
        for m in p.get("models", []):
            d = details.get(m, {})
            s = stab_map.get((p.get("name"), m), {})
            avail = s.get("availability") or 0
            lat = s.get("avg_latency_ms") or 10 ** 9
            grps = groups_of(qmap, bmap, p.get("name"), m)
            rows.append({
                "name": p.get("name", "?"), "model": m,
                "groups": "/".join(grps) if grps else "-",
                "ctx": d.get("context_length") or ctx_default.get(m),
                "avail": s.get("availability"),
                "lat": s.get("avg_latency_ms"),
                "checks": s.get("checks", 0),
                "last": s.get("last_status", ""),
            })
    # ---- 排除反代渠道和已死渠道；排序：组内强度 > 近7天用量(token) > 可用率 > 延迟 ----
    ustats = usage_stats()
    rows = [r for r in rows if r["name"] not in REVERSE_PROVIDERS]
    for r in rows:
        u = ustats.get((r["name"], r["model"]), {"tokens": 0, "calls": 0})
        r["tokens"], r["calls"] = u["tokens"], u["calls"]
    rows = [r for r in rows if not (r["checks"] and (r["avail"] or 0) == 0)]  # 死掉的不算
    specs = meta_specs()
    def tier(r):
        g = r["groups"]
        return 0 if g == "coding" else (1 if g == "transcribe" else 2)
    for r in rows:
        sp = specs.get(r["model"]) or {}
        r["act"] = sp.get("active") or 0
        r["tot"] = sp.get("total") or 0
    rows.sort(key=lambda r: (tier(r), -r["tokens"], -(r["avail"] or 0), r["lat"]))
    top = rows[:5]
    # 展示列: 强度(激活参数)参与呈现, 闭源未公开模型显示旗舰档
    # ---- 最近使用榜(滚动): 按最近调用顺序取5个, 正在调用的打 -> ----
    live_models = {(p.get("name"), m) for p in providers for m in p.get("models", [])}
    logs_all = tail_usage(60)
    seen = set()
    recent5 = []
    for d in reversed(logs_all):  # 新的在前
        k = (d.get("provider"), d.get("model"))
        if not k[1] or k in seen:
            continue
        if k[0] in REVERSE_PROVIDERS or k not in live_models:
            continue  # 反代/已清空渠道绝不上榜
        seen.add(k)
        recent5.append((k, d.get("ts", 0), d.get("req")))
        if len(recent5) >= 5:
            break
    row_map = {(r["name"], r["model"]): r for r in rows}
    now_ts = time.time()
    # 实单健康: 各模型最近30分钟真实调用的 dur 列表(no_probe渠道探针表为空, 以实单为准)
    real_durs = {}
    for d in logs_all:
        k = (d.get("provider"), d.get("model"))
        if now_ts - d.get("ts", 0) < 1800 and d.get("dur") is not None:
            real_durs.setdefault(k, []).append(d["dur"])

    print("▎最近使用 5 模型（滚动刷新；-> = 正在调用）")
    print(pad("渠道", 16) + pad("模型", 26) + pad("分组", 22) + pad("参数(A/T)", 11, "r")
          + pad("近7天", 7, "r") + pad("上下文", 7, "r") + pad("可用率", 7, "r") + pad("均延迟", 7, "r") + "  状态")
    print("-" * 108)
    for (pn, mn), last_ts, last_req in recent5:
        r = row_map.get((pn, mn)) or {"name": pn, "model": mn, "groups": "-",
                                       "ctx": None, "avail": None, "lat": None, "last": "",
                                       "tokens": ustats.get((pn, mn), {}).get("tokens", 0),
                                       "act": None, "tot": None}
        arrow = "->" if now_ts - last_ts < 120 else "  "
        if last_req in ("coding", "transcribe", "proxy"):
            r = dict(r); r["groups"] = last_req
        durs = real_durs.get((pn, mn)) or []
        last_call_fresh = now_ts - last_ts < 600
        if last_call_fresh:
            status = "正常(实单)"
        else:
            status = {"ok": "正常", "fail": "故障"}.get(r["last"], "未检")
        avail_s = f"{r['avail']:.0f}%" if r["avail"] is not None else "-"
        if durs:
            lat_s = f"{sum(durs)/len(durs):.1f}s"
        elif r["lat"]:
            lat_s = f"{r['lat']/1000:.1f}s"
        else:
            lat_s = "-"
        d2 = details.get(mn) or {}
        act = r["act"] or d2.get("active_params_b")
        tot = r["tot"] or d2.get("total_params_b")
        def fp(v):
            if v is None:
                return "未"
            return f"{v/1000:g}T" if v >= 1000 else f"{v:g}B"
        spec_s = f"{fp(act)}/{fp(tot)}" if (act or tot) else "未收录"
        m_disp = r["model"].rsplit("/", 1)[-1]
        print(pad(arrow + r["name"], 16) + pad(m_disp, 26) + pad(r["groups"], 22)
              + pad(spec_s, 11, "r") + pad(fmt_tokens(r["tokens"]), 7, "r") + pad(fmt_ctx(r["ctx"]), 7, "r")
              + pad(avail_s, 7, "r") + pad(lat_s, 7, "r") + "  " + status)
    print("-" * 108)

    # ---- 最近调用日志 ----
    print("▎最近调用日志")
    logs = tail_usage(12)
    if not logs:
        print("  （暂无调用记录）")
    else:
        print(pad("时间", 10) + pad("渠道", 14) + pad("模型", 28) + pad("分组", 20)
              + pad("输入", 8, "r") + pad("输出", 8, "r") + pad("延时", 7, "r"))
        for d in reversed(logs):  # 最新在上
            t = time.strftime("%H:%M:%S", time.localtime(d.get("ts", 0)))
            m_s = str(d.get("model", "-"))
            # 多级模型名只留末段, 防止挤爆分组列
            if "/" in m_s:
                m_s = m_s.rsplit("/", 1)[1]
            req = d.get("req")
            g_s = req if req in ("coding", "transcribe", "proxy", "all") else (
                "/".join(groups_of(qmap, bmap, d.get("provider", ""), m_s)) or "-")
            dur = d.get("dur")
            dur_s = f"{dur}s" if dur is not None else "-"
            print(pad(t, 10) + pad(d.get("provider", "-"), 14) + pad(m_s, 28) + pad(g_s, 20)
                  + pad(d.get("pt", 0), 8, "r") + pad(d.get("ct", 0), 8, "r") + pad(dur_s, 7, "r"))
    print("-" * 88)

    if isinstance(poll, dict) and "total_models" in poll:
        print(f"巡检: 共 {poll['total_models']} 个模型 | 面板 {BASE} | 改 providers.json 自动热重载")


def main():
    interval = float(sys.argv[1]) if len(sys.argv) > 1 else 5
    try:
        while True:
            cfg = load_json(os.path.join(HERE, "config.json"), {})
            providers = load_json(os.path.join(HERE, "providers.json"), [])
            render(cfg.get("local_api_key", ""), providers)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n退出监控。")


if __name__ == "__main__":
    main()
