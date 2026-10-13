# -*- coding: utf-8 -*-
"""模型网关监控 GUI v1：KPI 仪表盘设计（PySide6），全面参考 llama-cpp/llama/monitor_gui.py 框架。
- 顶部：标题 + 网关健康胶囊 + 时钟
- KPI 瓷砖行：渠道数 / 可用模型 / 今日调用 / 今日 token / 均延时 / 正在调用
- 卡片1：最近使用 5 模型（滚动；-> = 正在调用；实单健康）
- 卡片2：最近调用日志（最新在上）
- QSS 热重载 + 窗口几何持久化 + BAT 式拖框复制
数据层复用 monitor.py（API + usage.jsonl 解析逻辑同源，黑窗口不改继续可用）。
"""
import json
import os
import sys
import gc
import threading
import time
import datetime

from PySide6.QtCore import Qt, QTimer, QRect, QSize, QPoint
from PySide6.QtGui import QGuiApplication, QColor, QIcon, QPainter, QPixmap, QPen, QBrush, QRadialGradient
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                               QHBoxLayout, QLabel, QTableWidget,
                               QTableWidgetItem, QHeaderView, QFrame,
                               QPushButton, QRubberBand)

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import monitor as M  # 数据层同源：api()/tail_usage()/usage_stats()/fmt_* 等

CFG_FILE = os.path.join(HERE, "monitor-gui-cfg.json")
QSS_FILE = os.path.join(HERE, "monitor-gui.qss")

REVERSE_PROVIDERS = M.REVERSE_PROVIDERS


def load_qss():
    try:
        return open(QSS_FILE, encoding="utf-8").read()
    except OSError:
        return ""


# ---------------- 数据层（后台线程，1.5s 一轮） ----------------
def collect():
    cfg = M.load_json(os.path.join(HERE, "config.json"), {})
    providers = M.load_json(os.path.join(HERE, "providers.json"), [])
    key = cfg.get("local_api_key", "")
    d = {
        "n_providers": len(providers),
        "n_active": sum(1 for p in providers if p.get("models")),
        "error": "",
    }
    details = M.api("/api/model-details", key)
    stab = M.api("/api/stability", key)
    inflight = M.api("/api/inflight", key)
    inflight_set = {(x.get("provider"), x.get("model"))
                    for x in (inflight if isinstance(inflight, list) else [])}
    poll = M.api("/api/poll-status", key)
    if "__error__" in details:
        d["error"] = details["__error__"]
        d["ok"] = False
        return d
    d["ok"] = True

    stab_map = {(s.get("provider"), s.get("model")): s
                for s in (stab if isinstance(stab, list) else [])}
    qmap, bmap = M.build_group_map()
    specs = M.meta_specs()
    ustats = M.usage_stats()
    ctx_default = {"sensenova-6.8-flash-lite": 262144, "deepseek-ai/DeepSeek-V4-Pro": 1048576}

    rows = []
    for p in providers:
        for m in p.get("models", []):
            det = details.get(m, {})
            s = stab_map.get((p.get("name"), m), {})
            grps = M.groups_of(qmap, bmap, p.get("name"), m)
            u = ustats.get((p.get("name"), m), {"tokens": 0, "calls": 0})
            sp = specs.get(m) or {}
            rows.append({
                "name": p.get("name", "?"), "model": m,
                "groups": "/".join(grps) if grps else "-",
                "ctx": det.get("context_length") or ctx_default.get(m),
                "avail": s.get("availability"),
                "lat": s.get("avg_latency_ms"),
                "checks": s.get("checks", 0),
                "last": s.get("last_status", ""),
                "tokens": u["tokens"], "calls": u["calls"],
                "act": sp.get("active") or det.get("active_params_b") or 0,
                "tot": sp.get("total") or det.get("total_params_b") or 0,
            })

    # ---- 最近使用榜（滚动5），与 BAT 同规则 ----
    now_ts = time.time()
    live_models = {(p.get("name"), m) for p in providers for m in p.get("models", [])}
    logs_all = M.tail_usage(60)
    seen, recent5 = set(), []
    for rec in reversed(logs_all):
        k = (rec.get("provider"), rec.get("model"))
        if not k[1] or k in seen or k[0] in REVERSE_PROVIDERS or k not in live_models:
            continue
        seen.add(k)
        recent5.append((k, rec.get("ts", 0), rec.get("req")))
        if len(recent5) >= 5:
            break
    row_map = {(r["name"], r["model"]): r for r in rows}
    real_durs = {}
    for rec in logs_all:
        k = (rec.get("provider"), rec.get("model"))
        if now_ts - rec.get("ts", 0) < 1800 and rec.get("dur") is not None:
            real_durs.setdefault(k, []).append(rec["dur"])

    out5 = []
    for (pn, mn), last_ts, last_req in recent5:
        r = row_map.get((pn, mn)) or {"name": pn, "model": mn, "groups": "-",
                                      "ctx": None, "avail": None, "lat": None, "last": "",
                                      "tokens": ustats.get((pn, mn), {}).get("tokens", 0),
                                      "act": None, "tot": None}
        if last_req in ("coding", "transcribe", "proxy"):
            r = dict(r)
            r["groups"] = last_req
        durs = real_durs.get((pn, mn)) or []
        if now_ts - last_ts < 600:
            status = "正常(实单)"
        else:
            status = {"ok": "正常", "fail": "故障"}.get(r["last"], "未检")
        out5.append({
            "busy": (pn, mn) in inflight_set,  # 只认网关在途登记, 不用"刚完成"启发式(会双亮)
            "full": (pn, mn),
            "name": r["name"], "model": mn.rsplit("/", 1)[-1],
            "groups": r["groups"],
            "spec": _spec_s(r["act"], r["tot"]),
            "tokens": r["tokens"], "ctx": r["ctx"],
            "avail": r["avail"],
            "lat": (sum(durs) / len(durs)) if durs else (r["lat"] / 1000 if r["lat"] else None),
            "status": status,
        })
    d["recent5"] = out5
    # 在途请求若不在最近完成榜里, 补到榜顶（网关 INFLIGHT 直读, 真在跑）
    listed = {x.get("full") or (x["name"], x["model"]) for x in out5}
    for _p, _m in sorted(inflight_set):
        if (_p, _m) in listed:
            continue
        r = row_map.get((_p, _m)) or {"ctx": None, "avail": None, "lat": None,
                                      "tokens": ustats.get((_p, _m), {}).get("tokens", 0),
                                      "act": None, "tot": None}
        out5.insert(0, {
            "busy": True, "name": _p, "model": _m.rsplit("/", 1)[-1],
            "groups": M.groups_of(qmap, bmap, _p, _m) and "/".join(M.groups_of(qmap, bmap, _p, _m)) or "-",
            "spec": _spec_s(r["act"], r["tot"]), "tokens": r["tokens"], "ctx": r["ctx"],
            "avail": r["avail"], "lat": None, "status": "进行中",
        })
    d["logs"] = list(reversed(logs_all[-14:]))  # 最新在上

    # ---- KPI ----
    today0 = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    t_calls = t_toks = 0
    durs_recent = []
    for rec in logs_all:
        if rec.get("ts", 0) >= today0:
            t_calls += 1
            t_toks += (rec.get("pt", 0) or 0) + (rec.get("ct", 0) or 0)
        if now_ts - rec.get("ts", 0) < 1800 and rec.get("dur") is not None:
            durs_recent.append(rec["dur"])
    d["today_calls"] = t_calls
    d["today_tokens"] = t_toks
    d["avg_dur"] = (sum(durs_recent) / len(durs_recent)) if durs_recent else None
    d["inflight"] = sum(1 for x in out5 if x["busy"])
    if isinstance(poll, dict) and "total_models" in poll:
        d["poll_total"] = poll.get("total_models")
    return d


def _spec_s(act, tot):
    def fp(v):
        if v is None:
            return "未"
        return f"{v/1000:g}T" if v >= 1000 else f"{v:g}B"
    return f"{fp(act)}/{fp(tot)}" if (act or tot) else "未收录"


def fmt_dur(v):
    return f"{v:.1f}s" if v is not None else "-"


# ---------------- UI 部件 ----------------
def kpi_tile(caption):
    f = QFrame(); f.setProperty("class", "tile")
    f.setAttribute(Qt.WA_StyledBackground, True)
    v = QVBoxLayout(f); v.setContentsMargins(12, 8, 12, 8); v.setSpacing(2)
    cap = QLabel(caption); cap.setProperty("class", "cap"); cap.setAlignment(Qt.AlignHCenter)
    val = QLabel("-"); val.setProperty("class", "kpi"); val.setAlignment(Qt.AlignHCenter)
    v.addWidget(cap); v.addWidget(val)
    return f, val


def _app_icon() -> QIcon:
    """内存绘制应用图标：深色圆角方块 + 蓝色网关双向通路，无外部文件依赖"""
    icon = QIcon()
    for sz in (16, 24, 32, 48, 64, 128, 256):
        pm = QPixmap(sz, sz); pm.fill(Qt.transparent)
        p = QPainter(pm); p.setRenderHint(QPainter.Antialiasing)
        m = max(1, sz // 16)
        # 底板：深色圆角方块 + 微径向光
        grad = QRadialGradient(sz * 0.35, sz * 0.3, sz * 0.9)
        grad.setColorAt(0, QColor("#232a3d")); grad.setColorAt(1, QColor("#0e0f13"))
        p.setBrush(QBrush(grad)); p.setPen(QPen(QColor("#2c3448"), m))
        p.drawRoundedRect(m, m, sz - 2 * m, sz - 2 * m, sz // 5, sz // 5)
        # 两条通路（左进右出）+ 中心节点
        blue, green = QColor("#4f8cff"), QColor("#43d17c")
        pen = QPen(blue, max(1, sz // 10), Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin)
        p.setPen(pen)
        y1, y2 = sz * 0.36, sz * 0.64
        ah = sz // 7  # 箭头尺寸
        p.drawLine(int(sz * 0.16), int(y1), int(sz * 0.84), int(y1))
        p.drawLine(int(sz * 0.84) - ah, int(y1) - ah, int(sz * 0.84), int(y1))
        p.drawLine(int(sz * 0.84) - ah, int(y1) + ah, int(sz * 0.84), int(y1))
        p.setPen(QPen(green, max(1, sz // 10), Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin))
        p.drawLine(int(sz * 0.84), int(y2), int(sz * 0.16), int(y2))
        p.drawLine(int(sz * 0.16) + ah, int(y2) - ah, int(sz * 0.16), int(y2))
        p.drawLine(int(sz * 0.16) + ah, int(y2) + ah, int(sz * 0.16), int(y2))
        p.setPen(Qt.NoPen); p.setBrush(QColor("#d4d7e0"))
        p.drawEllipse(QRect(int(sz * 0.46), int(sz * 0.44), max(2, sz // 12), max(2, sz // 12)))
        p.end()
        icon.addPixmap(pm)
    return icon


class Win(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("模型网关监控")
        self.setWindowIcon(_app_icon())
        self.resize(980, 640)
        self.setMinimumSize(640, 440)
        self.data = {}
        self.build()
        self.load_geometry()
        t = QTimer(self); t.timeout.connect(self.refresh); t.start(1500)
        tgc = QTimer(self); tgc.timeout.connect(lambda: gc.collect()); tgc.start(60000)
        self._init_dragcopy()
        threading.Thread(target=self.looper, daemon=True).start()

    # ---------- UI ----------
    def build(self):
        root = QWidget(); root.setObjectName("root"); self.setCentralWidget(root)
        col = QVBoxLayout(root); col.setContentsMargins(12, 12, 12, 12); col.setSpacing(10)

        head = QHBoxLayout(); head.setSpacing(10)
        self.lb_title = QLabel("模型网关监控"); self.lb_title.setProperty("class", "big")
        head.addWidget(self.lb_title); head.addStretch(1)
        self.lb_health = QLabel("…"); self.lb_health.setProperty("class", "pill_ok")
        self.lb_clock = QLabel(); self.lb_clock.setProperty("class", "dim")
        head.addWidget(self.lb_health); head.addWidget(self.lb_clock)
        col.addLayout(head)

        kpis = QHBoxLayout(); kpis.setSpacing(8)
        tiles = [("渠道"), ("可用模型"), ("今日调用"), ("今日 Token"), ("近期均延时"), ("正在调用")]
        self.kpi_vals = []
        for cap in tiles:
            f, val = kpi_tile(cap)
            kpis.addWidget(f, stretch=1)
            self.kpi_vals.append(val)
        col.addLayout(kpis)

        # 卡片1：最近使用 5 模型
        c1, v1 = self._card()
        cap = QLabel("最近使用 5 模型（滚动 · -> = 正在调用 · 实单健康优先）"); cap.setProperty("class", "cap")
        v1.addWidget(cap)
        self.tbl5 = self._table(["渠道", "模型", "分组", "参数(激/总)", "近7天",
                                 "上下文", "可用率", "延时", "状态"])
        v1.addWidget(self.tbl5)
        col.addWidget(c1, stretch=3)

        # 卡片2：最近调用日志
        c2, v2 = self._card()
        cap = QLabel("最近调用日志"); cap.setProperty("class", "cap")
        v2.addWidget(cap)
        self.tbllog = self._table(["时间", "渠道", "模型", "分组", "输入", "输出", "延时"])
        v2.addWidget(self.tbllog)
        col.addWidget(c2, stretch=4)

        self.lb_toast = QLabel(""); self.lb_toast.setProperty("class", "toast")
        col.addWidget(self.lb_toast)

    def _card(self):
        f = QFrame(); f.setProperty("class", "card")
        f.setAttribute(Qt.WA_StyledBackground, True)
        v = QVBoxLayout(f); v.setContentsMargins(12, 8, 12, 10); v.setSpacing(6)
        return f, v

    def _table(self, headers):
        t = QTableWidget(0, len(headers))
        t.setAlternatingRowColors(True)
        t.setHorizontalHeaderLabels(headers)
        t.verticalHeader().setVisible(False)
        t.verticalHeader().setDefaultSectionSize(20)
        t.setEditTriggers(QTableWidget.NoEditTriggers)
        # 列宽可拖拽调整(Interactive); 双击表头某列 → 该列自适应内容宽度
        hh = t.horizontalHeader()
        hh.setSectionResizeMode(QHeaderView.Interactive)
        hh.setSectionsMovable(False)
        hh.sectionDoubleClicked.connect(lambda col, _t=t: _t.resizeColumnToContents(col))
        t._fitted = False
        t.setSelectionBehavior(QTableWidget.SelectRows)
        return t

    # ---------- BAT 式拖框复制 ----------
    def _init_dragcopy(self):
        QApplication.instance().installEventFilter(self)
        self._sel_origin = None
        self._sel_global = None
        self._rb = QRubberBand(QRubberBand.Rectangle, self.centralWidget())

    def _interactive(self, w):
        # 表格不再豁免：拖框可从任意位置起手（点击不动仍可正常选中单元格）。
        # 表头(QHeaderView)豁免：拖拽列宽/双击自适应是表头自己的手势, 不能被框选抢走
        while w is not None and w is not self:
            if isinstance(w, (QPushButton, QHeaderView)):
                return True
            w = w.parentWidget()
        return False

    def eventFilter(self, obj, ev):
        from PySide6.QtCore import QEvent
        t = ev.type()
        if t == QEvent.MouseButtonPress and ev.button() == Qt.LeftButton:
            gp = ev.globalPosition().toPoint()
            w = QApplication.widgetAt(gp)
            if w is not None and self.isAncestorOf(w) and not self._interactive(w):
                root = self.centralWidget()
                self._sel_global = gp
                self._sel_origin = root.mapFromGlobal(gp)
                self._rb.setGeometry(QRect(self._sel_origin, QSize(0, 0)))
                self._rb.show()
        elif t == QEvent.MouseMove and self._sel_global is not None:
            root = self.centralWidget()
            p = root.mapFromGlobal(ev.globalPosition().toPoint())
            x, y = min(self._sel_origin.x(), p.x()), min(self._sel_origin.y(), p.y())
            self._rb.setGeometry(QRect(x, y, abs(p.x() - self._sel_origin.x()),
                                       abs(p.y() - self._sel_origin.y())))
        elif t == QEvent.MouseButtonRelease and self._sel_global is not None:
            g = self._rb.geometry()
            self._rb.hide()
            self._sel_global = None
            if g.width() > 6 and g.height() > 6:
                self._copy_selected_text(g)
        return super().eventFilter(obj, ev)

    def _toast(self, msg):
        self.lb_toast.setText(msg)
        QTimer.singleShot(3000, lambda: self.lb_toast.setText(""))

    def _copy_selected_text(self, sel):
        """框选范围内的 QLabel 文字 + 表格单元格，从上到下、从左到右汇总进剪贴板。
        表格按行聚合，同行单元格用制表符连接（粘进 Excel/文本都是整行）。"""
        root = self.centralWidget()
        items = []  # (y, x, is_label, 行key, text)
        for lab in root.findChildren(QLabel):
            if not lab.text().strip():
                continue
            lg = QRect(lab.mapTo(root, lab.rect().topLeft()), lab.size())
            if lg.intersects(sel):
                items.append((lg.y(), lg.x(), True, id(lab), lab.text().strip()))
        for tbl in (self.tbl5, self.tbllog):
            vp = tbl.viewport()
            # 表头列标题也纳入框选复制(按列位置逐个收, 与单元格同序)
            hh = tbl.horizontalHeader()
            hh_y = hh.mapTo(root, QPoint(0, 0)).y()
            for col in range(tbl.columnCount()):
                if tbl.columnWidth(col) <= 0 or not tbl.horizontalHeaderItem(col):
                    continue
                sec = hh.sectionViewportPosition(col)
                hg = QRect(hh.mapTo(root, QPoint(sec, 0)), QSize(hh.sectionSize(col), hh.height()))
                if hg.intersects(sel):
                    items.append((hh_y, hg.x(), True, f"hh{id(tbl)}", tbl.horizontalHeaderItem(col).text()))
            for row in range(tbl.rowCount()):
                row_cells = []
                row_y = None
                for col in range(tbl.columnCount()):
                    it = tbl.item(row, col)
                    if it is None:
                        continue
                    rect = tbl.visualItemRect(it)
                    g_rect = QRect(vp.mapTo(root, rect.topLeft()), rect.size())
                    if g_rect.intersects(sel):
                        row_cells.append((g_rect.x(), it.text()))
                        row_y = g_rect.y() if row_y is None else row_y
                if row_cells:
                    row_cells.sort()
                    items.append((row_y, row_cells[0][0], False,
                                  f"{id(tbl)}:{row}", "\t".join(t for _, t in row_cells)))
        items.sort(key=lambda x: (x[0], x[1]))
        lines, last_key = [], None
        for y, x, is_label, key, text in items:
            if not is_label and key == last_key:
                continue
            if not is_label:
                last_key = key
            lines.append(text)
        text = "\n".join(lines)
        if text:
            QGuiApplication.clipboard().setText(text)
            self._toast(f"✓ 框选内容已复制（{len(lines)} 行，Ctrl+V 粘贴）")

    # ---------- 持久化 / QSS 热重载 ----------
    def load_geometry(self):
        try:
            cfg = json.load(open(CFG_FILE, encoding="utf-8"))
            g = cfg.get("geometry")
            if g and len(g) == 4:
                x, y, w, h = g
                self.setGeometry(x, y, max(w, 640), max(h, 440))
        except Exception:
            pass
        self._save_timer = QTimer(self, singleShot=True, interval=500)
        self._save_timer.timeout.connect(self.save_geometry)
        self._qss_mtime = 0
        tq = QTimer(self, interval=1000)
        tq.timeout.connect(self.hot_reload_qss)
        tq.start()

    def hot_reload_qss(self):
        try:
            mt = os.path.getmtime(QSS_FILE)
        except OSError:
            return
        if mt != self._qss_mtime:
            self._qss_mtime = mt
            QApplication.instance().setStyleSheet(load_qss())

    def save_geometry(self):
        try:
            g = self.geometry()
            json.dump({"geometry": [g.x(), g.y(), g.width(), g.height()]},
                      open(CFG_FILE, "w", encoding="utf-8"))
        except Exception:
            pass

    def moveEvent(self, ev):
        self._save_timer.start(); super().moveEvent(ev)

    def resizeEvent(self, ev):
        self._save_timer.start(); super().resizeEvent(ev)

    def closeEvent(self, ev):
        self.save_geometry(); super().closeEvent(ev)

    # ---------- 数据 ----------
    def looper(self):
        while True:
            try:
                self.data = collect()
            except Exception:
                pass
            time.sleep(2.0)  # 网关慢时别空转堆积; 4s超时+2s间隔=最坏每轮6s, 不挤爆主线程

    # ---------- 渲染 ----------
    def refresh(self):
        d = self.data
        if not d:
            return
        self.lb_clock.setText(datetime.datetime.now().strftime("%m-%d %H:%M:%S"))
        if not d.get("ok"):
            self.lb_health.setText("网关不可达")
            self._set_pill("pill_bad")
            self.lb_toast.setText(f"⚠ {d.get('error', '')[:120]}")
            return
        if self.lb_toast.text().startswith("⚠"):
            self.lb_toast.setText("")

        n_bad = sum(1 for x in d["recent5"] if x["status"] == "故障")
        if n_bad == 0:
            self.lb_health.setText("服务正常")
            self._set_pill("pill_ok")
        else:
            self.lb_health.setText(f"{n_bad} 路故障")
            self._set_pill("pill_bad")

        # KPI
        v = self.kpi_vals
        v[0].setText(f"{d['n_providers']}")
        v[1].setText(f"{d['n_active']}")
        v[2].setText(f"{d['today_calls']}")
        v[3].setText(M.fmt_tokens(d["today_tokens"]))
        v[4].setText(fmt_dur(d.get("avg_dur")))
        v[5].setText(f"{d['inflight']}")

        # 最近使用5
        rows5 = d["recent5"]
        self.tbl5.setRowCount(len(rows5))
        for i, r in enumerate(rows5):
            vals = [r["name"], r["model"],
                    r["groups"], r["spec"], M.fmt_tokens(r["tokens"]),
                    M.fmt_ctx(r["ctx"]),
                    f"{r['avail']:.0f}%" if r["avail"] is not None else "-",
                    fmt_dur(r["lat"]),
                    r["status"]]
            self._fill_row(self.tbl5, i, vals, status=r["status"], busy=r["busy"])

        # 日志
        qmap, bmap = M.build_group_map()
        logs = d["logs"]
        self.tbllog.setRowCount(len(logs))
        for i, rec in enumerate(logs):
            m_s = str(rec.get("model", "-")).rsplit("/", 1)[-1]
            req = rec.get("req")
            g_s = req if req in ("coding", "transcribe", "proxy", "all") else (
                "/".join(M.groups_of(qmap, bmap, rec.get("provider", ""), m_s)) or "-")
            vals = [time.strftime("%H:%M:%S", time.localtime(rec.get("ts", 0))),
                    rec.get("provider", "-"), m_s, g_s,
                    M.fmt_tokens(rec.get("pt", 0)), M.fmt_tokens(rec.get("ct", 0)),
                    fmt_dur(rec.get("dur"))]
            self._fill_row(self.tbllog, i, vals)
        # 首次有数据时整体自适应一次作为初始列宽(之后列宽归用户, 双击列头可再单列自适应)
        for _t in (self.tbl5, self.tbllog):
            if not _t._fitted and _t.rowCount() > 0:
                _t.resizeColumnsToContents()
                _t._fitted = True

    def _fill_row(self, tbl, i, vals, status=None, busy=False):
        for j, s in enumerate(vals):
            it = QTableWidgetItem(str(s))
            if busy:  # 正在调用：整行高亮（GUI 语义，不再用箭头）
                it.setBackground(QColor("#22304f"))
                it.setForeground(QColor("#dbe6ff"))
            elif j == len(vals) - 1:  # 状态列（最右）
                if status == "故障":
                    it.setForeground(QColor("#ff7b7b"))
                elif status and "正常" in status:
                    it.setForeground(QColor("#43d17c"))
                else:
                    it.setForeground(QColor("#aab1c0"))
            tbl.setItem(i, j, it)

    def _set_pill(self, cls):
        self.lb_health.setProperty("class", cls)
        self.lb_health.style().unpolish(self.lb_health)
        self.lb_health.style().polish(self.lb_health)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName("模型网关监控")
    app.setWindowIcon(_app_icon())
    app.setStyleSheet(load_qss())
    w = Win(); w.show()
    sys.exit(app.exec())
