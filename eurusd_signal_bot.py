#!/usr/bin/env python3
# EURUSD SIGNAL BOT v1.0 — direction caller for Pocket Option DEMO testing
#
# WHAT IT DOES
#   Every minute (during real forex hours only — OTC deliberately excluded), at
#   SIGNAL_LEAD_SECS before the minute boundary (default 5s, e.g. 09:59:55 for a
#   10:00:00 entry), it computes a battery of features on live EUR/USD and sends
#   a Telegram signal: UP or DOWN for a HORIZON_MINS expiry (default 5m).
#   Then it SCORES ITSELF: at entry time it snapshots the price, at expiry it
#   checks the outcome, and every result message carries the running accuracy vs
#   the break-even line for your payout. Per-feature accuracy is tracked too
#   (/features), so after a few hundred signals you can see which inputs — if
#   any — actually carry signal. This is the same instrument-first discipline as
#   the Polymarket bots: measure first, believe the scoreboard only.
#
# PRICE SOURCE
#   Binance EURUSDT (EUR vs Tether) via free push websocket — bookTicker mid as
#   the price, trade stream for order-flow features. USDT ~ USD, so it tracks
#   interbank EUR/USD within fractions of a pip; for 5-minute DIRECTION the two
#   are effectively identical. It's free, keyless, push, and battle-tested in
#   the other bots. (Upgrade path: a paid FX websocket like TraderMade/EODHD if
#   the demo test ever earns it.) Signals are gated to real forex hours so
#   you're never comparing against Pocket Option's synthetic OTC feed.
#
# HONEST NOTE
#   Break-even at payout p is 1/(1+p): 54.1% at 85%, 52.1% at 92%, 57.1% at 75%.
#   Sustained accuracy above that on 5-minute EUR/USD is one of the hardest
#   problems in trading. This bot exists to MEASURE whether any of these
#   features clear the bar — on demo money — not to promise that they do.
#
# ENV (required): TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
# ENV (optional): HORIZON_MINS=5, SIGNAL_LEAD_SECS=5, SIGNAL_MIN_SCORE=0.15,
#   PAYOUT_PCT=0.85, SEND_SKIPS=true, DB_PATH=eurusd_signals.db,
#   SIGNAL_EVERY_MINS=1, HEARTBEAT_SECS=60

import os
import time
import json
import sqlite3
import logging
import threading
import statistics
import requests
from datetime import datetime, timezone
from collections import deque

try:
    import websocket  # websocket-client
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

# ─── ENV / CONFIG ─────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

HORIZON_MINS      = int(os.environ.get("HORIZON_MINS", "5"))
SIGNAL_EVERY_MINS = int(os.environ.get("SIGNAL_EVERY_MINS", "1"))
SIGNAL_LEAD_SECS  = float(os.environ.get("SIGNAL_LEAD_SECS", "5"))
SIGNAL_MIN_SCORE  = float(os.environ.get("SIGNAL_MIN_SCORE", "0.15"))
PAYOUT_PCT        = float(os.environ.get("PAYOUT_PCT", "0.85"))
SEND_SKIPS        = os.environ.get("SEND_SKIPS", "true").lower() == "true"
DB_PATH           = os.environ.get("DB_PATH", "eurusd_signals.db")
HEARTBEAT_SECS    = int(os.environ.get("HEARTBEAT_SECS", "60"))

BREAK_EVEN = 1.0 / (1.0 + PAYOUT_PCT)  # win-rate needed to profit at this payout

# Binance's PUBLIC market-data domain (binance.vision) — same data as
# binance.com but not geo-restricted, so it works from any Railway region.
BINANCE_WS_URL = ("wss://data-stream.binance.vision/stream?streams="
                  "eurusdt@bookTicker/eurusdt@trade")
BINANCE_KLINES = "https://data-api.binance.vision/api/v3/klines"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler()])
log = logging.getLogger("eurusd-signal")

state = {
    "paused": False,
    "market_open_notified": None,   # last open/closed state we told the user about
    "last_msg_time": time.time(),
}
update_offset = None

# ─── LIVE DATA BUFFERS ───────────────────────────────────────────────────────
TICK_KEEP_SECS  = 7200   # 2h of mid ticks (enough for every lookback)
TRADE_KEEP_SECS = 1800   # 30m of trades (order-flow features)

_ticks  = deque()  # (ts, mid)
_trades = deque()  # (ts, price, qty, is_buyer_maker)
_data_lock = threading.Lock()
last_tick_time = 0.0

# finalized 1-minute bars: {minute_ts: [o, h, l, c]}
_bars = {}
_cur_minute = None


def _push_tick(ts, mid):
    global _cur_minute, last_tick_time
    with _data_lock:
        _ticks.append((ts, mid))
        cutoff = ts - TICK_KEEP_SECS
        while _ticks and _ticks[0][0] < cutoff:
            _ticks.popleft()
        # bar building
        mts = int(ts // 60) * 60
        bar = _bars.get(mts)
        if bar is None:
            _bars[mts] = [mid, mid, mid, mid]
            # prune bars older than 2h
            old = mts - TICK_KEEP_SECS
            for k in [k for k in _bars if k < old]:
                del _bars[k]
        else:
            bar[1] = max(bar[1], mid)
            bar[2] = min(bar[2], mid)
            bar[3] = mid
    last_tick_time = ts


def _push_trade(ts, price, qty, is_buyer_maker):
    with _data_lock:
        _trades.append((ts, price, qty, is_buyer_maker))
        cutoff = ts - TRADE_KEEP_SECS
        while _trades and _trades[0][0] < cutoff:
            _trades.popleft()


def mid_now():
    with _data_lock:
        return _ticks[-1][1] if _ticks else None


def mid_at_or_before(ts):
    """Newest tick at or before ts (for momentum lookbacks)."""
    with _data_lock:
        best = None
        for t, p in reversed(_ticks):
            if t <= ts:
                best = p
                break
        return best


def closes_1m(n):
    """Last n FINALIZED 1m closes (excludes the still-forming current minute)."""
    now_min = int(time.time() // 60) * 60
    with _data_lock:
        keys = sorted(k for k in _bars if k < now_min)
        return [_bars[k][3] for k in keys[-n:]]


def bars_recent(minutes):
    now_min = int(time.time() // 60) * 60
    with _data_lock:
        keys = sorted(k for k in _bars if now_min - minutes * 60 <= k < now_min)
        return [_bars[k][:] for k in keys]


def seed_history():
    """Backfill ~2h of 1m closes from Binance REST so indicators are warm at
    boot instead of needing a 40-minute live warmup. Fail-safe: on any error the
    bot just warms up live."""
    try:
        r = requests.get(BINANCE_KLINES,
                         params={"symbol": "EURUSDT", "interval": "1m", "limit": 120},
                         timeout=10)
        rows = r.json()
        if not isinstance(rows, list):
            raise ValueError(f"unexpected klines response: {str(rows)[:120]}")
        n = 0
        with _data_lock:
            for k in rows:
                if not isinstance(k, (list, tuple)) or len(k) < 5:
                    continue
                ots = int(int(k[0]) // 1000)
                o, h, l, c = float(k[1]), float(k[2]), float(k[3]), float(k[4])
                _bars[ots] = [o, h, l, c]
                _ticks.append((ots + 59, c))  # synthetic tick at bar close
                n += 1
        log.info(f"[SEED] backfilled {n} 1m bars from Binance REST")
    except Exception as e:
        log.warning(f"[SEED] backfill failed (will warm up live): {e}")


def binance_ws_worker():
    """Persistent push feed: bookTicker (mid price) + trades (order flow)."""
    while True:
        ws = None
        try:
            ws = websocket.create_connection(BINANCE_WS_URL, timeout=10)
            ws.settimeout(30)
            log.info("[Binance WS] connected (eurusdt bookTicker+trade)")
            while True:
                msg = ws.recv()
                if not msg:
                    continue
                d = json.loads(msg)
                stream = d.get("stream", "")
                data = d.get("data", {})
                now = time.time()
                if stream.endswith("bookTicker"):
                    b, a = float(data.get("b", 0)), float(data.get("a", 0))
                    if b > 0 and a > 0:
                        _push_tick(now, (b + a) / 2.0)
                elif stream.endswith("trade"):
                    p = float(data.get("p", 0))
                    q = float(data.get("q", 0) or 0)
                    if p > 0:
                        _push_trade(now, p, q, bool(data.get("m", False)))
        except Exception as e:
            log.warning(f"[Binance WS] error: {e} — reconnecting in 3s")
        finally:
            try:
                if ws:
                    ws.close()
            except Exception:
                pass
        time.sleep(3)


# ─── FOREX HOURS (live pair only — OTC excluded by design) ───────────────────
def is_forex_open(dt_utc=None):
    """Real EUR/USD trades ~Sun 21:10 UTC to Fri 21:00 UTC. Outside that window
    Pocket Option only offers the OTC synthetic — which this bot deliberately
    never signals on (their in-house feed can't be predicted from market data).
    Approximate boundaries; holidays not modeled."""
    dt = dt_utc or datetime.now(timezone.utc)
    wd, hm = dt.weekday(), dt.hour * 60 + dt.minute  # Mon=0..Sun=6
    if wd == 5:                       # Saturday
        return False
    if wd == 4 and hm >= 21 * 60:     # Friday from 21:00
        return False
    if wd == 6 and hm < 21 * 60 + 10: # Sunday before 21:10
        return False
    return True


# ─── INDICATORS (pure python, no deps) ───────────────────────────────────────
def ema(vals, n):
    if len(vals) < n:
        return None
    k = 2.0 / (n + 1)
    e = sum(vals[:n]) / n
    for v in vals[n:]:
        e = v * k + e * (1 - k)
    return e


def rsi(vals, n=14):
    if len(vals) < n + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(vals)):
        d = vals[i] - vals[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    ag = sum(gains[:n]) / n
    al = sum(losses[:n]) / n
    for i in range(n, len(gains)):
        ag = (ag * (n - 1) + gains[i]) / n
        al = (al * (n - 1) + losses[i]) / n
    if al == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + ag / al)


def macd_hist(vals):
    if len(vals) < 35:
        return None
    e12 = _ema_series(vals, 12)
    e26 = _ema_series(vals, 26)
    macd_line = [a - b for a, b in zip(e12[-len(e26):], e26)]
    sig = ema(macd_line, 9)
    if sig is None:
        return None
    return macd_line[-1] - sig


def _ema_series(vals, n):
    k = 2.0 / (n + 1)
    out = []
    e = sum(vals[:n]) / n
    out.append(e)
    for v in vals[n:]:
        e = v * k + e * (1 - k)
        out.append(e)
    return out


def boll_pct_b(vals, n=20, sd=2.0):
    if len(vals) < n:
        return None
    win = vals[-n:]
    m = sum(win) / n
    s = statistics.pstdev(win)
    if s == 0:
        return None
    upper, lower = m + sd * s, m - sd * s
    return (vals[-1] - lower) / (upper - lower)


# ─── FEATURE ENGINE ──────────────────────────────────────────────────────────
def build_features():
    """Compute every feature and its directional vote at this instant.
    Returns (features_dict, score, direction, mid). Each feature:
    {vote: -1/0/+1, weight, info}. Score = weighted vote sum / weight sum."""
    mid = mid_now()
    if mid is None:
        return None, 0.0, None, None
    now = time.time()
    F = {}

    def add(name, vote, weight, info):
        F[name] = {"vote": int(vote), "w": weight, "info": info}

    # momentum over multiple lookbacks (tick-based)
    for mins, wgt in [(1, 1.0), (3, 1.0), (5, 1.5), (15, 1.0)]:
        p0 = mid_at_or_before(now - mins * 60)
        if p0:
            momp = (mid - p0) / p0 * 100.0
            v = 1 if momp > 0.003 else (-1 if momp < -0.003 else 0)  # ~0.33 pip dead zone
            add(f"mom{mins}m", v, wgt, f"{momp:+.3f}%")
        else:
            add(f"mom{mins}m", 0, wgt, "—")

    closes = closes_1m(60)

    e9, e21 = ema(closes, 9), ema(closes, 21)
    if e9 and e21:
        add("ema9x21", 1 if e9 > e21 else (-1 if e9 < e21 else 0), 1.5,
            f"{'9>21' if e9 > e21 else '9<21'}")
        add("px_vs_ema21", 1 if mid > e21 else (-1 if mid < e21 else 0), 1.0,
            f"{(mid - e21) * 10000:+.1f}p")
    else:
        add("ema9x21", 0, 1.5, "warmup")
        add("px_vs_ema21", 0, 1.0, "warmup")

    r = rsi(closes, 14)
    if r is not None:
        if r >= 70:
            v = -1          # overbought → mean-revert vote
        elif r <= 30:
            v = 1
        elif abs(r - 50) >= 5:
            v = 1 if r > 50 else -1
        else:
            v = 0
        add("rsi14", v, 1.0, f"{r:.0f}")
    else:
        add("rsi14", 0, 1.0, "warmup")

    bb = boll_pct_b(closes, 20)
    if bb is not None:
        v = -1 if bb > 1.0 else (1 if bb < 0.0 else 0)   # only extremes vote (reversion)
        add("bollB", v, 1.0, f"{bb:.2f}")
    else:
        add("bollB", 0, 1.0, "warmup")

    mh = macd_hist(closes)
    if mh is not None:
        add("macd", 1 if mh > 0 else (-1 if mh < 0 else 0), 1.25, f"{mh * 10000:+.2f}")
    else:
        add("macd", 0, 1.25, "warmup")

    # candle streak (3 same-color finalized 1m candles → continuation)
    bars3 = bars_recent(4)
    if len(bars3) >= 3:
        cols = [1 if b[3] > b[0] else (-1 if b[3] < b[0] else 0) for b in bars3[-3:]]
        v = cols[0] if cols[0] != 0 and cols.count(cols[0]) == 3 else 0
        add("streak3", v, 0.75, f"{cols}")
    else:
        add("streak3", 0, 0.75, "warmup")

    # position in 15m range (breakout-momentum at the edges)
    b15 = bars_recent(15)
    if b15:
        hi = max(b[1] for b in b15)
        lo = min(b[2] for b in b15)
        if hi > lo:
            pos = (mid - lo) / (hi - lo)
            v = 1 if pos > 0.85 else (-1 if pos < 0.15 else 0)
            add("range15", v, 0.75, f"{pos:.2f}")
        else:
            add("range15", 0, 0.75, "flat")
    else:
        add("range15", 0, 0.75, "warmup")

    # taker order-flow imbalance, last 60s (from EURUSDT trades)
    with _data_lock:
        tr = [t for t in _trades if t[0] >= now - 60]
    buys = sum(q for (_, _, q, m) in tr if not m)   # taker buys
    sells = sum(q for (_, _, q, m) in tr if m)      # taker sells
    if buys + sells > 0:
        imb = (buys - sells) / (buys + sells)
        v = 1 if imb > 0.2 else (-1 if imb < -0.2 else 0)
        add("flow60s", v, 1.0, f"{imb:+.2f}")
    else:
        add("flow60s", 0, 1.0, "—")

    # realized 5m volatility (info only — no directional vote)
    with _data_lock:
        pts = [p for (t, p) in _ticks if t >= now - 300]
    rv = 0.0
    if len(pts) > 2:
        rv = (max(pts) - min(pts)) / mid * 100.0
    add("vol5m", 0, 0.0, f"{rv:.3f}%")

    wsum = sum(f["w"] for f in F.values() if f["vote"] != 0)
    vsum = sum(f["vote"] * f["w"] for f in F.values())
    tot_w = sum(f["w"] for f in F.values() if f["w"] > 0)
    score = (vsum / tot_w) if tot_w > 0 else 0.0
    direction = "UP" if score > 0 else ("DOWN" if score < 0 else None)
    return F, round(score, 3), direction, mid


# ─── DATABASE ────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created TEXT, entry_ts INTEGER, horizon_mins INTEGER,
        direction TEXT, score REAL, features TEXT,
        entry_price REAL, exit_price REAL,
        result TEXT, pips REAL, units REAL)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS feature_stats (
        name TEXT PRIMARY KEY, wins INTEGER, total INTEGER)""")
    # anything left PENDING from before a restart can't be scored honestly
    conn.execute("UPDATE signals SET result='VOID' WHERE result='PENDING'")
    conn.commit()
    conn.close()


def db_insert_signal(entry_ts, direction, score, features):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""INSERT INTO signals (created, entry_ts, horizon_mins, direction,
                 score, features, result) VALUES (?,?,?,?,?,?, 'PENDING')""",
              (datetime.now(timezone.utc).isoformat(), entry_ts, HORIZON_MINS,
               direction, score, json.dumps({k: v["vote"] for k, v in features.items()})))
    rid = c.lastrowid
    conn.commit()
    conn.close()
    return rid


def db_resolve(rid, entry_price, exit_price, result, pips, units):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""UPDATE signals SET entry_price=?, exit_price=?, result=?,
                    pips=?, units=? WHERE id=?""",
                 (entry_price, exit_price, result, pips, units, rid))
    conn.commit()
    conn.close()


def db_feature_tally(votes, won):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for name, v in votes.items():
        if v == 0:
            continue
        c.execute("INSERT INTO feature_stats (name,wins,total) VALUES (?,0,0) "
                  "ON CONFLICT(name) DO NOTHING", (name,))
        c.execute("UPDATE feature_stats SET wins=wins+?, total=total+1 WHERE name=?",
                  (1 if won else 0, name))
    conn.commit()
    conn.close()


def db_stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT result, units FROM signals WHERE result IN "
              "('WIN','LOSS','TIE') ORDER BY id DESC")
    rows = c.fetchall()
    conn.close()
    total = len(rows)
    wins = sum(1 for r, _ in rows if r == "WIN")
    ties = sum(1 for r, _ in rows if r == "TIE")
    units = sum(u or 0 for _, u in rows)
    decided = total - ties

    def wr(sub):
        d = [r for r, _ in sub if r != "TIE"]
        return (sum(1 for r in d if r == "WIN") / len(d) * 100) if d else None
    return {
        "total": total, "wins": wins, "ties": ties, "units": units,
        "wr_all": (wins / decided * 100) if decided else None,
        "wr_20": wr(rows[:20]), "wr_100": wr(rows[:100]),
    }


# ─── TELEGRAM ────────────────────────────────────────────────────────────────
def tg(msg):
    try:
        state["last_msg_time"] = time.time()
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                            "parse_mode": "HTML"}, timeout=8)
        log.info(f"[TG] {msg[:80]}")
    except Exception as e:
        log.error(f"TG error: {e}")


def get_updates():
    global update_offset
    try:
        params = {"timeout": 1, "allowed_updates": ["message"]}
        if update_offset:
            params["offset"] = update_offset
        res = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                           params=params, timeout=5)
        return res.json().get("result", [])
    except Exception:
        return []


def handle_commands():
    global update_offset
    for update in get_updates():
        update_offset = update["update_id"] + 1
        msg = update.get("message", {})
        text = msg.get("text", "").strip().lower()
        cid = str(msg.get("chat", {}).get("id", ""))
        if cid != str(TELEGRAM_CHAT_ID):
            continue
        if text == "/pause":
            state["paused"] = True
            tg("⏸ signals paused — /resume to continue (scoring of open signals continues)")
        elif text == "/resume":
            state["paused"] = False
            tg("▶️ signals resumed")
        elif text == "/stats":
            s = db_stats()
            wr = f"{s['wr_all']:.1f}%" if s["wr_all"] is not None else "—"
            w20 = f"{s['wr_20']:.0f}%" if s["wr_20"] is not None else "—"
            w100 = f"{s['wr_100']:.0f}%" if s["wr_100"] is not None else "—"
            verdict = ("ABOVE break-even ✅" if (s["wr_all"] or 0) / 100 > BREAK_EVEN
                       else "below break-even ⚠️")
            tg(f"📊 <b>EUR/USD {HORIZON_MINS}m signal scoreboard</b>\n"
               f"signals scored: {s['total']} (ties {s['ties']})\n"
               f"win rate: <b>{wr}</b> · last20 {w20} · last100 {w100}\n"
               f"break-even @{PAYOUT_PCT * 100:.0f}% payout: {BREAK_EVEN * 100:.1f}% — {verdict}\n"
               f"units P&L: {s['units']:+.1f} (1 unit = 1 stake)")
        elif text == "/features":
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT name, wins, total FROM feature_stats "
                      "WHERE total >= 5 ORDER BY 1.0*wins/total DESC")
            rows = c.fetchall()
            conn.close()
            if not rows:
                tg("no feature data yet (needs ≥5 scored votes per feature)")
            else:
                lines = [f"{n}: {w}/{t} ({w / t * 100:.0f}%)" for n, w, t in rows]
                tg("🔬 <b>per-feature accuracy</b> (when it voted)\n" + "\n".join(lines) +
                   f"\n\nbreak-even: {BREAK_EVEN * 100:.1f}%")
        elif text == "/help":
            tg("🤖 /stats — scoreboard\n/features — per-feature accuracy\n"
               "/pause /resume — signal flow")


# ─── PENDING SIGNALS + SCORER ────────────────────────────────────────────────
pending = []          # dicts: rid, entry_ts, deadline, direction, votes, entry_price
pending_lock = threading.Lock()


def scorer_worker():
    """Snapshots entry price at T0, scores at T0+horizon, tallies features,
    sends the result message with the running scoreboard."""
    while True:
        try:
            time.sleep(0.5)
            now = time.time()
            with pending_lock:
                items = list(pending)
            for s in items:
                if s["entry_price"] is None and now >= s["entry_ts"]:
                    m = mid_now()
                    if m is not None:
                        s["entry_price"] = m
                    elif now > s["entry_ts"] + 15:
                        s["dead"] = True  # feed gap at entry — void it
                if now >= s["deadline"]:
                    m = mid_now()
                    _resolve(s, m)
                    with pending_lock:
                        if s in pending:
                            pending.remove(s)
                elif s.get("dead"):
                    db_resolve(s["rid"], None, None, "VOID", None, None)
                    with pending_lock:
                        if s in pending:
                            pending.remove(s)
        except Exception as e:
            log.error(f"[SCORER] error: {e}")


def _resolve(s, exit_price):
    ep = s.get("entry_price")
    if ep is None or exit_price is None:
        db_resolve(s["rid"], ep, exit_price, "VOID", None, None)
        log.warning(f"[SCORER] VOID signal {s['rid']} (missing price)")
        return
    pips = (exit_price - ep) * 10000
    if abs(pips) < 0.01:
        result, units = "TIE", 0.0   # Pocket Option refunds stakes on an exact tie
    else:
        went_up = exit_price > ep
        won = (s["direction"] == "UP") == went_up
        result = "WIN" if won else "LOSS"
        units = PAYOUT_PCT if won else -1.0
        db_feature_tally(s["votes"], won)
    db_resolve(s["rid"], ep, exit_price, result, round(pips, 1), units)

    st = db_stats()
    emoji = "✅" if result == "WIN" else ("❌" if result == "LOSS" else "➖")
    wr = f"{st['wr_all']:.1f}%" if st["wr_all"] is not None else "—"
    w20 = f"{st['wr_20']:.0f}%" if st["wr_20"] is not None else "—"
    t = datetime.fromtimestamp(s["entry_ts"], tz=timezone.utc).strftime("%H:%M")
    tg(f"{emoji} EUR/USD {s['direction']} {t} · "
       f"{ep:.5f}→{exit_price:.5f} ({pips:+.1f}p) <b>{result}</b>\n"
       f"📊 last20 {w20} · all {st['total']}: {wr} "
       f"(BE {BREAK_EVEN * 100:.1f}%) · units {st['units']:+.1f}")


# ─── SIGNAL LOOP ─────────────────────────────────────────────────────────────
def fmt_reasons(F):
    """Compact reason string from the loudest features."""
    bits = []
    for name in ("mom5m", "ema9x21", "rsi14", "macd", "flow60s"):
        f = F.get(name)
        if not f:
            continue
        arrow = "↑" if f["vote"] > 0 else ("↓" if f["vote"] < 0 else "·")
        bits.append(f"{name}{arrow}{f['info']}")
    return " · ".join(bits)


def signal_loop():
    while True:
        try:
            now = time.time()
            # next entry boundary on the SIGNAL_EVERY_MINS grid
            step = SIGNAL_EVERY_MINS * 60
            t0 = (int(now // step) + 1) * step
            wake = t0 - SIGNAL_LEAD_SECS
            if wake > now:
                time.sleep(min(wake - now, 5))
                if time.time() < wake:
                    continue

            open_now = is_forex_open()
            if state["market_open_notified"] != open_now:
                state["market_open_notified"] = open_now
                if open_now:
                    tg("🌅 forex open — live EUR/USD signals running")
                else:
                    tg("🌙 forex closed — live EUR/USD resumes Sun ~21:10 UTC.\n"
                       "No signals until then (OTC deliberately excluded — "
                       "that feed is the broker's own synthetic).")
            if not open_now or state["paused"]:
                time.sleep(max(0.5, t0 - time.time() + 0.5))
                continue

            if time.time() - last_tick_time > 20:
                log.warning("[SIGNAL] feed stale — skipping this minute")
                time.sleep(max(0.5, t0 - time.time() + 0.5))
                continue

            F, score, direction, mid = build_features()
            if F is None:
                time.sleep(max(0.5, t0 - time.time() + 0.5))
                continue

            t0_str = datetime.fromtimestamp(t0, tz=timezone.utc).strftime("%H:%M:%S")
            if direction is None or abs(score) < SIGNAL_MIN_SCORE:
                log.info(f"[SIGNAL] {t0_str} skip (score {score:+.3f})")
                if SEND_SKIPS:
                    tg(f"⚪ EUR/USD {t0_str} — mixed signals (score {score:+.2f}) · skip")
            else:
                arrow = "⬆️" if direction == "UP" else "⬇️"
                votes = {k: v["vote"] for k, v in F.items()}
                rid = db_insert_signal(int(t0), direction, score, F)
                with pending_lock:
                    pending.append({"rid": rid, "entry_ts": t0,
                                    "deadline": t0 + HORIZON_MINS * 60,
                                    "direction": direction, "votes": votes,
                                    "entry_price": None})
                tg(f"🔮 <b>EUR/USD {arrow} {direction}</b>\n"
                   f"enter <b>{t0_str} UTC</b> · {HORIZON_MINS}m expiry\n"
                   f"confidence {abs(score) * 100:.0f}% · spot {mid:.5f}\n"
                   f"{fmt_reasons(F)}")
                log.info(f"[SIGNAL] {t0_str} {direction} score {score:+.3f} mid {mid:.5f}")

            # sleep past this boundary so we don't double-fire
            time.sleep(max(0.5, t0 - time.time() + 1.0))
        except Exception as e:
            log.error(f"[SIGNAL] loop error: {e}")
            time.sleep(2)


# ─── MAIN ────────────────────────────────────────────────────────────────────
def main():
    if not WEBSOCKET_AVAILABLE:
        log.error("websocket-client not installed")
        return
    init_db()
    seed_history()
    threading.Thread(target=binance_ws_worker, daemon=True).start()
    threading.Thread(target=scorer_worker, daemon=True).start()
    threading.Thread(target=signal_loop, daemon=True).start()

    tg(f"🔮 <b>EUR/USD signal bot LIVE</b>\n"
       f"{HORIZON_MINS}m direction call every {SIGNAL_EVERY_MINS}m, "
       f"sent {SIGNAL_LEAD_SECS:.0f}s before entry\n"
       f"break-even @{PAYOUT_PCT * 100:.0f}% payout: <b>{BREAK_EVEN * 100:.1f}%</b> — "
       f"the scoreboard decides\n"
       f"feed: Binance EURUSDT (live hours only, OTC excluded)\n"
       f"/stats · /features · /pause")

    hb_last = 0
    while True:
        try:
            handle_commands()
            now = time.time()
            if HEARTBEAT_SECS > 0 and now - hb_last >= HEARTBEAT_SECS:
                hb_last = now
                m = mid_now()
                age = now - last_tick_time if last_tick_time else -1
                with pending_lock:
                    np_ = len(pending)
                log.info(f"[Heartbeat] mid={m} age={age:.0f}s bars={len(_bars)} "
                         f"pending={np_} open={is_forex_open()}")
        except Exception as e:
            log.error(f"main loop error: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
