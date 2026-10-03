#!/usr/bin/env python3
"""Keep CLIProxyAPI's per-request usage records and serve a dashboard over them.

CLIProxyAPI (v7) publishes one record per request to a queue that readers drain
destructively and that expires after `redis-usage-queue-retention-seconds`.
This process drains that queue into SQLite every few seconds, so usage history
survives, and serves an HTML dashboard plus a JSON API over the stored rows.
"""

import argparse
import bisect
import hashlib
import json
import logging
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

log = logging.getLogger("cliproxyapi-usage")

QUEUE_BATCH = 500
ACCOUNTS_REFRESH_SECONDS = 300
MAX_SERIES = 5  # validated categorical slots; the rest fold into "Other"

SCHEMA = """
CREATE TABLE IF NOT EXISTS requests (
  request_id TEXT PRIMARY KEY,
  ts REAL NOT NULL,
  key_hash TEXT,
  provider TEXT,
  model TEXT,
  alias TEXT,
  endpoint TEXT,
  auth_index TEXT,
  auth_type TEXT,
  source TEXT,
  input_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  reasoning_tokens INTEGER NOT NULL DEFAULT 0,
  cached_tokens INTEGER NOT NULL DEFAULT 0,
  total_tokens INTEGER NOT NULL DEFAULT 0,
  latency_ms INTEGER,
  failed INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS requests_ts ON requests (ts);
CREATE TABLE IF NOT EXISTS accounts (
  auth_index TEXT PRIMARY KEY,
  label TEXT NOT NULL,
  updated REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS quotas (
  auth_index TEXT PRIMARY KEY,
  label TEXT NOT NULL,
  provider TEXT NOT NULL,
  position INTEGER NOT NULL,
  priority INTEGER,
  short_used REAL,
  short_reset REAL,
  short_window INTEGER,
  long_used REAL,
  long_reset REAL,
  long_window INTEGER,
  status TEXT NOT NULL,
  error TEXT,
  checked REAL NOT NULL
);
"""


def connect(path):
    db = sqlite3.connect(path, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def init_db(path):
    db = connect(path)
    try:
        db.executescript(SCHEMA)
        columns = {r["name"] for r in db.execute("PRAGMA table_info(quotas)")}
        if "long_window" not in columns:  # quotas is a cache rebuilt every round
            db.executescript("DROP TABLE quotas;" + SCHEMA)
    finally:
        db.close()


# --- Records ---------------------------------------------------------------

_FRACTION = re.compile(r"(\.\d{6})\d+")


def parse_timestamp(value):
    if isinstance(value, (int, float)):
        return value / 1000 if value > 1e11 else float(value)
    text = _FRACTION.sub(r"\1", str(value).strip()).replace("Z", "+00:00")
    return datetime.fromisoformat(text).timestamp()


def _int(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _token(tokens, name):
    return _int(tokens.get(f"{name}_tokens", tokens.get(name)))


def key_hash(api_key):
    return hashlib.sha256(api_key.encode()).hexdigest() if api_key else None


def normalize(record):
    """Map one usage-queue record to a requests row. The raw client key is never stored."""
    tokens = record.get("tokens") or {}
    row = {
        "ts": parse_timestamp(record.get("timestamp") or time.time()),
        "key_hash": key_hash(record.get("api_key")),
        "provider": record.get("provider"),
        "model": record.get("model"),
        "alias": record.get("alias"),
        "endpoint": record.get("endpoint"),
        "auth_index": None if record.get("auth_index") is None else str(record["auth_index"]),
        "auth_type": record.get("auth_type"),
        "source": record.get("source"),
        "input_tokens": _token(tokens, "input"),
        "output_tokens": _token(tokens, "output"),
        "reasoning_tokens": _token(tokens, "reasoning"),
        "cached_tokens": _token(tokens, "cached"),
        "total_tokens": _token(tokens, "total"),
        "latency_ms": _int(record.get("latency_ms")),
        "failed": 1 if record.get("failed") else 0,
    }
    if not row["total_tokens"]:
        row["total_tokens"] = row["input_tokens"] + row["output_tokens"]
    request_id = record.get("request_id")
    if not request_id:
        request_id = "sha256:" + hashlib.sha256(
            json.dumps(record, sort_keys=True, default=str).encode()).hexdigest()
    row["request_id"] = str(request_id)
    return row


def store(db, records):
    rows = [normalize(r) for r in records]
    if rows:
        cols = list(rows[0])
        db.executemany(
            f"INSERT OR IGNORE INTO requests ({', '.join(cols)}) "
            f"VALUES ({', '.join(':' + c for c in cols)})", rows)
    return len(rows)


def account_labels(payload):
    """Extract auth_index -> label pairs from GET /auth-files, whatever its exact shape."""
    files = payload.get("files", payload) if isinstance(payload, dict) else payload
    out = {}
    for f in files if isinstance(files, list) else []:
        if not isinstance(f, dict) or f.get("auth_index") is None:
            continue
        who = f.get("email") or f.get("account") or f.get("label") or f.get("name") or f.get("id")
        kind = f.get("provider") or f.get("type")
        out[str(f["auth_index"])] = " ".join(str(p) for p in (kind, who) if p)
    return out


# --- Management API --------------------------------------------------------

class Management:
    def __init__(self, proxy_url, key_file):
        self.base = proxy_url.rstrip("/") + "/v0/management"
        self.key_file = Path(key_file)

    def call(self, method, path, body=None):
        key = self.key_file.read_text().strip()
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.base + path, data=data, method=method, headers={
            "Authorization": f"Bearer {key}", "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp)

    def get(self, path):
        return self.call("GET", path)

    def upstream(self, auth_index, url, headers):
        """GET an upstream URL with a credential's own token (the proxy fills in $TOKEN$)."""
        resp = self.call("POST", "/api-call", {"auth_index": auth_index, "method": "GET", "url": url,
                                               "header": headers})
        if not 200 <= int(resp.get("status_code") or 0) < 300:
            raise ValueError(f"upstream {url} returned {resp.get('status_code')}")
        body = resp.get("body")
        return json.loads(body) if isinstance(body, str) else body


# --- Collector -------------------------------------------------------------

class Collector(threading.Thread):
    def __init__(self, db_path, api, interval):
        super().__init__(daemon=True, name="collector")
        self.db_path = db_path
        self.api = api
        self.interval = interval
        self.stop = threading.Event()

    def drain(self, db):
        total = 0
        while True:
            batch = self.api.get(f"/usage-queue?count={QUEUE_BATCH}") or []
            with db:
                total += store(db, batch)
            if len(batch) < QUEUE_BATCH:
                return total

    def refresh_accounts(self, db):
        labels = account_labels(self.api.get("/auth-files"))
        with db:
            db.executemany(
                "INSERT INTO accounts (auth_index, label, updated) VALUES (?, ?, ?) "
                "ON CONFLICT (auth_index) DO UPDATE SET label = excluded.label, updated = excluded.updated",
                [(k, v, time.time()) for k, v in labels.items()])

    def note(self, db, **values):
        with db:
            db.executemany("INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)",
                           [(k, str(v)) for k, v in values.items()])

    def run(self):
        db = connect(self.db_path)
        next_accounts = 0.0
        while not self.stop.is_set():
            try:
                if time.monotonic() >= next_accounts:
                    self.refresh_accounts(db)
                    next_accounts = time.monotonic() + ACCOUNTS_REFRESH_SECONDS
                n = self.drain(db)
                self.note(db, last_ok=time.time(), last_error="")
                if n:
                    log.info("stored %d records", n)
            except (OSError, ValueError, urllib.error.URLError, sqlite3.Error) as e:
                log.warning("collect failed: %s", e)
                self.note(db, last_error_at=time.time(), last_error=e)
            self.stop.wait(self.interval)


# --- Reset-aware priority --------------------------------------------------
#
# CLIProxyAPI prefers higher-priority credentials for new sessions (established
# session bindings stay put, and exhausted credentials fail over). Ranking each
# provider's accounts by soonest weekly reset spends allowance that would
# otherwise expire unused, while skipping accounts near a short-window limit.

CLAUDE_USAGE_URL = "https://api.anthropic.com/api/oauth/usage"
CLAUDE_HEADERS = {"Authorization": "Bearer $TOKEN$", "Content-Type": "application/json",
                  "anthropic-beta": "oauth-2025-04-20", "User-Agent": "claude-cli/2.1.280 (external, cli)"}
CODEX_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage"
CODEX_HEADERS = {"Authorization": "Bearer $TOKEN$", "Content-Type": "application/json",
                 "User-Agent": "codex-tui/0.149.1 (Mac OS 26.5.2; arm64) iTerm.app/3.6.11 (codex-tui; 0.149.1)"}
LONG_WINDOW_SECONDS = 86400  # windows at least a day long count as the "weekly" allowance
PRIORITY_STEP = 10


def claude_windows(usage):
    """Anthropic /api/oauth/usage -> (short, long) as {used, reset} dicts or None."""
    def window(key, seconds):
        w = usage.get(key)
        if not isinstance(w, dict) or w.get("utilization") is None:
            return None
        reset = parse_timestamp(w["resets_at"]) if w.get("resets_at") else None
        return {"used": float(w["utilization"]), "reset": reset, "window": seconds}
    window_lengths = {"five_hour": 5 * 3600, "seven_day": 7 * 86400}
    return tuple(window(k, s) for k, s in window_lengths.items()) + (False,)


def codex_windows(usage):
    """ChatGPT /wham/usage -> (short, long, blocked); windows are sorted by length."""
    limits = usage.get("rate_limit") or {}
    windows = []
    for key in ("primary_window", "secondary_window"):
        w = limits.get(key)
        if isinstance(w, dict) and w.get("used_percent") is not None:
            windows.append((int(w.get("limit_window_seconds") or 0),
                            {"used": float(w["used_percent"]), "reset": w.get("reset_at"),
                             "window": int(w.get("limit_window_seconds") or 0) or None}))
    short = next((w for secs, w in windows if secs < LONG_WINDOW_SECONDS), None)
    long = next((w for secs, w in windows if secs >= LONG_WINDOW_SECONDS), None)
    blocked = bool(limits.get("limit_reached")) or limits.get("allowed") is False
    return short, long, blocked


def blocker(account, short_limit):
    """Why an account should not take new sessions, or None if it can."""
    short, long = account["short"], account["long"]
    if account.get("error"):
        return "Unreadable"
    if account["blocked"] or (long and long["used"] >= 100) or (short and short["used"] >= 100):
        return "Limited"
    if short and short["used"] >= short_limit:
        return "Near short limit"
    return None


def usable(account, short_limit):
    return blocker(account, short_limit) is None


def rank(accounts, short_limit):
    """Order one provider's accounts best-first: usable ones by soonest long-window reset."""
    def key(a):
        long, short = a["long"] or {}, a["short"] or {}
        if usable(a, short_limit):
            return (0, long.get("reset") or float("inf"), long.get("used") or 0, a["name"])
        # Unusable accounts go last, soonest to become usable again first.
        return (1, short.get("reset") or long.get("reset") or float("inf"), 0, a["name"])
    return sorted(accounts, key=key)


def priorities(ranked):
    return {a["name"]: PRIORITY_STEP * (len(ranked) - i) for i, a in enumerate(ranked)}


class Prioritizer(threading.Thread):
    def __init__(self, db_path, api, interval, short_limit):
        super().__init__(daemon=True, name="prioritizer")
        self.db_path = db_path
        self.api = api
        self.interval = interval
        self.short_limit = short_limit
        self.stop = threading.Event()

    def quota(self, f):
        if f["provider"] == "claude":
            return claude_windows(self.api.upstream(f["auth_index"], CLAUDE_USAGE_URL, CLAUDE_HEADERS))
        headers = dict(CODEX_HEADERS)
        account_id = (f.get("id_token") or {}).get("chatgpt_account_id")
        if account_id:
            headers["Chatgpt-Account-Id"] = account_id
        return codex_windows(self.api.upstream(f["auth_index"], CODEX_USAGE_URL, headers))

    def once(self, db):
        files = self.api.get("/auth-files").get("files", [])
        groups = {}
        for f in files:
            provider = f.get("provider") or f.get("type")
            if provider in ("claude", "codex") and not f.get("disabled") and f.get("auth_index"):
                groups.setdefault(provider, []).append(f | {"provider": provider})
        now = time.time()
        for provider, members in groups.items():
            accounts, failed = [], False
            for f in members:
                a = {"name": f["name"], "auth_index": str(f["auth_index"]), "provider": provider,
                     "label": f"{provider} {f.get('email') or f['name']}", "priority": f.get("priority"),
                     "short": None, "long": None, "blocked": False, "error": None}
                try:
                    a["short"], a["long"], a["blocked"] = self.quota(f)
                except (OSError, ValueError, KeyError, TypeError, urllib.error.URLError) as e:
                    a["error"], failed = str(e), True
                accounts.append(a)
            ranked = rank(accounts, self.short_limit)
            wanted = priorities(ranked)
            if failed:  # never reorder a provider on partial information
                log.warning("%s: quota read failed for some accounts; priorities unchanged", provider)
            else:
                for a in accounts:
                    if a["priority"] != wanted[a["name"]]:
                        self.api.call("PATCH", "/auth-files/fields", {"name": a["name"], "priority": wanted[a["name"]]})
                        log.info("%s priority %s -> %s", a["name"], a["priority"], wanted[a["name"]])
                        a["priority"] = wanted[a["name"]]
            with db:
                for position, a in enumerate(ranked, 1):
                    s, l = a["short"] or {}, a["long"] or {}
                    db.execute(
                        "INSERT OR REPLACE INTO quotas (auth_index, label, provider, position, priority, "
                        "short_used, short_reset, short_window, long_used, long_reset, long_window, status, "
                        "error, checked) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (a["auth_index"], a["label"], provider, position, a["priority"], s.get("used"),
                         s.get("reset"), s.get("window"), l.get("used"), l.get("reset"), l.get("window"),
                         blocker(a, self.short_limit) or ("Preferred" if position == 1 else "Standby"),
                         a["error"], now))
        with db:
            db.execute("DELETE FROM quotas WHERE checked < ?", (now - 1,))

    def run(self):
        db = connect(self.db_path)
        while not self.stop.is_set():
            try:
                self.once(db)
            except (OSError, ValueError, urllib.error.URLError, sqlite3.Error) as e:
                log.warning("prioritize failed: %s", e)
            self.stop.wait(self.interval)


# --- Aggregation -----------------------------------------------------------

RANGES = {  # name -> (span, bucket)
    "24h": (timedelta(hours=24), "hour"),
    "7d": (timedelta(days=7), "6h"),
    "30d": (timedelta(days=30), "day"),
    "90d": (timedelta(days=90), "day"),
}


def bucket_edges(now, range_name):
    """Local-time bucket starts covering the range, oldest first."""
    span, unit = RANGES[range_name]
    if unit == "day":
        end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        edges = [end - timedelta(days=i) for i in range(span.days - 1, -1, -1)]
    else:
        step = 6 if unit == "6h" else 1
        end = now.replace(minute=0, second=0, microsecond=0)
        end = end.replace(hour=end.hour - end.hour % step)
        n = int(span.total_seconds() // 3600 // step)
        edges = [end - timedelta(hours=step * i) for i in range(n - 1, -1, -1)]
    return edges


def bucket_label(edge, range_name):
    unit = RANGES[range_name][1]
    if unit == "day":
        return edge.strftime("%b %-d")
    if unit == "6h":
        return edge.strftime("%a %-H:00")
    return edge.strftime("%-H:00")


class Labels:
    """sha256(client key) -> display name, from a JSON file reloaded when it changes."""

    def __init__(self, path):
        self.path = Path(path) if path else None
        self.mtime = None
        self.names = {}

    def current(self):
        if self.path and self.path.exists():
            mtime = self.path.stat().st_mtime
            if mtime != self.mtime:
                self.names = {k.lower(): v for k, v in json.loads(self.path.read_text()).items()}
                self.mtime = mtime
        return self.names

    def name(self, h):
        if not h:
            return "No key"
        for k, v in self.current().items():
            if h.startswith(k):
                return v
        return "Key " + h[:6]

    def order(self):
        return list(dict.fromkeys(self.current().values()))


# Split input the way T3 Code and CodexBar do: uncached input, cache reads, cache
# writes. Providers disagree on input_tokens: OpenAI/Codex includes cache reads
# (total = input + output), while Claude excludes them and reports cache writes
# only in total_tokens (total = input + cached + writes + output). A record whose
# parts fit inside its total is Claude-style; the remainder is cache writes.
_ANTHROPIC_STYLE = "(input_tokens + cached_tokens + output_tokens <= total_tokens)"
TOTALS = f"""COUNT(*) AS requests, SUM(failed) AS failed,
  SUM(CASE WHEN {_ANTHROPIC_STYLE} THEN input_tokens
      ELSE MAX(input_tokens - cached_tokens, 0) END) AS input,
  SUM(cached_tokens) AS cached,
  SUM(CASE WHEN {_ANTHROPIC_STYLE}
      THEN total_tokens - input_tokens - cached_tokens - output_tokens ELSE 0 END) AS cache_write,
  SUM(output_tokens) AS output,
  SUM(reasoning_tokens) AS reasoning, SUM(total_tokens) AS total, AVG(latency_ms) AS latency"""


def _totals(row):
    d = {k: row[k] or 0 for k in ("requests", "failed", "input", "cached", "cache_write", "output", "reasoning", "total")}
    d["latency_ms"] = round(row["latency"] or 0)
    return d


def summarize(db, labels, range_name, metric="tokens", now=None):
    now = now or datetime.now().astimezone()
    edges = bucket_edges(now, range_name)
    start = edges[0].timestamp()
    accounts = {r["auth_index"]: r["label"] for r in db.execute("SELECT * FROM accounts")}

    def account(auth_index, source):
        return accounts.get(auth_index) or source or (f"#{auth_index}" if auth_index else "Unknown")

    by_client, by_account, by_model = {}, {}, {}
    for r in db.execute(f"SELECT key_hash, auth_index, source, model, {TOTALS} FROM requests "
                        "WHERE ts >= ? GROUP BY key_hash, auth_index, source, model", (start,)):
        t = _totals(r)
        for table, name in ((by_client, labels.name(r["key_hash"])),
                            (by_account, account(r["auth_index"], r["source"])),
                            (by_model, r["model"] or "Unknown")):
            acc = table.setdefault(name, dict.fromkeys(t, 0) | {"_lat": 0})
            for k in t:
                if k != "latency_ms":
                    acc[k] += t[k]
            acc["_lat"] += t["latency_ms"] * t["requests"]
    tables = {}
    for name, table in (("clients", by_client), ("accounts", by_account), ("models", by_model)):
        rows = []
        for label, acc in table.items():
            lat = acc.pop("_lat")
            acc["latency_ms"] = round(lat / acc["requests"]) if acc["requests"] else 0
            rows.append({"name": label, **acc})
        tables[name] = sorted(rows, key=lambda x: -x["total" if metric == "tokens" else "requests"])

    # Series keep a fixed order (labels file first) so a client's color never depends on rank.
    known = labels.order()
    present = [c["name"] for c in tables["clients"]]
    ordered = [n for n in known if n in present] + sorted(n for n in present if n not in known)
    series = ordered[:MAX_SERIES] + (["Other"] if len(ordered) > MAX_SERIES else [])
    index = {n: min(i, MAX_SERIES) for i, n in enumerate(ordered)}

    starts = [e.timestamp() for e in edges]
    values = [[0] * len(series) for _ in edges]
    column = "total_tokens" if metric == "tokens" else "1"
    for r in db.execute(f"SELECT key_hash, CAST(ts / 3600 AS INTEGER) AS hour, SUM({column}) AS v "
                        "FROM requests WHERE ts >= ? GROUP BY key_hash, hour", (start,)):
        i = bisect.bisect_right(starts, r["hour"] * 3600) - 1
        s = index.get(labels.name(r["key_hash"]))  # None if the row landed after the tables were read
        if i >= 0 and s is not None:
            values[i][s] += r["v"] or 0

    overall = _totals(db.execute(f"SELECT {TOTALS} FROM requests WHERE ts >= ?", (start,)).fetchone())
    failures = [dict(r) | {"client": labels.name(r["key_hash"]), "account": account(r["auth_index"], r["source"])}
                for r in db.execute("SELECT ts, key_hash, auth_index, source, model, endpoint, latency_ms "
                                    "FROM requests WHERE failed = 1 AND ts >= ? ORDER BY ts DESC LIMIT 15",
                                    (start,))]
    for f in failures:
        del f["key_hash"]
    quotas = [dict(r) for r in db.execute("SELECT * FROM quotas ORDER BY provider, position")]
    meta = {r["k"]: r["v"] for r in db.execute("SELECT * FROM meta")}
    oldest = db.execute("SELECT MIN(ts) FROM requests").fetchone()[0]
    return {
        "range": range_name,
        "metric": metric,
        "generated": now.isoformat(timespec="seconds"),
        "totals": overall,
        "series": series,
        "buckets": [{"start": e.isoformat(timespec="minutes"), "label": bucket_label(e, range_name), "values": v}
                    for e, v in zip(edges, values)],
        **tables,
        "failures": failures,
        "quotas": quotas,
        "collector": {
            "last_ok": float(meta["last_ok"]) if meta.get("last_ok") else None,
            "last_error": meta.get("last_error") or None,
            "oldest_record": oldest,
        },
    }


# --- Rendering -------------------------------------------------------------

def compact(n):
    n = float(n or 0)
    for unit, size in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(n) >= size:
            v = n / size
            return f"{v:.1f}".rstrip("0").rstrip(".") + unit
    return f"{n:,.0f}"


def nice_max(v):
    if v <= 0:
        return 1
    mag = 10 ** (len(str(int(v))) - 1)
    for m in (1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10):
        if m * mag >= v:
            return m * mag
    return 10 * mag


def column_path(x, y, w, h, r):
    """A column segment with rounded top corners and a square base."""
    r = min(r, h, w / 2)
    return (f"M{x:.1f},{y + h:.1f}V{y + r:.1f}Q{x:.1f},{y:.1f} {x + r:.1f},{y:.1f}"
            f"H{x + w - r:.1f}Q{x + w:.1f},{y:.1f} {x + w:.1f},{y + r:.1f}V{y + h:.1f}Z")


def chart_svg(summary):
    W, H, left, right, top, bottom = 960, 260, 56, 8, 12, 28
    pw, ph = W - left - right, H - top - bottom
    buckets, n = summary["buckets"], len(summary["buckets"])
    ymax = nice_max(max((sum(b["values"]) for b in buckets), default=0))
    band = pw / n
    bw = min(24, band * 0.72)
    parts = [f'<svg class="chart" viewBox="0 0 {W} {H}" role="img" aria-labelledby="chart-title">']
    for i in range(5):
        v = ymax * i / 4
        y = top + ph - ph * i / 4
        cls = "baseline" if i == 0 else "grid"
        parts.append(f'<line class="{cls}" x1="{left}" x2="{W - right}" y1="{y:.1f}" y2="{y:.1f}"/>')
        parts.append(f'<text class="tick" x="{left - 8}" y="{y + 4:.1f}" text-anchor="end">{compact(v)}</text>')
    every = max(1, -(-n // 8))
    for i, b in enumerate(buckets):
        cx = left + band * i + band / 2
        x = cx - bw / 2
        y = top + ph
        stack = [(s, v) for s, v in enumerate(b["values"]) if v]
        for k, (s, v) in enumerate(stack):
            h = ph * v / ymax
            gap = 2 if k else 0
            seg_h = max(h - gap, 1)
            y -= h
            if k == len(stack) - 1:
                parts.append(f'<path class="s{s}" d="{column_path(x, y, bw, seg_h, 4)}"/>')
            else:
                parts.append(f'<rect class="s{s}" x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{seg_h:.1f}"/>')
        if (n - 1 - i) % every == 0:
            parts.append(f'<text class="tick" x="{cx:.1f}" y="{H - 8}" text-anchor="middle">'
                         f'{escape(b["label"])}</text>')
        parts.append(f'<rect class="hit" data-i="{i}" tabindex="0" x="{left + band * i:.1f}" y="{top}" '
                     f'width="{band:.1f}" height="{ph}"/>')
    parts.append("</svg>")
    return "".join(parts)


def until(ts):
    if not ts:
        return ""
    s = max(0, int(float(ts) - time.time()))
    d, h, m = s // 86400, s % 86400 // 3600, s % 3600 // 60
    return f"{d}d {h}h" if d else f"{h}h {m}m" if h else f"{m}m" if m else "<1m"


def ago(ts):
    if not ts:
        return "never"
    s = int(time.time() - float(ts))
    return f"{s}s ago" if s < 120 else f"{s // 60}m ago" if s < 7200 else f"{s // 3600}h ago"


def window_name(seconds, fallback):
    if not seconds:
        return fallback
    hours = seconds / 3600
    if hours == 168:
        return "Weekly"
    return f"{hours:.0f}-hour" if hours < 48 else f"{hours / 24:.0f}-day"


def meter_html(name, used, reset, window, now):
    """One limit window as T3 Code shows it: a bar of what is left, plus a tick
    where even spending would be (the share of the window still to run)."""
    left = max(0.0, min(100.0, 100 - used))
    severity = "crit" if left < 10 else "warn" if left < 25 else "ok"
    when = f"resets in {until(reset)}" if reset else "no reset time"
    tick, pace = "", ""
    if reset and window:
        time_left = max(0.0, min(100.0, 100 * (float(reset) - now) / window))
        tick = f'<b style="left:{time_left:.1f}%"></b>'
        pace = ("; ahead of pace" if left < time_left - 5 else "; under pace" if left > time_left + 5
                else "; on pace")
    label = f"{name}: {left:.0f}% left, {when}{pace}"
    return (f'<div class="win"><span class="wl">{escape(name)}</span>'
            f'<div class="meter {severity}" role="img" aria-label="{escape(label)}" title="{escape(label)}">'
            f'<i style="width:{left:.1f}%"></i>{tick}</div>'
            f'<span class="wv"><strong>{left:.0f}% left</strong><span>{escape(when)}</span></span></div>')


BADGES = {  # status -> (status color, glyph); each badge pairs its color with a glyph and a label
    "Preferred": ("good", "✓"), "Standby": ("neutral", ""), "Near short limit": ("warning", "!"),
    "Limited": ("critical", "✕"), "Unreadable": ("serious", "?"),
}


def quotas_html(quotas, now=None):
    if not quotas:
        return ""
    now = now or time.time()
    groups = {}
    for q in sorted(quotas, key=lambda q: (q["provider"], q["position"])):
        groups.setdefault(q["provider"], []).append(q)
    blocks = []
    for provider, qs in groups.items():
        cards = []
        for q in qs:
            who = q["label"][len(provider) + 1:] if q["label"].startswith(provider + " ") else q["label"]
            short_name = window_name(q["short_window"], "5-hour")
            kind, glyph = BADGES.get(q["status"], ("neutral", ""))
            text = f"Near {short_name.lower()} limit" if q["status"] == "Near short limit" else q["status"]
            badge = (f'<span class="badge {kind}"><span class="ic {kind}" aria-hidden="true">{glyph}</span>'
                     f'{escape(text)}</span>')
            if q["error"]:
                body = f'<p class="err">Could not read limits: {escape(q["error"])}</p>'
            else:
                rows = []
                if q["short_used"] is not None:
                    rows.append(meter_html(short_name, q["short_used"], q["short_reset"], q["short_window"], now))
                if q["long_used"] is not None:
                    rows.append(meter_html(window_name(q["long_window"], "Weekly"), q["long_used"],
                                           q["long_reset"], q["long_window"], now))
                body = f'<div class="wins">{"".join(rows)}</div>' if rows else '<p class="err">No limits reported</p>'
            priority = q["priority"] if q["priority"] is not None else "unset"
            cards.append(f'<div class="acct"><div class="acct-head"><span class="who" title="{escape(who)}">'
                         f'{escape(who)}</span>{badge}</div>{body}'
                         f'<div class="acct-foot">Rank {q["position"]} of {len(qs)} · priority {priority}</div></div>')
        blocks.append(f'<div class="provider"><h3>{escape(provider.title())}</h3>'
                      f'<div class="accts">{"".join(cards)}</div></div>')
    checked = ago(max(q["checked"] for q in quotas))
    return ('<section><div class="sec-head"><h2>Account limits</h2>'
            f'<span class="muted">Checked {checked}</span></div>'
            '<p class="intro">New sessions go to the account whose weekly limit resets soonest, skipping any '
            'near its short limit. Bars show what is left; the tick marks where even spending would be, so a '
            'bar ending left of its tick is ahead of pace.</p>'
            f'{"".join(blocks)}</section>')


def name_html(name, swatch=None):
    name = str(name)
    provider, _, rest = name.partition(" ")
    if swatch is not None:
        return f'<i class="sw k{swatch}"></i>{escape(name)}'
    if rest and "@" in rest and provider.isalpha() and provider.islower():
        return f'<span class="tag">{escape(provider.title())}</span>{escape(rest)}'
    return escape(name)


def table_html(title, rows, first, swatches=None):
    top = max((r["total"] for r in rows), default=0) or 1

    def dim(v, text=None):
        return f'<td class="dim{" zero" if not v else ""}">{text if v else "—"}</td>'

    def row(r):
        swatch = (swatches or {}).get(r["name"])
        fill = f"k{swatch}" if swatch is not None else "kacc"
        return (f'<tr><td class="name" title="{escape(str(r["name"]))}">{name_html(r["name"], swatch)}</td>'
                f'<td>{r["requests"]:,}</td>'
                f'<td class="tot"><span class="share"><i class="{fill}" style="width:{100 * r["total"] / top:.1f}%">'
                f'</i></span><strong>{compact(r["total"])}</strong></td>'
                + dim(r["input"], compact(r["input"])) + dim(r["cached"], compact(r["cached"]))
                + dim(r["cache_write"], compact(r["cache_write"])) + dim(r["output"], compact(r["output"]))
                + dim(r["failed"], f'{r["failed"]:,}')
                + f'<td class="dim">{r["latency_ms"] / 1000:.1f}s</td></tr>')

    head = (f'<tr><th class="name">{escape(first)}</th><th>Requests</th><th class="tot">Total tokens</th>'
            '<th>Uncached</th><th>Cache read</th><th>Cache write</th><th>Output</th><th>Failed</th>'
            '<th>Avg latency</th></tr>')
    body = "".join(row(r) for r in rows) or '<tr><td colspan="9" class="empty">No requests in this range</td></tr>'
    return (f'<section><div class="sec-head"><h2>{escape(title)}</h2></div>'
            f'<div class="scroll"><table class="data">{head}{body}</table></div></section>')


STYLE = """
:root { color-scheme: light; --page:#f9f9f7; --surface:#fcfcfb; --ink:#0b0b0b; --ink-2:#52514e;
  --muted:#898781; --grid:#e1e0d9; --axis:#c3c2b7; --ring:rgba(11,11,11,0.10); --accent:#2a78d6;
  --good:#0ca30c; --warning:#fab219; --serious:#ec835a; --critical:#d03b3b;
  --s0:#2a78d6; --s1:#eb6834; --s2:#1baf7a; --s3:#eda100; --s4:#e87ba4; --s5:#898781; }
@media (prefers-color-scheme: dark) { :root { color-scheme: dark; --page:#0d0d0d; --surface:#1a1a19;
  --ink:#ffffff; --ink-2:#c3c2b7; --grid:#2c2c2a; --axis:#383835; --ring:rgba(255,255,255,0.10);
  --accent:#3987e5; --s0:#3987e5; --s1:#d95926; --s2:#199e70; --s3:#c98500; --s4:#d55181; --s5:#6b6a65; } }
* { box-sizing: border-box; }
body { margin:0; background:var(--page); color:var(--ink); font:14px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif; }
main { max-width:1120px; margin:0 auto; padding:28px 24px 40px; }
header { display:flex; justify-content:space-between; align-items:baseline; gap:16px; flex-wrap:wrap; }
h1 { font-size:18px; font-weight:600; margin:0; } h2 { font-size:14px; font-weight:600; margin:0; }
.sub, .muted { color:var(--ink-2); } .muted { font-size:12px; }
.sec-head { display:flex; justify-content:space-between; align-items:baseline; gap:12px; margin-bottom:12px; }
.intro { color:var(--ink-2); margin:-4px 0 16px; max-width:78ch; font-size:13px; }
.filters { display:flex; gap:12px; margin:24px 0 12px; flex-wrap:wrap; }
.seg { display:inline-flex; border:1px solid var(--ring); border-radius:8px; overflow:hidden; font-size:13px; }
.seg a { padding:6px 12px; color:var(--ink-2); text-decoration:none; }
.seg a:hover { color:var(--ink); }
.seg a[aria-current] { background:var(--surface); color:var(--ink); font-weight:600; box-shadow:inset 0 0 0 1px var(--ring); }
.tiles { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; }
.tile, section { background:var(--surface); border:1px solid var(--ring); border-radius:12px; padding:16px 18px; }
section { margin-top:16px; }
.tile .label { color:var(--ink-2); font-size:13px; } .tile .value { font-size:26px; font-weight:600; margin-top:2px; }
/* account limits */
.provider + .provider { margin-top:20px; }
.provider h3 { font-size:12px; font-weight:600; color:var(--ink-2); margin:0 0 8px; text-transform:uppercase; letter-spacing:.04em; }
.accts { display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%,420px),1fr)); gap:12px; }
.acct { border:1px solid var(--ring); border-radius:10px; padding:14px 16px 12px; }
.acct-head { display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:14px; }
.who { font-weight:600; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.badge { display:inline-flex; align-items:center; gap:6px; flex-shrink:0; font-size:12px; color:var(--ink-2);
  padding:2px 9px 2px 4px; border:1px solid var(--ring); border-radius:999px; white-space:nowrap; }
.badge.good { color:var(--ink); font-weight:500; }
.ic { width:15px; height:15px; border-radius:50%; display:inline-grid; place-items:center; font-size:9px;
  font-weight:700; line-height:1; color:#ffffff; }
.ic.good { background:var(--good); } .ic.critical { background:var(--critical); }
.ic.warning { background:var(--warning); color:#0b0b0b; } .ic.serious { background:var(--serious); color:#0b0b0b; }
.ic.neutral { box-shadow:inset 0 0 0 1.5px var(--muted); }
.wins { display:flex; flex-direction:column; gap:10px; }
.win { display:grid; grid-template-columns:4.5rem minmax(0,1fr) 7.5rem; grid-template-areas:"l m v";
  gap:4px 14px; align-items:center; }
.wl { grid-area:l; color:var(--ink-2); font-size:13px; } .meter { grid-area:m; } .wv { grid-area:v; }
@media (max-width:560px) {
  main { padding:20px 14px 32px; } .tile, section { padding:14px; } .acct { padding:12px 14px 10px; }
  .win { grid-template-columns:1fr auto; grid-template-areas:"l v" "m m"; }
  .wv { flex-direction:row; gap:6px; align-items:baseline; }
}
.meter { position:relative; height:20px; }
.meter::before { content:""; position:absolute; inset:5px 0; border-radius:999px;
  background:color-mix(in oklab, var(--fill) 18%, var(--surface)); }
.meter i { position:absolute; left:0; top:5px; bottom:5px; border-radius:999px; background:var(--fill); }
.meter b { position:absolute; top:1px; bottom:1px; width:2px; margin-left:-1px; border-radius:1px;
  background:var(--ink); box-shadow:0 0 0 2px var(--surface); }
.meter.ok { --fill:var(--accent); } .meter.warn { --fill:var(--warning); } .meter.crit { --fill:var(--critical); }
.wv { display:flex; flex-direction:column; text-align:right; line-height:1.25; }
.wv strong { font-weight:600; font-variant-numeric:tabular-nums; }
.wv span { color:var(--ink-2); font-size:12px; font-variant-numeric:tabular-nums; }
.acct-foot { margin-top:12px; color:var(--muted); font-size:12px; }
.err { color:var(--ink-2); margin:0; font-size:13px; }
/* chart */
.legend { display:flex; gap:16px; flex-wrap:wrap; color:var(--ink-2); margin-bottom:8px; font-size:13px; }
.legend i { display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:6px; vertical-align:-1px; }
.chart { width:100%; height:auto; display:block; }
.chart .grid { stroke:var(--grid); stroke-width:1; } .chart .baseline { stroke:var(--axis); stroke-width:1; }
.chart .tick { fill:var(--muted); font-size:11px; font-variant-numeric:tabular-nums; }
.chart .hit { fill:transparent; outline:none; }
.chart .hit:hover, .chart .hit:focus-visible { fill:var(--ink); fill-opacity:0.04; }
.s0{fill:var(--s0)} .s1{fill:var(--s1)} .s2{fill:var(--s2)} .s3{fill:var(--s3)} .s4{fill:var(--s4)} .s5{fill:var(--s5)}
.k0{background:var(--s0)} .k1{background:var(--s1)} .k2{background:var(--s2)} .k3{background:var(--s3)} .k4{background:var(--s4)} .k5{background:var(--s5)} .kacc{background:var(--accent)}
/* tables */
.scroll { overflow-x:auto; }
table { border-collapse:collapse; width:100%; font-variant-numeric:tabular-nums; font-size:13px; }
th { color:var(--muted); font-weight:500; font-size:12px; text-align:right; padding:0 10px 8px; white-space:nowrap;
  border-bottom:1px solid var(--grid); }
td { text-align:right; padding:9px 10px; border-bottom:1px solid var(--grid); white-space:nowrap; }
tr:last-child td { border-bottom:0; }
th:first-child, td:first-child, .name { text-align:left; }
td.name { max-width:280px; overflow:hidden; text-overflow:ellipsis; }
td.dim { color:var(--ink-2); } td.dim.zero { color:var(--muted); }
td.tot strong { font-weight:600; display:inline-block; min-width:4.2em; }
.share { display:inline-block; position:relative; width:64px; height:6px; border-radius:999px; background:var(--grid);
  vertical-align:middle; margin-right:10px; overflow:hidden; }
.share i { position:absolute; left:0; top:0; bottom:0; border-radius:999px; }
.sw { display:inline-block; width:9px; height:9px; border-radius:2px; margin-right:8px; vertical-align:0; }
.tag { font-size:11px; color:var(--ink-2); border:1px solid var(--ring); border-radius:4px; padding:0 5px; margin-right:8px; }
td.empty { text-align:center; color:var(--muted); }
.failures th:not(:last-child), .failures td:not(:last-child) { text-align:left; }
details { margin-top:12px; font-size:13px; } summary { cursor:pointer; color:var(--ink-2); }
.status { margin-top:20px; color:var(--muted); font-size:12px; }
.status .bad { color:var(--critical); } .status a { color:var(--ink-2); }
#tip { position:fixed; pointer-events:none; background:var(--surface); border:1px solid var(--ring); font-size:13px;
  border-radius:8px; padding:8px 10px; box-shadow:0 4px 16px rgba(0,0,0,0.12); display:none; min-width:160px; }
#tip .when { color:var(--ink-2); margin-bottom:4px; }
#tip .row { display:flex; gap:8px; align-items:center; justify-content:space-between; }
#tip .row span:first-child { display:flex; align-items:center; gap:6px; color:var(--ink-2); }
#tip .key { width:12px; border-top:2px solid; } #tip b { font-variant-numeric:tabular-nums; }
#tip .kl0{border-color:var(--s0)} #tip .kl1{border-color:var(--s1)} #tip .kl2{border-color:var(--s2)} #tip .kl3{border-color:var(--s3)} #tip .kl4{border-color:var(--s4)} #tip .kl5{border-color:var(--s5)}
"""

SCRIPT = """
const data = JSON.parse(document.getElementById('chart-data').textContent);
const tip = document.getElementById('tip');
function show(el, x, y) {
  const b = data.buckets[+el.dataset.i];
  tip.replaceChildren();
  const when = document.createElement('div'); when.className = 'when'; when.textContent = b.label; tip.append(when);
  const rows = data.series.map((s, i) => [s, b.values[i], i]);
  rows.push(['Total', b.values.reduce((a, v) => a + v, 0), -1]);
  for (const [name, v, i] of rows) {
    const row = document.createElement('div'); row.className = 'row';
    const label = document.createElement('span');
    if (i >= 0) { const k = document.createElement('i'); k.className = 'key kl' + Math.min(i, 5); label.append(k); }
    label.append(document.createTextNode(name));
    const value = document.createElement('b'); value.textContent = v.toLocaleString();
    row.append(label, value); tip.append(row);
  }
  tip.style.display = 'block';
  const r = tip.getBoundingClientRect();
  tip.style.left = Math.min(x + 14, innerWidth - r.width - 8) + 'px';
  tip.style.top = Math.max(8, y - r.height - 12) + 'px';
}
for (const el of document.querySelectorAll('.hit')) {
  el.addEventListener('pointermove', e => show(el, e.clientX, e.clientY));
  el.addEventListener('pointerleave', () => tip.style.display = 'none');
  el.addEventListener('focus', () => { const r = el.getBoundingClientRect(); show(el, r.left + r.width / 2, r.top + 40); });
  el.addEventListener('blur', () => tip.style.display = 'none');
}
"""


def render(summary, title):
    rng, metric, t = summary["range"], summary["metric"], summary["totals"]
    seg = lambda key, options, cur, other: "".join(  # noqa: E731
        f'<a href="?{key}={o}&{other}"{" aria-current=page" if o == cur else ""}>{escape(lbl)}</a>'
        for o, lbl in options)
    ranges = seg("range", [(r, "Last " + r) for r in RANGES], rng, f"metric={metric}")
    metrics = seg("metric", [("tokens", "Tokens"), ("requests", "Requests")], metric, f"range={rng}")
    fail_rate = f"{100 * t['failed'] / t['requests']:.1f}%" if t["requests"] else "0%"
    tiles = "".join(f'<div class="tile"><div class="label">{lbl}</div><div class="value">{v}</div></div>' for lbl, v in (
        ("Requests", compact(t["requests"])), ("Total tokens", compact(t["total"])),
        ("Output tokens", compact(t["output"])), ("Failed requests", fail_rate)))
    legend = "".join(f'<span><i class="k{min(i, 5)}"></i>{escape(s)}</span>' for i, s in enumerate(summary["series"]))
    unit = "Total tokens" if metric == "tokens" else "Requests"
    bucket_rows = "".join(
        "<tr><td>{}</td>{}<td>{:,}</td></tr>".format(
            escape(b["start"].replace("T", " ")), "".join(f"<td>{v:,}</td>" for v in b["values"]), sum(b["values"]))
        for b in summary["buckets"])
    bucket_head = "<tr><th>Bucket start</th>{}<th>Total</th></tr>".format(
        "".join(f"<th>{escape(s)}</th>" for s in summary["series"]))
    swatches = {s: min(i, 5) for i, s in enumerate(summary["series"])}
    failures = "".join(
        f"<tr><td class='dim'>{datetime.fromtimestamp(f['ts']).strftime('%b %-d %H:%M:%S')}</td>"
        f"<td>{escape(f['client'])}</td><td>{name_html(f['account'])}</td>"
        f"<td class='dim'>{escape(str(f['model'] or ''))}</td><td class='dim'>{escape(str(f['endpoint'] or ''))}</td>"
        f"<td>{(f['latency_ms'] or 0) / 1000:.1f}s</td></tr>"
        for f in summary["failures"]) or '<tr><td colspan="6" class="empty">No failed requests</td></tr>'
    c = summary["collector"]
    stale = not c["last_ok"] or time.time() - c["last_ok"] > 120
    status = (f'Collector: last poll <span class="{"bad" if stale else ""}">{ago(c["last_ok"])}</span>'
              + (f' · last error: <span class="bad">{escape(c["last_error"])}</span>' if c["last_error"] else "")
              + f' · history since {datetime.fromtimestamp(c["oldest_record"]).strftime("%b %-d %Y %H:%M") if c["oldest_record"] else "—"}'
              + ' · <a href="/api/summary?range=' + rng + '">JSON</a>')
    chart_json = json.dumps({"series": summary["series"], "buckets": summary["buckets"]}).replace("</", "<\\/")
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>{escape(title)}</title>
<style>{STYLE}</style></head><body><main>
<header><h1>{escape(title)}</h1><span class="muted">Updated {escape(summary["generated"][11:16])}</span></header>
{quotas_html(summary["quotas"])}
<div class="filters"><div class="seg">{ranges}</div><div class="seg">{metrics}</div></div>
<div class="tiles">{tiles}</div>
<section><div class="sec-head"><h2 id="chart-title">{unit} by client</h2></div><div class="legend">{legend}</div>{chart_svg(summary)}
<details><summary>Chart data</summary><div class="scroll"><table>{bucket_head}{bucket_rows}</table></div></details></section>
{table_html("By client", summary["clients"], "Client", swatches)}
{table_html("By upstream account", summary["accounts"], "Account")}
{table_html("By model", summary["models"], "Model")}
<section><div class="sec-head"><h2>Recent failures</h2></div><div class="scroll"><table class="failures"><tr><th>Time</th>
<th>Client</th><th>Account</th><th>Model</th><th>Endpoint</th><th>Latency</th></tr>{failures}</table></div></section>
<div class="status">{status}</div></main><div id="tip" role="tooltip"></div>
<script type="application/json" id="chart-data">{chart_json}</script><script>{SCRIPT}</script></body></html>"""


# --- HTTP ------------------------------------------------------------------

def make_handler(db_path, labels, title):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            log.debug(fmt, *args)

        def send(self, code, body, ctype):
            data = body.encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            url = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            rng = q.get("range") if q.get("range") in RANGES else "24h"
            metric = q.get("metric") if q.get("metric") in ("tokens", "requests") else "tokens"
            if url.path == "/healthz":
                return self.send(200, "ok\n", "text/plain")
            if url.path not in ("/", "/api/summary"):
                return self.send(404, "not found\n", "text/plain")
            db = connect(db_path)
            try:
                summary = summarize(db, labels, rng, metric)
            finally:
                db.close()
            if url.path == "/api/summary":
                return self.send(200, json.dumps(summary, indent=2), "application/json")
            return self.send(200, render(summary, title), "text/html; charset=utf-8")

    return Handler


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--proxy-url", default="http://127.0.0.1:8317")
    p.add_argument("--management-key-file", required=True)
    p.add_argument("--db", default="usage.db")
    p.add_argument("--labels-file", help="JSON object mapping sha256(client key) or a prefix of it to a name")
    p.add_argument("--listen", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8318)
    p.add_argument("--poll-interval", type=float, default=5)
    p.add_argument("--title", default="CLIProxyAPI usage")
    p.add_argument("--prioritize", action="store_true",
                   help="rank Claude/Codex credentials by soonest weekly reset by setting their priority")
    p.add_argument("--prioritize-interval", type=float, default=300)
    p.add_argument("--short-window-limit", type=float, default=90,
                   help="percent of the short (e.g. 5-hour) window above which an account is skipped")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    init_db(args.db)
    api = Management(args.proxy_url, args.management_key_file)
    Collector(args.db, api, args.poll_interval).start()
    if args.prioritize:
        Prioritizer(args.db, api, args.prioritize_interval, args.short_window_limit).start()
    server = ThreadingHTTPServer((args.listen, args.port), make_handler(args.db, Labels(args.labels_file), args.title))
    log.info("dashboard on http://%s:%d", args.listen, args.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
