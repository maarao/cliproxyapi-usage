import json
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import cliproxyapi_usage as u

NOW = datetime(2026, 10, 2, 12, 30).astimezone()


def record(minutes_ago, key="k-maanav", model="claude-opus-5-5", failed=False, auth_index=1, **tokens):
    ts = (NOW - timedelta(minutes=minutes_ago)).isoformat()
    return {
        "timestamp": ts, "latency_ms": 1200, "source": "vt@example.com", "auth_index": auth_index,
        "tokens": {"input_tokens": 100, "output_tokens": 20, "cached_tokens": 50, "total_tokens": 120} | tokens,
        "failed": failed, "provider": "claude", "model": model, "endpoint": "/v1/messages",
        "auth_type": "oauth", "api_key": key, "request_id": f"req-{u.key_hash(key)[:8]}-{minutes_ago}-{model}",
    }


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.dir.name) / "usage.db")
        u.init_db(self.db_path)
        self.labels_path = Path(self.dir.name) / "labels.json"
        self.labels_path.write_text(json.dumps({u.key_hash("k-maanav"): "Maanav", u.key_hash("k-hermes")[:12]: "Hermes"}))
        self.labels = u.Labels(self.labels_path)

    def tearDown(self):
        self.dir.cleanup()


class RecordTests(Base):
    def test_normalize_hashes_key_and_never_stores_it(self):
        row = u.normalize(record(5))
        self.assertEqual(row["key_hash"], u.key_hash("k-maanav"))
        self.assertNotIn("k-maanav", json.dumps(row))

    def test_parse_timestamp_variants(self):
        self.assertEqual(u.parse_timestamp("2026-10-02T04:00:00Z"), u.parse_timestamp("2026-10-02T04:00:00.000000000+00:00"))
        self.assertEqual(u.parse_timestamp(1790913600000), 1790913600.0)

    def test_missing_request_id_and_total_are_derived(self):
        r = record(1)
        del r["request_id"]
        r["tokens"] = {"input": 7, "output": 3}
        row = u.normalize(r)
        self.assertTrue(row["request_id"].startswith("sha256:"))
        self.assertEqual(row["total_tokens"], 10)

    def test_store_is_idempotent(self):
        with u.connect(self.db_path) as db:
            u.store(db, [record(1), record(1)])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM requests").fetchone()[0], 1)

    def test_account_labels(self):
        payload = {"files": [{"auth_index": 3, "type": "claude", "email": "mr@example.com"}, {"name": "no-index"}]}
        self.assertEqual(u.account_labels(payload), {"3": "claude mr@example.com"})


class SummaryTests(Base):
    def test_summary_groups_and_buckets(self):
        with u.connect(self.db_path) as db:
            u.store(db, [record(10), record(70, key="k-hermes", model="gpt-6.1-sol"),
                         record(90, key="k-unknown", failed=True), record(60 * 30)])
            db.execute("INSERT INTO accounts VALUES ('1', 'claude vt@example.com', 0)")
            s = u.summarize(db, self.labels, "24h", now=NOW)
        self.assertEqual(s["totals"]["requests"], 3)  # the 30h-old record is out of range
        self.assertEqual(s["series"][:2], ["Maanav", "Hermes"])
        self.assertTrue(s["series"][2].startswith("Key "))
        self.assertEqual(sum(sum(b["values"]) for b in s["buckets"]), 360)
        self.assertEqual(s["buckets"][-1]["values"][0], 120)
        self.assertEqual(s["accounts"][0]["name"], "claude vt@example.com")
        self.assertEqual(len(s["failures"]), 1)
        self.assertEqual((s["totals"]["input"], s["totals"]["cache_write"]), (3 * (100 - 50), 0))
        self.assertNotIn("key_hash", s["failures"][0])

    def test_series_fold_into_other_and_colors_follow_entity(self):
        with u.connect(self.db_path) as db:
            u.store(db, [record(5, key=f"k-{i}") for i in range(7)] + [record(5, key="k-maanav")])
            s = u.summarize(db, self.labels, "7d", metric="requests", now=NOW)
        self.assertEqual(len(s["series"]), u.MAX_SERIES + 1)
        self.assertEqual(s["series"][0], "Maanav")
        self.assertEqual(s["series"][-1], "Other")
        self.assertEqual(sum(s["buckets"][-1]["values"]), 8)

    def test_input_split_matches_t3_and_codexbar(self):
        codex = record(5) | {"tokens": {"input_tokens": 18922, "cached_tokens": 14848,
                                        "output_tokens": 148, "total_tokens": 19070}}
        claude = record(6) | {"tokens": {"input_tokens": 8, "cached_tokens": 608530, "output_tokens": 1848,
                                         "reasoning_tokens": 177, "total_tokens": 612585}}
        with u.connect(self.db_path) as db:
            u.store(db, [codex, claude])
            t = u.summarize(db, self.labels, "24h", now=NOW)["totals"]
        self.assertEqual(t["input"], (18922 - 14848) + 8)
        self.assertEqual(t["cached"], 14848 + 608530)
        self.assertEqual(t["cache_write"], 612585 - 8 - 608530 - 1848)
        self.assertEqual(t["input"] + t["cached"] + t["cache_write"] + t["output"], t["total"])

    def test_render_escapes_labels(self):
        self.labels_path.write_text(json.dumps({u.key_hash("k-maanav"): "<b>x</b>"}))
        with u.connect(self.db_path) as db:
            u.store(db, [record(5)])
            html = u.render(u.summarize(db, self.labels, "24h", now=NOW), "Usage")
        self.assertNotIn("<b>x</b>", html)
        self.assertIn("&lt;b&gt;x&lt;/b&gt;", html)

    def test_empty_database_renders(self):
        with u.connect(self.db_path) as db:
            for rng in u.RANGES:
                self.assertIn("No requests in this range", u.render(u.summarize(db, self.labels, rng, now=NOW), "Usage"))


class CollectorTests(Base):
    def test_drains_queue_and_reads_accounts(self):
        queue = [record(i) for i in range(u.QUEUE_BATCH + 3)]
        seen_auth = []

        class Fake(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                seen_auth.append(self.headers.get("Authorization"))
                if self.path.startswith("/v0/management/usage-queue"):
                    n = int(self.path.split("count=")[1])
                    body, queue[:n] = queue[:n], []
                elif self.path == "/v0/management/auth-files":
                    body = {"files": [{"auth_index": 1, "provider": "claude", "email": "vt@example.com"}]}
                else:
                    self.send_response(404)
                    self.end_headers()
                    return
                data = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Fake)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        key = Path(self.dir.name) / "key"
        key.write_text("secret\n")
        c = u.Collector(self.db_path, u.Management(f"http://127.0.0.1:{server.server_port}", key), 0.05)
        c.start()
        deadline = time.time() + 5
        with u.connect(self.db_path) as db:
            while time.time() < deadline:
                if db.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == u.QUEUE_BATCH + 3:
                    break
                time.sleep(0.05)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM requests").fetchone()[0], u.QUEUE_BATCH + 3)
            self.assertEqual(db.execute("SELECT label FROM accounts").fetchone()[0], "claude vt@example.com")
        c.stop.set()
        server.shutdown()
        server.server_close()
        self.assertEqual(set(seen_auth), {"Bearer secret"})


HOUR = 3600


def acct(name, short=None, long=None, blocked=False):
    w = lambda x: None if x is None else {"used": x[0], "reset": x[1]}  # noqa: E731
    return {"name": name, "short": w(short), "long": w(long), "blocked": blocked}


class RankTests(unittest.TestCase):
    def test_soonest_weekly_reset_first(self):
        r = u.rank([acct("late", (10, 1), (40, 100 * HOUR)), acct("soon", (10, 1), (90, 30 * HOUR))], 90)
        self.assertEqual([a["name"] for a in r], ["soon", "late"])
        self.assertEqual(u.priorities(r), {"soon": 20, "late": 10})

    def test_blocker_reasons(self):
        self.assertEqual(u.blocker(acct("x", (95, 1), (10, 1)), 90), "Near short limit")
        self.assertEqual(u.blocker(acct("x", (10, 1), (10, 1), blocked=True), 90), "Limited")
        self.assertEqual(u.blocker(acct("x", None, (100, 1)), 90), "Limited")
        self.assertIsNone(u.blocker(acct("x", (10, 1), (10, 1)), 90))

    def test_near_short_limit_or_exhausted_goes_last(self):
        r = u.rank([acct("hot", (95, 2 * HOUR), (10, 1 * HOUR)), acct("spent", (0, 1), (100, 2 * HOUR)),
                    acct("blocked", None, (5, 3 * HOUR), blocked=True), acct("ok", (20, 1), (50, 90 * HOUR))], 90)
        self.assertEqual(r[0]["name"], "ok")
        self.assertEqual({a["name"] for a in r[1:]}, {"hot", "spent", "blocked"})

    def test_windows_parse(self):
        short, long, blocked = u.claude_windows({
            "five_hour": {"utilization": 14.0, "resets_at": "2026-10-02T08:40:00.319010+00:00"},
            "seven_day": {"utilization": 89.0, "resets_at": "2026-10-05T10:00:00.319032+00:00"}})
        self.assertEqual((short["used"], long["used"], blocked), (14.0, 89.0, False))
        self.assertEqual((short["window"], long["window"]), (18000, 604800))
        self.assertLess(short["reset"], long["reset"])
        short, long, blocked = u.codex_windows({"rate_limit": {
            "allowed": True, "limit_reached": False, "secondary_window": None,
            "primary_window": {"used_percent": 49, "limit_window_seconds": 604800, "reset_at": 1791408823}}})
        self.assertEqual((short, long, blocked), (None, {"used": 49.0, "reset": 1791408823, "window": 604800}, False))
        self.assertTrue(u.codex_windows({"rate_limit": {"limit_reached": True}})[2])


class LimitsViewTests(unittest.TestCase):
    def test_meter_shows_left_and_pace_tick(self):
        now = 1_000_000.0
        html = u.meter_html("5-hour", 80, now + 9000, 18000, now)  # 20% left, half the window to run
        self.assertIn("width:20.0%", html)
        self.assertIn("left:50.0%", html)
        self.assertIn("ahead of pace", html)
        self.assertIn('class="meter warn"', html)
        self.assertIn('class="meter crit"', u.meter_html("Weekly", 95, None, None, now))

    def test_window_names(self):
        self.assertEqual([u.window_name(s, "x") for s in (18000, 604800, 86400 * 30, None)],
                         ["5-hour", "Weekly", "30-day", "x"])

    def test_old_quotas_table_is_rebuilt(self):
        with tempfile.TemporaryDirectory() as d:
            path = str(Path(d) / "u.db")
            db = u.connect(path)
            db.execute("CREATE TABLE quotas (auth_index TEXT PRIMARY KEY, label TEXT)")
            db.commit(); db.close()
            u.init_db(path)
            db = u.connect(path)
            self.assertIn("long_window", {r["name"] for r in db.execute("PRAGMA table_info(quotas)")})
            db.close()


class PrioritizerTests(Base):
    def test_sets_changed_priorities_and_skips_providers_with_failures(self):
        files = [
            {"name": "codex-a.json", "provider": "codex", "auth_index": "a", "priority": 100,
             "id_token": {"chatgpt_account_id": "acct-a"}},
            {"name": "codex-b.json", "provider": "codex", "auth_index": "b", "priority": 10,
             "id_token": {"chatgpt_account_id": "acct-b"}},
            {"name": "claude-c.json", "provider": "claude", "auth_index": "c", "priority": None},
            {"name": "claude-d.json", "provider": "claude", "auth_index": "d", "priority": None},
        ]
        codex = {"a": 1791408823, "b": 1791046716}

        class Api:
            patches = []

            def get(self, path):
                return {"files": files}

            def call(self, method, path, body=None):
                self.patches.append(body)
                return {"status": "ok"}

            def upstream(self, auth_index, url, headers):
                if auth_index in codex:
                    assert headers["Chatgpt-Account-Id"] == f"acct-{auth_index}"
                    return {"rate_limit": {"allowed": True, "primary_window": {
                        "used_percent": 40, "limit_window_seconds": 604800, "reset_at": codex[auth_index]}}}
                raise OSError("claude usage unavailable")

        api = Api()
        with u.connect(self.db_path) as db:
            u.Prioritizer(self.db_path, api, 300, 90).once(db)
            rows = {r["auth_index"]: dict(r) for r in db.execute("SELECT * FROM quotas")}
        self.assertEqual(api.patches, [{"name": "codex-a.json", "priority": 10}, {"name": "codex-b.json", "priority": 20}])
        self.assertEqual((rows["b"]["position"], rows["a"]["position"]), (1, 2))
        self.assertEqual(rows["c"]["error"], "claude usage unavailable")
        with u.connect(self.db_path) as db:
            html = u.render(u.summarize(db, u.Labels(None), "24h"), "Usage")
        self.assertIn("Account limits", html)
        self.assertIn("Unreadable", html)


if __name__ == "__main__":
    unittest.main()
