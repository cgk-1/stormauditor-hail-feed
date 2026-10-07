"""feedguard.py - shared guard rails for the StormAuditor feed ingesters.

Archive Phase 5, Stage 1 (2026-10-07). ONE IDENTICAL COPY lives in each feed
repo (stormauditor-hail-feed, -wind-feed, -hazard-engine); keep them in sync
(the md5 of this file is printed at the start of every run).

What it provides
  * http_get(): a timeout on every call, retries with exponential backoff and
    full jitter, polite pacing for IEM hosts (serial, >= 0.25 s apart), and a
    hard distinction between "the file does not exist" (HTTP 404/410 ->
    UpstreamMissing, never retried) and "the fetch failed" (UpstreamError
    after all retries). Byte-range reads are length-checked.
  * parse_dates(): DATE=YYYY-MM-DD or DATE=START..END (also the legacy
    INGEST_DATE forms YYYYMMDD, a:b and comma lists), shared by nightly and
    backfill runs.
  * Run: the run ledger.
      - DRY_RUN=1 computes everything and writes nothing: every write RPC is
        recorded (row counts + md5 of the payload, gzip JSONL dump under
        FEED_OUT_DIR/payloads/) instead of being sent. Read-only RPCs still run.
      - write(): one write unit = the RPC sequence for one state-day (its
        first call deletes what it re-supplies). On an ambiguous failure
        (timeout / dropped connection after the request was sent) the WHOLE
        unit is replayed from its first call, so a retry can never duplicate
        appended rows. HTTP error responses are rolled back server side
        (PostgREST runs each RPC in one transaction) and are retried per call.
      - error()/quarantine(): an unexpected input is never dropped silently:
        it gets a JSON quarantine record under FEED_OUT_DIR/quarantine/, an
        ::error:: annotation, and the run exits non-zero at the end.
      - finish(): completeness per day (expected vs received), written to the
        GitHub job summary and printed as ONE machine-readable line
        'FEED_RESULT {json}' (also FEED_OUT_DIR/result.json), so a backfill
        driver can verify every day. Returns the process exit code.
"""
import datetime as dt
import gzip
import hashlib
import json
import os
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

UTC = dt.timezone.utc


class FeedError(RuntimeError):
    """Base class for every feed failure."""


class UpstreamMissing(FeedError):
    """The upstream object definitively does not exist (HTTP 404/410)."""


class UpstreamError(FeedError):
    """The upstream fetch kept failing after all retries."""


class ValidationError(FeedError):
    """The input does not look the way the ingester expects."""


class SafeRetry(FeedError):
    """The DB call certainly did NOT commit (PostgREST 500 = the statement failed and
    rolled back, e.g. 57014 statement timeout; 503 = not executed; connect timeout =
    never sent), so the same chunk may be sent again. timeoutish: load/timeout class
    (eligible for splitting) rather than a deterministic error."""

    def __init__(self, msg, timeoutish):
        super().__init__(msg)
        self.timeoutish = timeoutish


class AmbiguousWrite(FeedError):
    """The request may or may not have reached the database."""


def env_flag(name, default=False):
    v = (os.environ.get(name) or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "on")


def env_float(name, default):
    v = (os.environ.get(name) or "").strip()
    return float(v) if v else default


def file_md5(path):
    with open(path, "rb") as fh:
        return hashlib.md5(fh.read()).hexdigest()


# ------------------------------------------------------------------ HTTP
# IEM asks for polite clients: requests are serial in every feed and at least
# this many seconds apart per host (on top of the feeds' own sleeps).
POLITE_GAP = {"mesonet.agron.iastate.edu": 0.25, "mtarchive.geol.iastate.edu": 0.25}
_LAST_HIT = {}


def _pace(host):
    gap = POLITE_GAP.get(host or "")
    if not gap:
        return
    wait = _LAST_HIT.get(host, -1e9) + gap - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _LAST_HIT[host] = time.monotonic()


def backoff_sleep(attempt, base=2.0, cap=60.0):
    """Exponential backoff with jitter: base*2^attempt (capped), x [0.5, 1.0)."""
    d = min(cap, base * (2 ** attempt))
    time.sleep(d * (0.5 + random.random() * 0.5))


def http_get(url, *, timeout=60, retries=5, headers=None, byte_range=None,
             missing_ok=False, what=None, base_sleep=2.0):
    """GET url -> bytes.

    HTTP 404/410 means the object does not exist: returns None when
    missing_ok, else raises UpstreamMissing. It is never retried.
    Everything else (5xx, 429, 403, timeouts, resets, short reads) is retried
    `retries` times in total with backoff + jitter, then UpstreamError.
    byte_range=(a, b) asks for bytes a..b inclusive (b=None: to the end) and
    requires HTTP 206 and, when b is given, exactly b-a+1 bytes back."""
    hdrs = dict(headers or {})
    if byte_range is not None:
        a, b = byte_range
        hdrs["Range"] = f"bytes={a}-{'' if b is None else b}"
    host = urllib.parse.urlsplit(url).hostname
    label = what or url
    last = None
    for attempt in range(retries):
        _pace(host)
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=hdrs),
                                        timeout=timeout) as r:
                data = r.read()
                status = getattr(r, "status", 200)
            if byte_range is not None:
                if status != 206:
                    raise UpstreamError(f"range request not honoured (HTTP {status})")
                if b is not None and len(data) != b - a + 1:
                    raise UpstreamError(f"short range read: {len(data)} of {b - a + 1} bytes")
            return data
        except urllib.error.HTTPError as e:
            if e.code in (404, 410):
                if missing_ok:
                    return None
                raise UpstreamMissing(f"{label}: HTTP {e.code} (not published)") from None
            last = f"HTTP {e.code}"
        except UpstreamError as e:
            last = str(e)
        except Exception as e:  # timeouts, resets, DNS, TLS
            last = f"{type(e).__name__}: {e}"
        if attempt < retries - 1:
            print(f"  [retry {attempt + 1}/{retries - 1}] {label}: {last}", flush=True)
            backoff_sleep(attempt, base_sleep)
    raise UpstreamError(f"{label}: failed after {retries} attempts ({last})")


def http_head(url, *, timeout=60, retries=5, headers=None, what=None):
    """HEAD url -> response headers (dict-like). 404/410 -> UpstreamMissing."""
    host = urllib.parse.urlsplit(url).hostname
    label = what or url
    last = None
    for attempt in range(retries):
        _pace(host)
        try:
            req = urllib.request.Request(url, headers=dict(headers or {}), method="HEAD")
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.headers
        except urllib.error.HTTPError as e:
            if e.code in (404, 410):
                raise UpstreamMissing(f"{label}: HTTP {e.code} (not published)") from None
            last = f"HTTP {e.code}"
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
        if attempt < retries - 1:
            print(f"  [retry {attempt + 1}/{retries - 1}] HEAD {label}: {last}", flush=True)
            backoff_sleep(attempt)
    raise UpstreamError(f"HEAD {label}: failed after {retries} attempts ({last})")


# ------------------------------------------------------------------ dates
def _one_date(s):
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise ValidationError(f"bad date {s!r} (use YYYY-MM-DD, YYYYMMDD, or START..END)")


def parse_dates(raw, *, max_days=1200, allow_today=False):
    """'2026-10-05' | '2026-10-01..2026-10-05' | '20261001:20261005' | comma
    lists of those -> ordered, de-duplicated list of dates. Refuses days that
    are not finished yet (a local day ends up to 08Z the next UTC day)."""
    out = []
    for tok in [t.strip() for t in (raw or "").split(",") if t.strip()]:
        sep = ".." if ".." in tok else (":" if ":" in tok else None)
        if sep:
            a, b = tok.split(sep, 1)
            d0, d1 = _one_date(a), _one_date(b)
            if d1 < d0:
                raise ValidationError(f"date range {tok!r} ends before it starts")
            cur = d0
            while cur <= d1:
                out.append(cur)
                cur += dt.timedelta(days=1)
        else:
            out.append(_one_date(tok))
    seen, res = set(), []
    for d in out:
        if d not in seen:
            seen.add(d)
            res.append(d)
    if len(res) > max_days:
        raise ValidationError(f"{len(res)} dates requested (max {max_days} per run)")
    today = dt.datetime.now(UTC).date()
    for d in res:
        if d >= today and not allow_today:
            raise ValidationError(f"{d} is not a finished day yet (UTC today is {today})")
    return res


def requested_dates(**kw):
    """Explicit dates from DATE (preferred) or INGEST_DATE; None = scheduled default."""
    raw = (os.environ.get("DATE") or "").strip() or (os.environ.get("INGEST_DATE") or "").strip()
    return parse_dates(raw, **kw) if raw else None


# ------------------------------------------------------------------ run ledger
class _FakeResponse:
    status_code = 200
    text = ""

    def json(self):
        return None


# Chunked write RPCs: (rows key, has p_append, re-sending an APPEND chunk is idempotent).
# Verified against the DB definitions 2026-10-07: p_append=false deletes the key range then
# inserts; p_append=true only inserts. "idempotent" = the insert is an upsert / do-nothing,
# so an ambiguous append can be sent again; plain-insert appends cannot (duplicates), so an
# ambiguous failure there replays the whole write unit from its first (deleting) call.
# Anything not listed (ingest_swath counts n_bands per call, wind_swath_begin/_add, ...)
# is never split and keeps the old retry behaviour.
CHUNK_RPCS = {
    "ingest_points": ("p_points", True, False),
    "ingest_wind_points": ("p_points", True, False),
    "hz_hrrr_ingest": ("p_points", True, True),
    "hz_bg_coarse_ingest": ("p_points", False, True),
    "hz_station_bg_ingest": ("p_rows", True, True),
    "hz_station_peak_ingest": ("p_rows", False, True),
    "hz_station_daily_ingest": ("p_rows", True, True),
    "hz_station_daily_ingest_v4": ("p_rows", True, True),
    "hz_lsr_ingest": ("p_rows", True, False),
    "hz_lsr_ingest_v4": ("p_rows", True, False),
    "hz_storm_events_ingest": ("p_rows", True, True),
    "hz_storm_events_ingest_v4": ("p_rows", True, True),
}
SPLIT_MIN_ROWS = int(os.environ.get("FEED_SPLIT_MIN_ROWS") or 250)
SAME_SIZE_TRIES = 2          # timeout-class failures at one chunk size before it is halved
MAX_TRIES_MIN_SIZE = 4       # attempts at the smallest size (and for non-timeout errors), as before


def _rows_in(payload):
    for k in ("p_points", "p_rows", "p_features"):
        v = payload.get(k)
        if isinstance(v, list):
            return k, len(v)
    if "p_feature" in payload:
        return "p_feature", 1
    return None, 0


def _safe(s):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(s))[:80]


class Run:
    def __init__(self, feed, *, dry_run=None, out_dir=None):
        self.feed = feed
        self.dry_run = env_flag("DRY_RUN") if dry_run is None else dry_run
        self.out_dir = out_dir or os.environ.get("FEED_OUT_DIR") or "feed-out"
        self.started = dt.datetime.now(UTC)
        self.base = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
        self.anon = os.environ.get("SUPABASE_ANON_KEY") or ""
        self.secret = os.environ.get("INGEST_SECRET") or ""
        self.days = {}
        self.order = []
        self.errors = []
        self.warnings = []
        self.quarantine_files = []
        self.deferred = []
        self.meta = {}
        self._md5 = {}
        self._qn = 0
        here = os.path.dirname(os.path.abspath(__file__))
        self.meta["feedguard_md5"] = file_md5(os.path.join(here, "feedguard.py"))
        if not self.dry_run and not (self.base and self.anon and self.secret):
            raise FeedError("SUPABASE_URL, SUPABASE_ANON_KEY and INGEST_SECRET are required "
                            "(or set DRY_RUN=1)")
        print(f"[{feed}] {'DRY RUN - nothing will be written' if self.dry_run else 'LIVE run'}; "
              f"feedguard md5 {self.meta['feedguard_md5']}", flush=True)

    # ---- per-day ledger
    def day(self, key):
        key = str(key)
        if key not in self.days:
            self.days[key] = {"day": key, "status": "ok", "expected": {}, "received": {},
                              "written": [], "empty": [], "failed": [], "skipped": {},
                              "rows": {}, "calls": 0, "notes": []}
            self._md5[key] = hashlib.md5()
            self.order.append(key)
        return self.days[key]

    def expect(self, key, what, n):
        self.day(key)["expected"][what] = n

    def receive(self, key, what, n=1):
        r = self.day(key)["received"]
        r[what] = r.get(what, 0) + n

    def set_received(self, key, what, value):
        self.day(key)["received"][what] = value

    def skip(self, key, why, item):
        self.day(key)["skipped"].setdefault(why, []).append(item)

    def note(self, key, msg):
        print(f"  [note] {key}: {msg}", flush=True)
        self.day(key)["notes"].append(msg)

    def warn(self, key, msg):
        print(f"::warning::{self.feed} {key}: {msg}", flush=True)
        self.warnings.append({"day": str(key), "msg": msg})
        d = self.day(key)
        if d["status"] == "ok":
            d["status"] = "warning"

    def defer(self, key, msg, *, pending=None):
        """The day's inputs are not all published yet (normal for the newest
        day): NOTHING is written for it, the run stays green, and a later run
        picks it up (the schedule must guarantee one does - see each feed)."""
        print(f"::notice::{self.feed} {key}: DEFERRED - {msg}", flush=True)
        d = self.day(key)
        d["status"] = "deferred"
        d["deferred"] = {"reason": msg, "pending": pending or []}
        self.deferred.append({"day": str(key), "msg": msg})

    def quarantine(self, key, scope, reason, details=None):
        self._qn += 1
        qdir = os.path.join(self.out_dir, "quarantine")
        os.makedirs(qdir, exist_ok=True)
        path = os.path.join(qdir, f"{self.feed}-{_safe(key)}-{_safe(scope)}-{self._qn}.json")
        rec = {"feed": self.feed, "day": str(key), "scope": scope, "reason": reason,
               "details": details, "utc": dt.datetime.now(UTC).isoformat(),
               "dry_run": self.dry_run, "git_sha": os.environ.get("GITHUB_SHA"),
               "run_id": os.environ.get("GITHUB_RUN_ID")}
        with open(path, "w") as fh:
            json.dump(rec, fh, indent=1, default=str)
        self.quarantine_files.append(path)
        return path

    def error(self, key, scope, msg, *, details=None, quarantine=True):
        """Record a failure for (day, scope). Nothing is written for that scope;
        the run continues with the other scopes and exits non-zero."""
        path = self.quarantine(key, scope, msg, details) if quarantine else None
        print(f"::error::{self.feed} {key} [{scope}]: {msg}"
              + (f" (quarantine record {path})" if path else ""), flush=True)
        self.errors.append({"day": str(key), "scope": scope, "msg": msg, "quarantine": path})
        d = self.day(key)
        d["status"] = "error"
        if scope not in d["failed"]:
            d["failed"].append(scope)

    # ---- database I/O
    def _headers(self):
        return {"apikey": self.anon, "Authorization": f"Bearer {self.anon}",
                "Content-Type": "application/json"}

    def _post(self, name, payload, timeout):
        import requests
        body = json.dumps(payload)
        last = ""
        for attempt in range(4):
            try:
                r = requests.post(f"{self.base}/rest/v1/rpc/{name}", headers=self._headers(),
                                  data=body, timeout=(20, timeout))
            except requests.exceptions.ConnectTimeout as e:   # never reached the server
                last = f"{name} connect timeout: {e}"
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                raise AmbiguousWrite(f"{name}: {type(e).__name__}: {e}") from None
            else:
                if r.status_code < 300:
                    return r
                last = f"{name} HTTP {r.status_code}: {r.text[:300]}"
                if r.status_code in (400, 401, 403, 404, 409, 422):
                    raise FeedError(last)          # rejected; retrying cannot help
            if attempt < 3:
                print(f"  [retry {attempt + 1}/3] {last}", flush=True)
                backoff_sleep(attempt, 1.5, 30)
        raise FeedError(last)

    def _post_once(self, name, payload, timeout):
        """One attempt, classified: SafeRetry (certainly not committed), AmbiguousWrite
        (may have committed), FeedError (rejected: 4xx)."""
        import requests
        try:
            r = requests.post(f"{self.base}/rest/v1/rpc/{name}", headers=self._headers(),
                              data=json.dumps(payload), timeout=(20, timeout))
        except requests.exceptions.ConnectTimeout as e:
            raise SafeRetry(f"{name} connect timeout: {e}", timeoutish=True) from None
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            raise AmbiguousWrite(f"{name}: {type(e).__name__}: {e}") from None
        if r.status_code < 300:
            return r
        msg = f"{name} HTTP {r.status_code}: {r.text[:300]}"
        if r.status_code in (400, 401, 403, 404, 409, 422):
            raise FeedError(msg)
        if r.status_code in (502, 504):          # gateway gave up; the statement may have run
            raise AmbiguousWrite(msg)
        if r.status_code == 500:
            raise SafeRetry(msg, timeoutish="57014" in r.text)
        raise SafeRetry(msg, timeoutish=r.status_code == 503)

    def _retry_log(self, key, entry):
        self.day(key).setdefault("retries", []).append(entry)
        print(f"  [{entry['action']}] {key} {entry['scope']} {entry['rpc']} ({entry['rows']} rows): "
              f"{entry.get('error', '')[:160]}", flush=True)

    def _write_chunk(self, key, scope, name, payload, timeout):
        """One chunk of a CHUNK_RPCS write. Certain failures retry the SAME chunk; after
        SAME_SIZE_TRIES timeout-class failures the chunk is halved (first half keeps its
        p_append, the second half appends) recursively down to SPLIT_MIN_ROWS, then fails
        loudly. An ambiguous failure is retried only when re-sending is idempotent (a
        deleting first chunk, or an upsert RPC); otherwise it goes up to the unit replay."""
        rows_key, has_append, idem = CHUNK_RPCS[name]
        rows = payload.get(rows_key) or []
        resend_ok = idem or not has_append or payload.get("p_append") is False
        fails = 0
        while True:
            try:
                return self._post_once(name, payload, timeout)
            except SafeRetry as e:
                err, timeoutish = e, e.timeoutish
            except AmbiguousWrite as e:
                if not resend_ok:
                    self._retry_log(key, {"scope": scope, "rpc": name, "rows": len(rows), "action": "unit-replay",
                                          "error": str(e)})
                    raise
                err, timeoutish = e, True
            fails += 1
            if timeoutish and fails >= SAME_SIZE_TRIES and len(rows) > SPLIT_MIN_ROWS:
                k = len(rows) // 2
                first = dict(payload, **{rows_key: rows[:k]})
                second = dict(payload, **{rows_key: rows[k:]})
                if has_append:
                    second["p_append"] = True
                self._retry_log(key, {"scope": scope, "rpc": name, "rows": len(rows), "action": "split",
                                      "into": [k, len(rows) - k], "after_failures": fails, "error": str(err)})
                self._write_chunk(key, scope, name, first, timeout)
                return self._write_chunk(key, scope, name, second, timeout)
            if fails >= MAX_TRIES_MIN_SIZE:
                self._retry_log(key, {"scope": scope, "rpc": name, "rows": len(rows), "action": "failed",
                                      "attempts": fails, "error": str(err)})
                raise FeedError(f"{name} failed after {fails} attempt(s) at {len(rows)} rows "
                                f"(smallest chunk {SPLIT_MIN_ROWS}): {err}")
            self._retry_log(key, {"scope": scope, "rpc": name, "rows": len(rows), "action": "retry",
                                  "attempt": fails, "error": str(err)})
            backoff_sleep(fails - 1, 1.5, 30)

    def rpc_read(self, name, payload, timeout=60):
        """Read-only RPC: runs in dry-run mode too."""
        return self._post(name, payload, timeout)

    def rest_count(self, table, params, timeout=30):
        """Exact row count via PostgREST (read-only). Raises on any failure."""
        import requests
        last = ""
        for attempt in range(4):
            try:
                r = requests.get(f"{self.base}/rest/v1/{table}", params=params,
                                 headers={"apikey": self.anon, "Authorization": f"Bearer {self.anon}",
                                          "Prefer": "count=exact", "Range": "0-0"},
                                 timeout=timeout)
                cr = r.headers.get("Content-Range", "")
                if r.status_code < 300 and "/" in cr and cr.split("/")[-1].isdigit():
                    return int(cr.split("/")[-1])
                last = f"HTTP {r.status_code} Content-Range={cr!r}"
            except Exception as e:
                last = f"{type(e).__name__}: {e}"
            if attempt < 3:
                backoff_sleep(attempt, 1.5, 20)
        raise FeedError(f"count {table} {params}: {last}")

    def _record(self, key, scope, calls):
        d = self.day(key)
        h = self._md5[key]
        for name, payload in calls:
            clean = {k: v for k, v in payload.items() if k != "p_secret"}
            line = json.dumps(clean)
            h.update(name.encode() + b"\n" + line.encode() + b"\n")
            k, n = _rows_in(payload)
            if k:
                rk = f"{name}.{k}"
                d["rows"][rk] = d["rows"].get(rk, 0) + n
            d["calls"] += 1
            if self.dry_run:
                pdir = os.path.join(self.out_dir, "payloads")
                os.makedirs(pdir, exist_ok=True)
                with gzip.open(os.path.join(pdir, f"{self.feed}-{_safe(key)}.jsonl.gz"), "at") as fh:
                    fh.write(json.dumps({"scope": scope, "rpc": name, "payload": clean}) + "\n")

    def write(self, key, scope, calls, *, timeout=120):
        """Execute one write unit (list of (rpc, payload)); its first call must
        delete/replace what the unit re-supplies, so replaying it is safe."""
        calls = [(n, p) for n, p in calls]
        if not calls:
            return
        if self.day(key)["status"] == "deferred":
            raise FeedError(f"refusing to write {scope} for deferred day {key}")
        self._record(key, scope, calls)
        if self.dry_run:
            return _FakeResponse()
        last = None
        for unit_try in range(3):
            try:
                r = None
                for name, payload in calls:
                    r = (self._write_chunk(key, scope, name, payload, timeout) if name in CHUNK_RPCS
                         else self._post(name, payload, timeout))
                return r
            except AmbiguousWrite as e:
                last = e
                print(f"  [replay {unit_try + 1}/2] {key} {scope}: {e} - replaying the unit "
                      f"from its first call", flush=True)
                backoff_sleep(unit_try, 3, 30)
        raise FeedError(f"write unit {scope} failed after replays: {last}")

    def written(self, key, scope):
        d = self.day(key)
        if scope not in d["written"]:
            d["written"].append(scope)

    def empty(self, key, scope):
        d = self.day(key)
        if scope not in d["empty"]:
            d["empty"].append(scope)

    # ---- end of run
    def result(self):
        days = []
        for key in self.order:
            d = dict(self.days[key])
            d["payload_md5"] = self._md5[key].hexdigest() if d["calls"] else None
            days.append(d)
        status = ("error" if self.errors else "warning" if self.warnings
                  else "deferred" if self.deferred else "ok")
        retries = sum(len(d.get("retries", [])) for d in days)
        return {"feed": self.feed, "status": status, "dry_run": self.dry_run, "write_retries": retries,
                "started_utc": self.started.isoformat(),
                "finished_utc": dt.datetime.now(UTC).isoformat(),
                "git_sha": os.environ.get("GITHUB_SHA"), "run_id": os.environ.get("GITHUB_RUN_ID"),
                "meta": self.meta, "days": days, "errors": self.errors, "deferred": self.deferred,
                "warnings": self.warnings, "quarantine": self.quarantine_files}

    def _summary_md(self, res):
        icon = {"ok": "OK", "warning": "WARNING", "error": "FAILED",
                "deferred": "DEFERRED (inputs not published yet)"}[res["status"]]
        L = [f"### {self.feed} ingest: {icon}{' (DRY RUN, nothing written)' if self.dry_run else ''}", ""]
        L.append("| day | status | expected | received | written | empty | failed | rows | payload md5 |")
        L.append("|---|---|---|---|---|---|---|---|---|")
        for d in res["days"]:
            fmt = lambda m: "<br>".join(f"{k}: {v}" for k, v in m.items()) or "-"
            L.append(f"| {d['day']} | {d['status']} | {fmt(d['expected'])} | {fmt(d['received'])} | "
                     f"{len(d['written'])} | {len(d['empty'])} | {', '.join(d['failed']) or '-'} | "
                     f"{fmt(d['rows'])} | `{d['payload_md5'] or '-'}` |")
        partial = [d for d in res["days"] if d["failed"] and d["written"]]
        if partial:
            L += ["", "**Partial completion** (complete scopes were written; failed scopes were NOT):"]
            for d in partial:
                L.append(f"- {d['day']}: written {len(d['written'])} ({', '.join(d['written'])}); "
                         f"failed {len(d['failed'])} ({', '.join(d['failed'])})")
        if res["deferred"]:
            L += ["", "**Deferred** (nothing written for these days; a later run ingests them)"]
            L += [f"- {d['day']}: {d['msg']}" for d in res["deferred"]]
        if res["errors"]:
            L += ["", "**Errors**"] + [f"- {e['day']} [{e['scope']}]: {e['msg']}" for e in res["errors"]]
        if res["warnings"]:
            L += ["", "**Warnings**"] + [f"- {w['day']}: {w['msg']}" for w in res["warnings"]]
        notes = [(d["day"], n) for d in res["days"] for n in d["notes"]]
        if notes:
            L += ["", "**Notes**"] + [f"- {k}: {n}" for k, n in notes]
        if res["quarantine"]:
            L += ["", f"Quarantine records: {len(res['quarantine'])} (artifact `feed-out`)"]
        return "\n".join(L) + "\n"

    def finish(self):
        res = self.result()
        try:
            os.makedirs(self.out_dir, exist_ok=True)
            with open(os.path.join(self.out_dir, "result.json"), "w") as fh:
                json.dump(res, fh, indent=1, default=str)
            sp = os.environ.get("GITHUB_STEP_SUMMARY")
            if sp:
                with open(sp, "a") as fh:
                    fh.write(self._summary_md(res))
        except Exception as e:  # reporting must never mask the real status
            print(f"::warning::could not write run summary: {e}")
        print("FEED_RESULT " + json.dumps(res, sort_keys=True, separators=(",", ":"), default=str),
              flush=True)
        return 1 if res["status"] == "error" else 0


def main_guard(run, fn):
    """Run fn(run); any uncaught exception becomes a recorded error. Exits
    with the run's status code (non-zero on any error)."""
    try:
        fn(run)
    except Exception as e:
        import traceback
        traceback.print_exc()
        run.error("run", "fatal", f"{type(e).__name__}: {e}")
    sys.exit(run.finish())
