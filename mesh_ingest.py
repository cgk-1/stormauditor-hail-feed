#!/usr/bin/env python3
"""
MRMS MESH -> Supabase hail ingester for stormauditor.com  (v3: local-clock days)

DAY CONVENTION (v3): each "day" is the LOCAL CALENDAR DAY (midnight-to-midnight,
local clock time, DST-aware) of the state's dominant timezone. For each local
date D and timezone group, we fetch the MESH_Max_1440min file whose 24-hour
rolling window ENDS at local midnight ending D (i.e., 00:00 local on D+1,
converted to UTC, rounded to the archive's 30-minute file cadence). This makes
StormAuditor dates match a homeowner's clock and commercial reports.

TIMESTAMPS (v3): every state-day now stores window_end_utc, the exact UTC end
of the 24h window the values cover (window = (end-24h, end]). This makes any
future date-convention change a pure database relabel, never a re-download.

Also retained from v2: 0.50" capture floor, exact point values, in-state max,
MultiPolygon coercion, complete state boundaries, retries, chunked point writes.
Retention: 3 years, purged incrementally by purge_old_hail() on every run.

Env (GitHub secrets): SUPABASE_URL, SUPABASE_ANON_KEY, INGEST_SECRET
Optional: INGEST_DATE (local YYYYMMDD, single/comma/range a:b), STATES,
          STATE_PAUSE (sec between states, default 0.4)

Deps: pygrib numpy rasterio shapely requests
"""
import os, gzip, json, time, datetime as dt
from zoneinfo import ZoneInfo
import urllib.request
import numpy as np
import pygrib
import requests
from rasterio.features import shapes
from rasterio.transform import from_origin
from shapely.geometry import shape, mapping, Point, MultiPolygon, Polygon
from shapely.prepared import prep
from shapely.ops import unary_union

BANDS = [0.50, 0.75, 1.00, 1.25, 1.50, 1.75, 2.00, 2.50, 3.00]
POINT_FLOOR = 0.50
GRID_NORTH, GRID_WEST, GRID_RES = 55.0, -130.0, 0.01
UTC = dt.timezone.utc

# Dominant IANA timezone per state (approved approximation: one zone per state).
STATE_TZ = {
 "Alabama":"America/Chicago","Arizona":"America/Phoenix","Arkansas":"America/Chicago",
 "California":"America/Los_Angeles","Colorado":"America/Denver","Connecticut":"America/New_York",
 "Delaware":"America/New_York","Florida":"America/New_York","Georgia":"America/New_York",
 "Idaho":"America/Boise","Illinois":"America/Chicago","Indiana":"America/Indiana/Indianapolis",
 "Iowa":"America/Chicago","Kansas":"America/Chicago","Kentucky":"America/New_York",
 "Louisiana":"America/Chicago","Maine":"America/New_York","Maryland":"America/New_York",
 "Massachusetts":"America/New_York","Michigan":"America/Detroit","Minnesota":"America/Chicago",
 "Mississippi":"America/Chicago","Missouri":"America/Chicago","Montana":"America/Denver",
 "Nebraska":"America/Chicago","Nevada":"America/Los_Angeles","New Hampshire":"America/New_York",
 "New Jersey":"America/New_York","New Mexico":"America/Denver","New York":"America/New_York",
 "North Carolina":"America/New_York","North Dakota":"America/Chicago","Ohio":"America/New_York",
 "Oklahoma":"America/Chicago","Oregon":"America/Los_Angeles","Pennsylvania":"America/New_York",
 "Rhode Island":"America/New_York","South Carolina":"America/New_York","South Dakota":"America/Chicago",
 "Tennessee":"America/Chicago","Texas":"America/Chicago","Utah":"America/Denver",
 "Vermont":"America/New_York","Virginia":"America/New_York","Washington":"America/Los_Angeles",
 "West Virginia":"America/New_York","Wisconsin":"America/Chicago","Wyoming":"America/Denver",
}
PERMITTED_STATES = set(STATE_TZ)

STATE_ABBR = {
 "Alabama":"AL","Arizona":"AZ","Arkansas":"AR","California":"CA","Colorado":"CO",
 "Connecticut":"CT","Delaware":"DE","Florida":"FL","Georgia":"GA","Idaho":"ID",
 "Illinois":"IL","Indiana":"IN","Iowa":"IA","Kansas":"KS","Kentucky":"KY",
 "Louisiana":"LA","Maine":"ME","Maryland":"MD","Massachusetts":"MA","Michigan":"MI",
 "Minnesota":"MN","Mississippi":"MS","Missouri":"MO","Montana":"MT","Nebraska":"NE",
 "Nevada":"NV","New Hampshire":"NH","New Jersey":"NJ","New Mexico":"NM","New York":"NY",
 "North Carolina":"NC","North Dakota":"ND","Ohio":"OH","Oklahoma":"OK","Oregon":"OR",
 "Pennsylvania":"PA","Rhode Island":"RI","South Carolina":"SC","South Dakota":"SD",
 "Tennessee":"TN","Texas":"TX","Utah":"UT","Vermont":"VT","Virginia":"VA",
 "Washington":"WA","West Virginia":"WV","Wisconsin":"WI","Wyoming":"WY",
}
# Months where MESH bright-band / cool-season false positives dominate the
# false-alarm budget (verified 2026-08-27: e.g. Maine showed ~25 "hail days"
# EVERY month incl. January; New York had 389 cold-month days with no ground
# report, avg 1.5in). The guard below drops a cold-month state-day ONLY when
# BOTH lines of evidence say artifact: zero in-state hail LSRs for the local
# day AND the statewide max temperature stayed below 45F. Any fetch failure
# keeps the day (fail-open) so real hail in rural / under-spotted areas is
# never dropped just for lack of a report.
COLD_MONTHS = {11, 12, 1, 2, 3}

BOUNDARY_URL = "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json"
_CACHE = {}
_ALL_STATES_CACHE = None


def load_state_geom(name):
    global _ALL_STATES_CACHE
    if name in _CACHE:
        return _CACHE[name]
    if _ALL_STATES_CACHE is None:
        gj = json.loads(urllib.request.urlopen(BOUNDARY_URL, timeout=60).read())
        _ALL_STATES_CACHE = {f["properties"]["name"]: f["geometry"] for f in gj["features"]}
    if name not in _ALL_STATES_CACHE:
        raise RuntimeError(f"no boundary found for {name}")
    geom = shape(_ALL_STATES_CACHE[name]).buffer(0)
    _CACHE[name] = geom
    return geom


def window_end_utc(state, local_date_str):
    """UTC datetime of local midnight ENDING local date D (= 00:00 local on D+1),
    DST-aware, rounded to the archive's 30-minute file cadence."""
    tz = ZoneInfo(STATE_TZ[state])
    y, m, d = int(local_date_str[:4]), int(local_date_str[4:6]), int(local_date_str[6:])
    end = (dt.datetime(y, m, d, tzinfo=tz) + dt.timedelta(days=1)).astimezone(UTC)
    # round to nearest 30 min (files exist at :00 and :30)
    if end.minute < 15:
        end = end.replace(minute=0)
    elif end.minute < 45:
        end = end.replace(minute=30)
    else:
        end = (end + dt.timedelta(hours=1)).replace(minute=0)
    return end.replace(second=0, microsecond=0)


def fetch_mesh_at(anchor_utc):
    """Download the MESH_Max_1440min file whose window ends at anchor_utc.
    Retries; returns mm array or None."""
    ds = anchor_utc.strftime("%Y%m%d")
    ts = anchor_utc.strftime("%H%M%S")
    url = (f"https://mtarchive.geol.iastate.edu/{ds[:4]}/{ds[4:6]}/{ds[6:]}/mrms/ncep/"
           f"MESH_Max_1440min/MESH_Max_1440min_00.50_{ds}-{ts}.grib2.gz")
    for attempt in range(4):
        try:
            raw = urllib.request.urlopen(url, timeout=120).read()
            with open("/tmp/_mesh.grib2", "wb") as fh:
                fh.write(gzip.decompress(raw))
            g = pygrib.open("/tmp/_mesh.grib2")
            vals = np.asarray(g[1].values)
            g.close()
            return vals
        except Exception as e:
            if attempt == 3:
                print(f"  [warn] fetch failed {url}: {e}")
                return None
            time.sleep(2 * (attempt + 1))


def classify(vals_mm):
    inches = np.where(vals_mm >= 0, vals_mm / 25.4, 0.0)
    # Radar-artifact ceiling: MESH above 8 inches is physically impossible
    # (hail spikes / three-body scatter). Zero those cells so neither bands
    # nor points nor the day max can be poisoned.
    inches = np.where(inches <= 8.0, inches, 0.0)
    # Spatial-support despeckle for giant hail: a real >=3in core always sits
    # inside a broader swath of smaller hail. Isolated >=3in cells with fewer
    # than 5 supporting cells (>=0.75in) in their 5x5 neighborhood are radar
    # artifacts (cool-season bright-band / clutter) — zero them. Confirmed
    # empirically: 59 such state-days in history, all with ZERO ground reports.
    big = inches >= 3.0
    if big.any():
        support = (inches >= 0.75).astype(np.int16)
        # 5x5 neighborhood sum via shifted adds (no scipy dependency)
        nb = np.zeros_like(support)
        for dy in range(-2, 3):
            for dx in range(-2, 3):
                if dy == 0 and dx == 0:
                    continue
                nb[max(0,dy):support.shape[0]+min(0,dy) or None,
                   max(0,dx):support.shape[1]+min(0,dx) or None] +=                   support[max(0,-dy):support.shape[0]+min(0,-dy) or None,
                          max(0,-dx):support.shape[1]+min(0,-dx) or None]
        inches = np.where(big & (nb < 5), 0.0, inches)
    cls = np.zeros(inches.shape, dtype=np.int16)
    for i, b in enumerate(BANDS, start=1):
        cls[inches >= b] = i
    return cls, inches


def _window(arr, geom):
    minx, miny, maxx, maxy = geom.bounds
    c0 = int((minx - GRID_WEST) / GRID_RES); c1 = int((maxx - GRID_WEST) / GRID_RES) + 1
    r0 = int((GRID_NORTH - maxy) / GRID_RES); r1 = int((GRID_NORTH - miny) / GRID_RES) + 1
    sub = arr[r0:r1, c0:c1]
    t = from_origin(GRID_WEST + c0 * GRID_RES, GRID_NORTH - r0 * GRID_RES, GRID_RES, GRID_RES)
    return sub, t


def build_bands(cls, geom):
    sub, t = _window(cls, geom)
    if int((sub > 0).sum()) == 0:
        return []
    band_polys = {}
    for geo, val in shapes(sub.astype("int16"), transform=t):
        val = int(val)
        if val:
            band_polys.setdefault(val, []).append(shape(geo))
    out = []
    for val, plist in sorted(band_polys.items()):
        merged = unary_union(plist).intersection(geom).simplify(0.008)
        if merged.is_empty:
            continue
        if merged.geom_type == "Polygon":
            merged = MultiPolygon([merged])
        elif merged.geom_type != "MultiPolygon":
            polys = [g for g in merged.geoms if isinstance(g, Polygon)] if hasattr(merged, "geoms") else []
            if not polys:
                continue
            merged = MultiPolygon(polys)
        out.append({"band": val, "min_in": BANDS[val - 1], "geom": mapping(merged)})
    return out


def extract_points(inches, geom, floor=POINT_FLOOR):
    minx, miny, maxx, maxy = geom.bounds
    c0 = int((minx - GRID_WEST) / GRID_RES); r0 = int((GRID_NORTH - maxy) / GRID_RES)
    c1 = int((maxx - GRID_WEST) / GRID_RES) + 1; r1 = int((GRID_NORTH - miny) / GRID_RES) + 1
    sub = inches[r0:r1, c0:c1]
    # Ceiling: MESH cells above 8 inches are radar artifacts (hail spikes /
    # three-body scatter) — the world-record hailstone is 8". Discard them so
    # they can't poison swaths, points, or per-day maxima.
    ys, xs = np.where((sub >= floor) & (sub <= 8.0))
    pg = prep(geom); out = []
    for yy, xx in zip(ys.tolist(), xs.tolist()):
        lon = GRID_WEST + (c0 + xx) * GRID_RES + GRID_RES / 2
        lat = GRID_NORTH - (r0 + yy) * GRID_RES - GRID_RES / 2
        if pg.contains(Point(lon, lat)):
            out.append({"lon": round(lon, 3), "lat": round(lat, 3),
                        "v": round(float(sub[yy, xx]), 2)})
    return out


def cold_season_check(state, date_iso, anchor_utc):
    """(skip?, reason). Cold-month artifact guard — see COLD_MONTHS note."""
    ab = STATE_ABBR.get(state)
    if not ab:
        return (False, "")
    try:
        sts = (anchor_utc - dt.timedelta(hours=24)).strftime("%Y-%m-%dT%H:%MZ")
        ets = anchor_utc.strftime("%Y-%m-%dT%H:%MZ")
        url = (f"https://mesonet.agron.iastate.edu/geojson/lsr.py?sts={sts}&ets={ets}"
               f"&states={ab}&type=H")
        gj = json.loads(urllib.request.urlopen(url, timeout=60).read())
        if gj.get("features"):
            return (False, "")   # ground-truth hail exists — always keep
    except Exception:
        return (False, "")       # can't check reports — keep the day
    try:
        url = f"https://mesonet.agron.iastate.edu/api/1/daily.json?network={ab}_ASOS&date={date_iso}"
        data = json.loads(urllib.request.urlopen(url, timeout=60).read()).get("data", [])
        temps = [r.get("max_tmpf") for r in data if isinstance(r.get("max_tmpf"), (int, float))]
        if temps and max(temps) < 45.0:
            return (True, f"no hail LSR + statewide max temp {max(temps):.0f}F < 45F (bright-band signature)")
    except Exception:
        pass
    return (False, "")


def db_day_states(base, anon, date_iso):
    """How many states already have hail rows for date_iso (-1 = unknown)."""
    try:
        r = requests.get(f"{base}/rest/v1/hail_days",
                         params={"valid_date": f"eq.{date_iso}", "select": "state"},
                         headers={"apikey": anon, "Authorization": f"Bearer {anon}",
                                  "Prefer": "count=exact", "Range": "0-0"},
                         timeout=30)
        cr = r.headers.get("Content-Range", "")
        return int(cr.split("/")[-1]) if "/" in cr else -1
    except Exception:
        return -1


def rpc(base, anon, name, payload):
    last = ""
    for attempt in range(4):
        try:
            r = requests.post(f"{base}/rest/v1/rpc/{name}",
                              headers={"apikey": anon, "Authorization": f"Bearer {anon}",
                                       "Content-Type": "application/json"},
                              data=json.dumps(payload), timeout=120)
            if r.status_code < 300:
                return r
            last = f"{name} {r.status_code}: {r.text[:200]}"
        except Exception as e:
            last = f"{name} exception: {e}"
        time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(last)


def process_local_date(local_date, states, base, anon, secret, pause=0.4):
    """Ingest one LOCAL calendar date for the given states. States are grouped
    by their UTC anchor so each distinct anchor file is downloaded once
    (typically 4-5 downloads for all 48 states)."""
    date_iso = f"{local_date[:4]}-{local_date[4:6]}-{local_date[6:]}"
    groups = {}
    for st in states:
        if st not in PERMITTED_STATES:
            print(f"  [skip] {st} not permitted")
            continue
        groups.setdefault(window_end_utc(st, local_date), []).append(st)

    stored = 0
    for anchor, group_states in sorted(groups.items()):
        vals = fetch_mesh_at(anchor)
        if vals is None:
            print(f"  {date_iso}: no MESH file for anchor {anchor.isoformat()}; "
                  f"skipping {len(group_states)} state(s)")
            continue
        cls, inches = classify(vals)
        wend = anchor.isoformat()
        for st in group_states:
            try:
                geom = load_state_geom(st)
                bands = build_bands(cls, geom)
                if not bands:
                    continue
                pts = extract_points(inches, geom)
                if not pts:
                    continue
                max_in = max(p["v"] for p in pts)
                if int(date_iso[5:7]) in COLD_MONTHS:
                    skip, why = cold_season_check(st, date_iso, anchor)
                    if skip:
                        print(f"  {date_iso}  {st:16s} [cold-season artifact guard] skipped: {why}")
                        continue
                rpc(base, anon, "ingest_swath",
                    {"p_secret": secret, "p_state": st, "p_date": date_iso,
                     "p_max_in": max_in, "p_features": bands, "p_window_end": wend})
                for i in range(0, len(pts), 4000):
                    rpc(base, anon, "ingest_points",
                        {"p_secret": secret, "p_state": st, "p_date": date_iso,
                         "p_points": pts[i:i+4000], "p_append": i > 0,
                         "p_window_end": wend})
                stored += 1
                print(f"  {date_iso}  {st:16s} max {max_in}\" ({len(bands)} band(s), "
                      f"{len(pts)} pts, window end {wend})")
                time.sleep(pause)
            except Exception as e:
                print(f"  [error] {date_iso} {st}: {e}")
    if stored == 0:
        print(f"{date_iso}: no on-land hail >= {POINT_FLOOR}\" in selected state(s).")
    return stored


def main():
    base = os.environ["SUPABASE_URL"].rstrip("/")
    anon = os.environ["SUPABASE_ANON_KEY"]
    secret = os.environ["INGEST_SECRET"]
    pause = float(os.environ.get("STATE_PAUSE", "0.4") or "0.4")

    raw = os.environ.get("INGEST_DATE")
    dates = []
    if raw:
        for tok in [d.strip() for d in raw.split(",") if d.strip()]:
            if ":" in tok:
                a, b = tok.split(":")
                d0 = dt.datetime.strptime(a, "%Y%m%d").date()
                d1 = dt.datetime.strptime(b, "%Y%m%d").date()
                cur = d0
                while cur <= d1:
                    dates.append(cur.strftime("%Y%m%d")); cur += dt.timedelta(days=1)
            else:
                dates.append(tok)
    else:
        # Scheduled run: yesterday, PLUS SELF-HEAL (2026-08-27): re-ingest any
        # of the 2 days before that with ZERO ingested states — catches late
        # archive files and skipped Actions runs (the 2026-08-26 nationwide
        # miss: the 09:00 run found no MESH files yet, exited "success", and
        # nothing ever retried). A genuinely quiet national day is re-checked
        # harmlessly (run costs ~1 min and writes nothing).
        # Load discipline (2026-08-27): a day is (re)ingested ONLY while the DB
        # has zero states for it — so the morning run does the real work and
        # the afternoon self-heal pass costs three count queries (~seconds)
        # unless something actually failed. No duplicate daily rewrites.
        today = dt.datetime.now(UTC).date()
        for back in (1, 2, 3):
            d = today - dt.timedelta(days=back)
            if db_day_states(base, anon, d.strftime("%Y-%m-%d")) == 0:
                if back > 1:
                    print(f"[self-heal] {d} has zero ingested states — re-running that date")
                dates.append(d.strftime("%Y%m%d"))
        if not dates:
            print("All recent dates already ingested — nothing to do.")
    states_env = os.environ.get("STATES")
    states = ([s.strip() for s in states_env.split(",")] if states_env
              else sorted(PERMITTED_STATES))

    print(f"MESH ingest v3 (local-clock days): {len(dates)} date(s), {len(states)} state(s)")
    total = 0
    for d in dates:
        total += process_local_date(d, states, base, anon, secret, pause)

    try:
        rpc(base, anon, "purge_old_hail", {"p_secret": secret})
        print("Rolling purge: removed hail data older than 3 years.")
    except Exception as e:
        print(f"[warn] purge failed: {e}")
    print(f"Done. {total} state-day(s) written across {len(dates)} date(s).")


if __name__ == "__main__":
    main()
