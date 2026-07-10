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
    ys, xs = np.where(sub >= floor)
    pg = prep(geom); out = []
    for yy, xx in zip(ys.tolist(), xs.tolist()):
        lon = GRID_WEST + (c0 + xx) * GRID_RES + GRID_RES / 2
        lat = GRID_NORTH - (r0 + yy) * GRID_RES - GRID_RES / 2
        if pg.contains(Point(lon, lat)):
            out.append({"lon": round(lon, 3), "lat": round(lat, 3),
                        "v": round(float(sub[yy, xx]), 2)})
    return out


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
    raw = os.environ.get("INGEST_DATE") or \
        (dt.datetime.now(UTC).date() - dt.timedelta(days=1)).strftime("%Y%m%d")
    dates = []
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

    base = os.environ["SUPABASE_URL"].rstrip("/")
    anon = os.environ["SUPABASE_ANON_KEY"]
    secret = os.environ["INGEST_SECRET"]
    pause = float(os.environ.get("STATE_PAUSE", "0.4") or "0.4")
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
