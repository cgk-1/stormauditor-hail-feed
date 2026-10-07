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

STAGE 1 HARDENING (Archive Phase 5, 2026-10-07) - the math is unchanged; the
payloads are byte-identical to the previous version on normal days:
  * Upstream fallback: IEM mtarchive is the primary source; when it does not
    have an anchor file (it holds nothing before 2022-06-15) the SAME file is
    read from AWS noaa-mrms-pds (gzip bytes proven identical on overlap dates).
    Never a neighbouring file: a missing anchor fails its states loudly.
  * Strict validation of every MESH file (message count, discipline/category/
    parameter, 7000x3500 0.01-degree grid at 54.995N/129.995W, scan order,
    valid time = anchor, value codes, coverage) and of every state's output
    (lattice, bounds, value range, duplicates). Anything unexpected: that
    state-day is NOT written, a quarantine record is saved, the run fails.
  * Per-state failures are no longer swallowed: complete states are still
    written, the job ends non-zero and the summary lists both.
  * Every anchor of a day is fetched before anything is written. Scheduled
    runs DEFER the newest day (write nothing, green run) while an anchor is
    not published yet; the next dispatch's zero-state self-heal ingests it.
    Explicit DATE runs fail instead. (At 10:10Z all anchors exist: the last,
    08Z in winter, lands ~08:08Z.)
  * Cold-season guard (COLD_GUARD=1 default, 0 = off): its IEM checks retry and
    a failed check now fails that state-day instead of silently keeping it.
  * State boundaries are vendored (data/us-states.json, md5-checked).
  * DRY_RUN=1, DATE=YYYY-MM-DD|START..END, completeness summary and the
    FEED_RESULT json line: see feedguard.py.

DAY_CONVENTION=v4 (Archive Phase 5 Stage 3, 2026-10-07; default v3 = unchanged):
  every MRMS cell uses its OWN zone's local day (tzwin.py, data/tz/tz_mrms.npz)
  instead of its state's zone (plan T1: Pensacola/Panama City CT, El Paso MT,
  west KS/NE/SD/ND MT, north ID PT, west KY CT, east TN ET, MI UP CT, ...).
  * The zone windows are the same 4-5 anchors the state groups already fetch
    (no extra downloads); each cell takes the MESH value of its own zone's
    anchor; classify/despeckle runs once on that national composite.
  * window_end_utc is stored PER POINT (its own zone's window end; the archive
    already keeps a window index per point). hail_days.window_end_utc keeps the
    state's own zone window (as v3).
  * DST days (plan T2): the 24 h MESH_Max_1440min file is not the local day.
    Fall-back (25 h): max(1440 file at the window end, MESH_Max_60min ending at
    start+1 h). Spring-forward (23 h): the 1440 file covers one hour of the
    previous day; cells whose 1440 value is only reached in that hour
    (MESH_Max_60min ending at start >= 1440) take the max of the 23 hourly
    MESH_Max_60min files of the day instead; every other cell keeps the 1440
    value (max-of-hourly is NOT identical to the 1440 product: tested 2025-05-19,
    1,175 of 24.5M cells lower, never higher). 60-min files: AWS noaa-mrms-pds
    only (~53 KB each; IEM does not archive them). Missing file = the day fails.
  * Test flags (DRY_RUN only): V4_ZONES=state, V4_DST=0 (see tzwin.py).

STAGE 4 (owner-approved 2026-10-07): the workflow passes DAY_CONVENTION=v4 on
every schedule/dispatch unless a dispatch sets day_convention=v3 (the code's
own default stays v3, so local runs without the variable are unchanged).
Explicit-date runs end each fully successful day with the clear-step
(clearstep.py, RPC hz_day_clear_states_v2): states of the run's scope that were
not re-supplied lose their stale hail_days/hail_polygons/hail_points rows.
CLEAR_STEP=0 turns it off; DRY_RUN only reports what it would clear.

Env (GitHub secrets): SUPABASE_URL, SUPABASE_ANON_KEY, INGEST_SECRET
Optional: DATE / INGEST_DATE (local dates), STATES, STATE_PAUSE (sec between
          states, default 0.4), COLD_GUARD (1/0), DRY_RUN (1/0), FEED_OUT_DIR,
          DAY_CONVENTION (v3|v4), V4_ZONES / V4_DST (dry-run test flags), CLEAR_STEP (1/0)

Deps: requirements.txt (exact pins)
"""
import os, gzip, json, time, tempfile, datetime as dt
from zoneinfo import ZoneInfo
import numpy as np
import pygrib
from rasterio.features import shapes
from rasterio.transform import from_origin
from shapely.geometry import shape, mapping, Point, MultiPolygon, Polygon
from shapely.prepared import prep
from shapely.ops import unary_union

import clearstep
import feedguard as fg
import tzwin

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
# day AND the statewide max temperature stayed below 45F. Since 2026-10-07 a
# check that cannot be completed (IEM down after retries, unexpected reply)
# FAILS that state-day loudly instead of keeping it silently.
COLD_MONTHS = {11, 12, 1, 2, 3}

# State boundaries: PublicaMundi us-states.json (52 features), vendored
# 2026-10-07 from raw.githubusercontent.com/PublicaMundi/MappingAPI/master/
# data/geojson/us-states.json - byte-identical to what every earlier run
# downloaded at runtime. A changed file would silently change which cells
# belong to a state, so the md5 is enforced.
HERE = os.path.dirname(os.path.abspath(__file__))
BOUNDARY_FILE = os.path.join(HERE, "data", "us-states.json")
BOUNDARY_MD5 = "56968c4d4db9777511c4fd363e684a5c"
_CACHE = {}
_ALL_STATES_CACHE = None

# MESH_Max_1440min sources. Identical gzip bytes (md5-verified 2023-01-01,
# 2024-05-28, 2026-10-05). IEM has nothing before 2022-06-15.
MESH_IEM_URL = ("https://mtarchive.geol.iastate.edu/{y}/{m}/{d}/mrms/ncep/MESH_Max_1440min/"
                "MESH_Max_1440min_00.50_{ds}-{ts}.grib2.gz")
MESH_AWS_URL = ("https://noaa-mrms-pds.s3.amazonaws.com/CONUS/MESH_Max_1440min_00.50/{ds}/"
                "MRMS_MESH_Max_1440min_00.50_{ds}-{ts}.grib2.gz")
# v4 DST days only (plan T2): hourly maxima, AWS only (IEM keeps no 60-min archive).
MESH60_AWS_URL = ("https://noaa-mrms-pds.s3.amazonaws.com/CONUS/MESH_Max_60min_00.50/{ds}/"
                  "MRMS_MESH_Max_60min_00.50_{ds}-{ts}.grib2.gz")
MESH60_PARAM = 30          # MESH_Max_60min parameterNumber (1440 min = 34)

# What every MESH_Max_1440min file must look like (identical 2021-10 -> 2026-10).
MESH_INT_KEYS = {"discipline": 209, "parameterCategory": 3, "parameterNumber": 34,
                 "Ni": 7000, "Nj": 3500, "jScansPositively": 0, "iScansNegatively": 0}
MESH_FLOAT_KEYS = {"latitudeOfFirstGridPointInDegrees": 54.995,
                   "longitudeOfFirstGridPointInDegrees": 230.005,
                   "iDirectionIncrementInDegrees": 0.01, "jDirectionIncrementInDegrees": 0.01}
MESH_CODES = {-3.0, -1.0}       # -3 = no radar coverage, -1 = no hail
MESH_MAX_MM = fg.env_float("MESH_MAX_MM", 1000.0)          # encoding sanity, not physics
MESH_MAX_NOCOV = fg.env_float("MESH_MAX_NOCOV_FRAC", 0.50)  # observed 0.33-0.39 (2021-2026)
MESH_PUBLISH_GRACE_H = fg.env_float("MESH_PUBLISH_GRACE_H", 12.0)


def load_state_geom(name):
    global _ALL_STATES_CACHE
    if name in _CACHE:
        return _CACHE[name]
    if _ALL_STATES_CACHE is None:
        with open(BOUNDARY_FILE, "rb") as fh:
            raw = fh.read()
        import hashlib
        got = hashlib.md5(raw).hexdigest()
        if got != BOUNDARY_MD5:
            raise fg.ValidationError(f"state boundary file md5 {got} != pinned {BOUNDARY_MD5}")
        gj = json.loads(raw)
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


def fetch_mesh_raw(anchor_utc):
    """gzip bytes of the MESH_Max_1440min file ending at anchor_utc.
    IEM first, AWS when IEM does not have it (or keeps failing).
    Returns (raw, source, notes). Raises UpstreamMissing when neither has it."""
    ds = anchor_utc.strftime("%Y%m%d")
    ts = anchor_utc.strftime("%H%M%S")
    urls = [("IEM", MESH_IEM_URL.format(y=ds[:4], m=ds[4:6], d=ds[6:], ds=ds, ts=ts)),
            ("AWS", MESH_AWS_URL.format(ds=ds, ts=ts))]
    notes, missing = [], 0
    for src, url in urls:
        try:
            return fg.http_get(url, timeout=120, retries=4, what=f"MESH {src} {ds}-{ts}"), src, notes
        except fg.UpstreamMissing as e:
            missing += 1
            notes.append(str(e))
        except fg.UpstreamError as e:
            notes.append(str(e))
    if missing == len(urls):
        raise fg.UpstreamMissing(f"MESH anchor {ds}-{ts} is on neither IEM nor AWS")
    raise fg.UpstreamError(f"MESH anchor {ds}-{ts} could not be fetched: {' | '.join(notes)}")


def decode_mesh(raw, anchor_utc, param=34):
    """Decode + validate one MESH file. Returns the mm array exactly as before
    (np.asarray of the single message's values). param: GRIB parameterNumber
    (34 = MESH_Max_1440min, the only product v3 reads; 30 = MESH_Max_60min)."""
    try:
        grib = gzip.decompress(raw)
    except Exception as e:
        raise fg.ValidationError(f"MESH gzip unreadable: {e}")
    fd, path = tempfile.mkstemp(suffix=".grib2")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(grib)
        g = pygrib.open(path)
        try:
            if g.messages != 1:
                raise fg.ValidationError(f"MESH file has {g.messages} messages (expected 1)")
            m = g[1]
            for k, want in dict(MESH_INT_KEYS, parameterNumber=param).items():
                if int(m[k]) != want:
                    raise fg.ValidationError(f"MESH {k}={m[k]} (expected {want})")
            if m["gridType"] != "regular_ll":
                raise fg.ValidationError(f"MESH gridType={m['gridType']} (expected regular_ll)")
            for k, want in MESH_FLOAT_KEYS.items():
                if abs(float(m[k]) - want) > 1e-5:
                    raise fg.ValidationError(f"MESH {k}={m[k]} (expected {want})")
            vd, vt = int(m["validityDate"]), int(m["validityTime"])
            if (vd, vt) != (int(anchor_utc.strftime("%Y%m%d")), anchor_utc.hour * 100 + anchor_utc.minute):
                raise fg.ValidationError(f"MESH valid time {vd} {vt:04d} != anchor {anchor_utc.isoformat()}")
            raw_vals = m.values
            if np.ma.isMaskedArray(raw_vals) and np.ma.is_masked(raw_vals):
                raise fg.ValidationError("MESH field carries a bitmap/mask (not expected)")
            vals = np.asarray(raw_vals)
        finally:
            g.close()
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
    if vals.shape != (3500, 7000):
        raise fg.ValidationError(f"MESH grid shape {vals.shape} (expected (3500, 7000))")
    if not np.isfinite(vals).all():
        raise fg.ValidationError(f"MESH has {int((~np.isfinite(vals)).sum())} non-finite values")
    neg = set(np.unique(vals[vals < 0]).tolist())
    if not neg <= MESH_CODES:
        raise fg.ValidationError(f"MESH has unexpected negative codes {sorted(neg - MESH_CODES)[:5]}")
    vmax = float(vals.max())
    if vmax > MESH_MAX_MM:
        # A single spike is capped downstream (8-inch ceiling); don't fail the
        # whole anchor (~10 states) for it — say so loudly instead.
        print(f"::warning::MESH max {vmax} mm > {MESH_MAX_MM} (encoding?); values above the cap are clipped downstream")
    nocov = float((vals == -3).mean())
    if nocov > MESH_MAX_NOCOV:
        raise fg.ValidationError(f"MESH no-coverage fraction {nocov:.3f} > {MESH_MAX_NOCOV} "
                                 f"(radar outage?)")
    return vals


def fetch_mesh_at(anchor_utc):
    """Backward-compatible helper: validated mm array (raises on any problem)."""
    raw, _src, _notes = fetch_mesh_raw(anchor_utc)
    return decode_mesh(raw, anchor_utc)


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


def validate_state_output(state, geom, bands, pts):
    """The payload must look like every payload before it: on-lattice in-state
    points 0.50-8.00 in with no duplicates; ordered known bands of
    MultiPolygons. Raises ValidationError otherwise."""
    minx, miny, maxx, maxy = geom.bounds
    seen = set()
    for p in pts:
        lon, lat, v = p["lon"], p["lat"], p["v"]
        if not (POINT_FLOOR <= v <= 8.0):
            raise fg.ValidationError(f"{state}: point value {v} outside 0.50-8.00 in")
        if not (minx - 0.01 <= lon <= maxx + 0.01 and miny - 0.01 <= lat <= maxy + 0.01):
            raise fg.ValidationError(f"{state}: point {lon},{lat} outside the state bounds")
        if round(abs(lon) * 1000) % 10 != 5 or round(abs(lat) * 1000) % 10 != 5:
            raise fg.ValidationError(f"{state}: point {lon},{lat} is off the MRMS 0.01-degree lattice")
        if (lon, lat) in seen:
            raise fg.ValidationError(f"{state}: duplicate point {lon},{lat}")
        seen.add((lon, lat))
    last = 0
    for b in bands:
        if not (last < b["band"] <= len(BANDS)) or b["min_in"] != BANDS[b["band"] - 1]:
            raise fg.ValidationError(f"{state}: unexpected band {b['band']}/{b['min_in']}")
        if b["geom"]["type"] != "MultiPolygon" or not b["geom"]["coordinates"]:
            raise fg.ValidationError(f"{state}: band {b['band']} geometry {b['geom']['type']}")
        last = b["band"]


def cold_season_check(state, date_iso, anchor_utc):
    """(skip?, reason). Cold-month artifact guard — see COLD_MONTHS note.
    Raises when the evidence cannot be read; the caller then keeps the day (fail open)."""
    ab = STATE_ABBR.get(state)
    if not ab:
        return (False, "")
    sts = (anchor_utc - dt.timedelta(hours=24)).strftime("%Y-%m-%dT%H:%MZ")
    ets = anchor_utc.strftime("%Y-%m-%dT%H:%MZ")
    url = (f"https://mesonet.agron.iastate.edu/geojson/lsr.py?sts={sts}&ets={ets}"
           f"&states={ab}&type=H")
    gj = json.loads(fg.http_get(url, timeout=60, retries=4, what=f"cold-guard LSR {ab} {date_iso}"))
    if not isinstance(gj, dict) or not isinstance(gj.get("features"), list):
        raise fg.ValidationError(f"cold guard: unexpected LSR reply for {ab}")
    if gj["features"]:
        return (False, "")   # ground-truth hail exists — always keep
    url = f"https://mesonet.agron.iastate.edu/api/1/daily.json?network={ab}_ASOS&date={date_iso}"
    js = json.loads(fg.http_get(url, timeout=60, retries=4, what=f"cold-guard daily {ab} {date_iso}"))
    if not isinstance(js, dict) or not isinstance(js.get("data"), list):
        raise fg.ValidationError(f"cold guard: unexpected daily.json reply for {ab}")
    temps = [r.get("max_tmpf") for r in js["data"] if isinstance(r.get("max_tmpf"), (int, float))]
    if temps and max(temps) < 45.0:
        return (True, f"no hail LSR + statewide max temp {max(temps):.0f}F < 45F (bright-band signature)")
    return (False, "")


def process_local_date(run, local_date, states, pause=0.4, cold_guard=True, policy="strict"):
    """Ingest one LOCAL calendar date for the given states. States are grouped
    by their UTC anchor so each distinct anchor file is downloaded once
    (typically 4-5 downloads for all 48 states). Returns states written."""
    date_iso = f"{local_date[:4]}-{local_date[4:6]}-{local_date[6:]}"
    key = date_iso
    groups = {}
    for st in states:
        groups.setdefault(window_end_utc(st, local_date), []).append(st)
    run.expect(key, "states", len(states))
    run.expect(key, "anchors", len(groups))

    # 2026-10-07: every anchor file is fetched BEFORE anything is written. When
    # the newest day's anchors are not all published yet (MESH lands ~8 min
    # after the hour), a scheduled run (policy defer) writes nothing and the
    # zero-state self-heal of the next dispatch (12:10Z / 16:10Z) ingests it.
    raws, missing = {}, []
    for anchor in sorted(groups):
        try:
            raws[anchor] = fetch_mesh_raw(anchor)
        except fg.UpstreamMissing as e:
            missing.append(anchor)
            raws[anchor] = e
        except fg.FeedError as e:
            raws[anchor] = e
    if missing and policy == "defer" and all(
            dt.datetime.now(UTC) - a < dt.timedelta(hours=MESH_PUBLISH_GRACE_H) for a in missing):
        run.defer(key, f"MESH anchor(s) {[a.strftime('%m-%d %H%MZ') for a in missing]} not published "
                       f"yet; nothing written - the next dispatch ingests the day",
                  pending=[a.isoformat() for a in missing])
        return 0

    stored = 0
    for anchor, group_states in sorted(groups.items()):
        try:
            if isinstance(raws[anchor], Exception):
                raise raws[anchor]
            raw, src, notes = raws[anchor]
            vals = decode_mesh(raw, anchor)
        except fg.FeedError as e:
            run.error(key, f"anchor {anchor.strftime('%H%MZ')}", str(e),
                      details={"anchor_utc": anchor.isoformat(), "states": group_states})
            for st in group_states:
                run.day(key)["failed"].append(st)
            continue
        run.receive(key, "anchors")
        run.receive(key, f"anchors_{src}")
        if src != "IEM":
            run.note(key, f"anchor {anchor.isoformat()} read from {src} ({'; '.join(notes)})")
        cls, inches = classify(vals)
        wend = anchor.isoformat()
        for st in group_states:
            try:
                geom = load_state_geom(st)
                bands = build_bands(cls, geom)
                if not bands:
                    run.empty(key, st)
                    continue
                pts = extract_points(inches, geom)
                if not pts:
                    run.empty(key, st)
                    continue
                max_in = max(p["v"] for p in pts)
                if int(date_iso[5:7]) in COLD_MONTHS and cold_guard:
                    try:
                        skip, why = cold_season_check(st, date_iso, anchor)
                    except Exception as ge:
                        # Fail OPEN (pre-2026-10-07 behaviour): when the evidence
                        # cannot be read, keep the day rather than lose real hail.
                        skip, why = False, ""
                        run.warn(key, f"{st}: cold-season guard could not check ({type(ge).__name__}: {ge}); day kept")
                    if skip:
                        print(f"  {date_iso}  {st:16s} [cold-season artifact guard] skipped: {why}")
                        run.skip(key, "cold_guard", st)
                        continue
                validate_state_output(st, geom, bands, pts)
                calls = [("ingest_swath",
                          {"p_secret": run.secret, "p_state": st, "p_date": date_iso,
                           "p_max_in": max_in, "p_features": bands, "p_window_end": wend})]
                for i in range(0, len(pts), 4000):
                    calls.append(("ingest_points",
                                  {"p_secret": run.secret, "p_state": st, "p_date": date_iso,
                                   "p_points": pts[i:i+4000], "p_append": i > 0,
                                   "p_window_end": wend}))
                run.write(key, st, calls)
                run.written(key, st)
                stored += 1
                print(f"  {date_iso}  {st:16s} max {max_in}\" ({len(bands)} band(s), "
                      f"{len(pts)} pts, window end {wend})")
                if not run.dry_run:
                    time.sleep(pause)
            except Exception as e:
                run.error(key, st, f"{type(e).__name__}: {e}",
                          details={"anchor_utc": wend})
    run.set_received(key, "states_ok", len(run.day(key)["written"]) + len(run.day(key)["empty"])
                     + len(run.day(key)["skipped"].get("cold_guard", [])))
    if stored == 0 and not run.day(key)["failed"]:
        print(f"{date_iso}: no on-land hail >= {POINT_FLOOR}\" in selected state(s).")
    return stored


# ===================================================================== v4
# DAY_CONVENTION=v4 (Archive Phase 5 Stage 3). Everything below is used only
# when DAY_CONVENTION=v4; the v3 path above is unchanged.

def round_anchor(end):
    """The archive's 30-minute file cadence rounding of window_end_utc()."""
    if end.minute < 15:
        end = end.replace(minute=0)
    elif end.minute < 45:
        end = end.replace(minute=30)
    else:
        end = (end + dt.timedelta(hours=1)).replace(minute=0)
    return end.replace(second=0, microsecond=0)


def bbox_window(geom, margin=3):
    """Rows/cols of the MRMS lattice covering the state's bbox (+margin cells)."""
    minx, miny, maxx, maxy = geom.bounds
    c0 = max(0, int((minx - GRID_WEST) / GRID_RES) - margin)
    c1 = min(7000, int((maxx - GRID_WEST) / GRID_RES) + 1 + margin)
    r0 = max(0, int((GRID_NORTH - maxy) / GRID_RES) - margin)
    r1 = min(3500, int((GRID_NORTH - miny) / GRID_RES) + 1 + margin)
    return r0, r1, c0, c1


def cell_of(lon, lat):
    """(row, col) of the MRMS cell whose centre is (lon, lat) (3-dp rounded)."""
    return (int(round((GRID_NORTH - lat) / GRID_RES - 0.5)),
            int(round((lon - GRID_WEST) / GRID_RES - 0.5)))


def fetch_mesh60_raw(t):
    ds, ts = t.strftime("%Y%m%d"), t.strftime("%H%M%S")
    return fg.http_get(MESH60_AWS_URL.format(ds=ds, ts=ts), timeout=120, retries=4,
                       what=f"MESH60 AWS {ds}-{ts}")


def v4_needed_files(dg, g, dst_fix):
    """[(kind, utc time)] that group g's day field needs. kind 1440 = the
    MESH_Max_1440min anchor (ALWAYS also what v3 fetches for that window),
    60 = MESH_Max_60min (DST transition days only)."""
    start, end = dg.groups[g]
    need = [("1440", round_anchor(end))]
    hours = dg.hours(g)
    if dst_fix and hours == 25:
        need.append(("60", start + dt.timedelta(hours=1)))
    elif dst_fix and hours == 23:
        need += [("60", start + dt.timedelta(hours=k)) for k in range(0, 24)]
    elif hours != 24 and dst_fix:
        raise fg.ValidationError(f"unexpected {hours} h local day")
    return need


def v4_group_field(dg, g, files, dst_fix):
    """MESH (mm) field of group g's local day (plan T2 on DST days)."""
    start, end = dg.groups[g]
    anchor = round_anchor(end)
    f = decode_mesh(files[("1440", anchor)], anchor)
    hours = dg.hours(g)
    if not dst_fix or hours == 24:
        return f
    if hours == 25:     # 1440 file = (start+1h, end]; add the missing first hour
        t = start + dt.timedelta(hours=1)
        return np.fmax(f, decode_mesh(files[("60", t)], t, MESH60_PARAM))
    # 23 h: 1440 file = (start-1h, end] includes the previous day's last hour X.
    x = decode_mesh(files[("60", start)], start, MESH60_PARAM)
    c = None
    for k in range(1, 24):
        t = start + dt.timedelta(hours=k)
        v = decode_mesh(files[("60", t)], t, MESH60_PARAM)
        c = v if c is None else np.fmax(c, v, out=c)
    # where the 1440 value exceeds the extra hour it is reached inside the day:
    # keep it (exact); else the max may come from the extra hour -> 23 hourly files
    return np.where(f > x, f, c)


def process_local_date_v4(run, local_date, states, pause=0.4, cold_guard=True, policy="strict",
                          flags=None):
    """v4: each cell's own-zone local day. Same RPCs, payload layout and order
    as v3 (with V4_ZONES=state V4_DST=0 the payload is byte-identical to v3)."""
    flags = flags or {"zones": "real", "dst": True}
    real = flags["zones"] == "real"
    date_iso = f"{local_date[:4]}-{local_date[4:6]}-{local_date[6:]}"
    key = date_iso
    zm = tzwin.zone_map("mrms")
    geoms = {st: load_state_geom(st) for st in states}
    st_zone = {st: tzwin.zone_id(STATE_TZ[st]) for st in states}
    st_zones = {}
    for st in states:
        if real:
            r0, r1, c0, c1 = bbox_window(geoms[st])
            zs = set(np.unique(zm.zone[r0:r1, c0:c1]).tolist()) - {0}
        else:
            zs = set()
        st_zones[st] = zs | {st_zone[st]}
    dg = tzwin.DayGroups(local_date, set().union(*st_zones.values()))
    st_group = {st: tzwin.group_of_window(dg, tzwin.local_window(STATE_TZ[st], local_date))
                for st in states}
    st_groups = {st: sorted({int(dg.lut[z]) for z in st_zones[st]}) for st in states}
    anchors = {g: round_anchor(dg.groups[g][1]) for g in range(len(dg.groups))}
    used = sorted({g for st in states for g in st_groups[st]})
    run.day(key)["v4_groups"] = dg.describe()
    run.expect(key, "states", len(states))
    run.expect(key, "anchors", len({anchors[g] for g in used}))

    # Every file of every group is fetched BEFORE anything is written (as v3).
    files, missing, failed_g = {}, [], {}
    for g in used:
        for kind, t in v4_needed_files(dg, g, flags["dst"]):
            if (kind, t) in files:
                continue
            try:
                if kind == "1440":
                    raw, src, notes = fetch_mesh_raw(t)
                    if src != "IEM":
                        run.note(key, f"anchor {t.isoformat()} read from {src} ({'; '.join(notes)})")
                    run.receive(key, f"anchors_{src}")
                else:
                    raw = fetch_mesh60_raw(t)
                    run.receive(key, "mesh60_files")
                files[(kind, t)] = raw
            except fg.UpstreamMissing as e:
                missing.append(t)
                failed_g.setdefault(g, str(e))
            except fg.FeedError as e:
                failed_g.setdefault(g, str(e))
    if missing and policy == "defer" and all(
            dt.datetime.now(UTC) - a < dt.timedelta(hours=MESH_PUBLISH_GRACE_H) for a in missing):
        run.defer(key, f"MESH file(s) {sorted({a.strftime('%m-%d %H%MZ') for a in missing})} not "
                       f"published yet; nothing written - the next dispatch ingests the day",
                  pending=sorted({a.isoformat() for a in missing}))
        return 0

    def group_field(g):
        if g in failed_g:
            raise fg.ValidationError(failed_g[g])
        return v4_group_field(dg, g, files, flags["dst"])

    by_anchor = {}
    for st in states:
        by_anchor.setdefault(anchors[st_group[st]], []).append(st)

    # Real zones: one national composite per STATE window group: every cell takes
    # its own zone's field; cells with no US zone (> 1 deg from US land and
    # waters, never stored) keep the state's own window (= v3), so the pixel
    # union / clip / simplify of a one-zone state is byte-identical to v3.
    gfields, composites = {}, {}
    if real:
        gmap = dg.lut[zm.zone]
        for g in used:
            try:
                gfields[g] = group_field(g)
                run.receive(key, "anchors")
            except fg.FeedError as e:
                failed_g.setdefault(g, str(e))

    def national(g_state):
        if g_state not in composites:
            comp = tzwin.compose(gmap, gfields, gfields[g_state])
            composites[g_state] = classify(comp)
            del comp
        return composites[g_state]

    stored = 0
    for anchor, group_states in sorted(by_anchor.items()):
        cls = inches = None
        if not real:
            g = st_group[group_states[0]]
            try:
                cls, inches = classify(group_field(g))
                run.receive(key, "anchors")
            except fg.FeedError as e:
                failed_g.setdefault(g, str(e))
        for st in group_states:
            bad = [g for g in st_groups[st] if g in failed_g]
            if bad:
                run.error(key, st, f"zone window(s) {[dg.describe()[g]['zones'][0] for g in bad]} "
                                   f"unavailable: {failed_g[bad[0]]}",
                          details={"anchor_utc": anchor.isoformat()})
                continue
            try:
                geom = geoms[st]
                c_, i_ = national(st_group[st]) if real else (cls, inches)
                bands = build_bands(c_, geom)
                if not bands:
                    run.empty(key, st)
                    continue
                pts = extract_points(i_, geom)
                if not pts:
                    run.empty(key, st)
                    continue
                wends = []
                for p in pts:
                    if real:
                        r, c = cell_of(p["lon"], p["lat"])
                        z = int(zm.zone[r, c])
                        if z == 0 or dg.lut[z] == tzwin.NO_GROUP:
                            raise fg.ValidationError(f"{st}: stored cell {p['lon']},{p['lat']} has no "
                                                     f"US zone (zone id {z})")
                        wends.append(anchors[int(dg.lut[z])])
                    else:
                        wends.append(anchor)
                max_in = max(p["v"] for p in pts)
                if int(date_iso[5:7]) in COLD_MONTHS and cold_guard:
                    try:
                        skip, why = cold_season_check(st, date_iso, anchor)
                    except Exception as ge:
                        skip, why = False, ""
                        run.warn(key, f"{st}: cold-season guard could not check ({type(ge).__name__}: {ge}); day kept")
                    if skip:
                        print(f"  {date_iso}  {st:16s} [cold-season artifact guard] skipped: {why}")
                        run.skip(key, "cold_guard", st)
                        continue
                validate_state_output(st, geom, bands, pts)
                calls = [("ingest_swath",
                          {"p_secret": run.secret, "p_state": st, "p_date": date_iso,
                           "p_max_in": max_in, "p_features": bands, "p_window_end": anchor.isoformat()})]
                first = True
                for we in sorted(set(wends)):
                    sub = [p for p, w in zip(pts, wends) if w == we]
                    for i in range(0, len(sub), 4000):
                        calls.append(("ingest_points",
                                      {"p_secret": run.secret, "p_state": st, "p_date": date_iso,
                                       "p_points": sub[i:i+4000], "p_append": not first,
                                       "p_window_end": we.isoformat()}))
                        first = False
                run.write(key, st, calls)
                run.written(key, st)
                stored += 1
                other = sum(1 for w in wends if w != anchor)
                print(f"  {date_iso}  {st:16s} max {max_in}\" ({len(bands)} band(s), "
                      f"{len(pts)} pts, window end {anchor.isoformat()}"
                      f"{f'; {other} pts in another zone' if other else ''})")
                if not run.dry_run:
                    time.sleep(pause)
            except Exception as e:
                run.error(key, st, f"{type(e).__name__}: {e}", details={"anchor_utc": anchor.isoformat()})
    run.set_received(key, "states_ok", len(run.day(key)["written"]) + len(run.day(key)["empty"])
                     + len(run.day(key)["skipped"].get("cold_guard", [])))
    if stored == 0 and not run.day(key)["failed"]:
        print(f"{date_iso}: no on-land hail >= {POINT_FLOOR}\" in selected state(s).")
    return stored


def parse_states():
    states_env = (os.environ.get("STATES") or "").strip()
    if not states_env:
        return sorted(PERMITTED_STATES)
    states = [s.strip() for s in states_env.split(",") if s.strip()]
    bad = [s for s in states if s not in PERMITTED_STATES]
    if bad:
        raise fg.ValidationError(f"STATES has unknown/not permitted state(s): {bad}")
    return states


def main(run):
    pause = float(os.environ.get("STATE_PAUSE", "0.4") or "0.4")
    cold_guard = (os.environ.get("COLD_GUARD") or "1").strip() != "0"
    run.meta.update({"boundary_md5": BOUNDARY_MD5, "cold_guard": cold_guard,
                     "numpy": np.__version__, "pygrib": pygrib.__version__})
    conv = tzwin.convention()
    flags = tzwin.test_flags(run.dry_run) if conv == "v4" else None
    run.meta["day_convention"] = conv
    if conv == "v4":
        run.meta.update({"tzwin_md5": tzwin.module_md5(), "zone_map_md5": tzwin.zone_map("mrms").md5,
                         "v4_flags": flags})

    explicit = fg.requested_dates()
    dates = []
    if explicit is not None:
        dates = [d.strftime("%Y%m%d") for d in explicit]
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
        # 2026-10-07: a count that cannot be read is an error (it used to be
        # treated as "not zero" and the day was silently skipped).
        today = dt.datetime.now(UTC).date()
        for back in (1, 2, 3):
            d = today - dt.timedelta(days=back)
            try:
                n = run.rest_count("hail_days", {"valid_date": f"eq.{d.isoformat()}", "select": "state"})
            except Exception as e:
                run.error(d.isoformat(), "self-heal check", str(e), quarantine=False)
                continue
            if n == 0:
                if back > 1:
                    print(f"[self-heal] {d} has zero ingested states — re-running that date")
                dates.append(d.strftime("%Y%m%d"))
        if not dates:
            print("All recent dates already ingested — nothing to do.")
    states = parse_states()

    print(f"MESH ingest {conv} ({'state-zone' if conv == 'v3' else 'own-zone'} local-clock days): "
          f"{len(dates)} date(s), {len(states)} state(s)"
          f"{'' if cold_guard else ' [COLD_GUARD=0]'}{f' {flags}' if flags else ''}")
    total = 0
    policy = "strict" if explicit is not None else "defer"
    for d in dates:
        if conv == "v4":
            total += process_local_date_v4(run, d, states, pause, cold_guard, policy, flags)
        else:
            total += process_local_date(run, d, states, pause, cold_guard, policy)
        # Stage 4 clear-step (explicit-date runs only, fully successful days only):
        # states of this run's scope that were NOT re-supplied lose their stale
        # hail_days / hail_polygons / hail_points rows (clearstep.py).
        key = f"{d[:4]}-{d[4:6]}-{d[6:]}"
        clearstep.after_day(run, key, "HAIL", states, run.day(key)["written"],
                            explicit=explicit is not None)

    if not run.dry_run:
        try:
            run._post("purge_old_hail", {"p_secret": run.secret}, 120)
            print("Rolling purge: removed hail data older than 3 years.")
        except Exception as e:
            print(f"[warn] purge failed: {e}")
    print(f"Done. {total} state-day(s) written across {len(dates)} date(s).")


if __name__ == "__main__":
    try:
        _run = fg.Run("hail")
    except fg.FeedError as e:
        print(f"::error::{e}")
        raise SystemExit(2)
    fg.main_guard(_run, main)
