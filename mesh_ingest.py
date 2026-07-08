#!/usr/bin/env python3
"""
MRMS MESH -> Supabase hail-swath ingester for stormauditor.com (Lovable Cloud).

Works WITHOUT a direct database URL. It pushes finished swath polygons into your
Lovable-managed Supabase by calling a locked SQL function (`ingest_swath`) over
the public REST API, authenticated with your anon key + a shared secret. This is
required because Lovable Cloud does not expose the direct Postgres connection
string or service-role key.

For a given UTC date: download ONE national MRMS MESH_Max_1440min grid (24h max
estimated hail size, ~1 km), classify into inch bands, polygonize, clip to each
permitted state, simplify, and POST each state's result to ingest_swath. Empty
state-days (no on-land hail >= 0.75") are skipped.

This is the ONLY component that touches GRIB2 - it runs in GitHub Actions (free),
never in Supabase or Lovable.

Env vars required (set as GitHub repo secrets):
  SUPABASE_URL        e.g. https://abcdxyz.supabase.co
  SUPABASE_ANON_KEY   the anon / publishable key from your Lovable app
  INGEST_SECRET       a random password you also store in the app_config table
Optional:
  INGEST_DATE         YYYYMMDD (default: yesterday UTC)
  STATES              comma list of state names (default: all permitted)

Deps: pygrib numpy rasterio shapely requests
"""
import os, gzip, json, datetime as dt
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
POINT_FLOOR = 0.50   # store raw MESH cell values >= this (inches) for precise lookups
GRID_NORTH, GRID_WEST, GRID_RES = 55.0, -130.0, 0.01

PERMITTED_STATES = {
    "Alabama","Arizona","Arkansas","California","Colorado","Connecticut","Delaware",
    "Florida","Georgia","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky",
    "Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi",
    "Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico",
    "New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania",
    "Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont",
    "Virginia","Washington","West Virginia","Wisconsin","Wyoming",
}

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


def fetch_mesh(date_str):
    y, m, d = date_str[:4], date_str[4:6], date_str[6:]
    # UTC calendar day (23:30Z 24h-max file). Kept deliberately: the existing
    # 2-year dataset was built on this convention and validates well against
    # commercial tools; changing to 06Z mid-table would date storms
    # inconsistently. The 72-hour date-of-loss window absorbs most
    # day-convention differences when comparing against 6am-6am reports.
    url = (f"https://mtarchive.geol.iastate.edu/{y}/{m}/{d}/mrms/ncep/"
           f"MESH_Max_1440min/MESH_Max_1440min_00.50_{date_str}-233000.grib2.gz")
    try:
        raw = urllib.request.urlopen(url, timeout=120).read()
    except Exception as e:
        print(f"  [warn] could not fetch {url}: {e}")
        return None
    with open("/tmp/_mesh.grib2", "wb") as fh:
        fh.write(gzip.decompress(raw))
    g = pygrib.open("/tmp/_mesh.grib2")
    vals = g[1].values
    g.close()
    return np.asarray(vals)


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


def build_bands(cls, inches, geom):
    sub, t = _window(cls, geom)
    if int((sub > 0).sum()) == 0:
        return [], 0.0
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
            merged = MultiPolygon([merged])          # column is MultiPolygon-typed
        elif merged.geom_type != "MultiPolygon":
            polys = [g for g in merged.geoms if isinstance(g, Polygon)] if hasattr(merged, "geoms") else []
            if not polys:
                continue
            merged = MultiPolygon(polys)
        out.append({"band": val, "min_in": BANDS[val - 1], "geom": mapping(merged)})
    sub_in, _ = _window(inches, geom)
    max_in = round(float(sub_in.max()), 2) if sub_in.size else 0.0
    return out, max_in


def extract_points(inches, geom, floor=POINT_FLOOR):
    """Return raw MESH cell values (inches) inside the state as {lon,lat,v} list."""
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


def push_points(base, anon, secret, state, date_iso, points):
    r = requests.post(
        f"{base}/rest/v1/rpc/ingest_points",
        headers={"apikey": anon, "Authorization": f"Bearer {anon}",
                 "Content-Type": "application/json"},
        data=json.dumps({"p_secret": secret, "p_state": state,
                         "p_date": date_iso, "p_points": points}),
        timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"points {r.status_code}: {r.text[:200]}")


def push(base, anon, secret, state, date_iso, bands, max_in):
    r = requests.post(
        f"{base}/rest/v1/rpc/ingest_swath",
        headers={"apikey": anon, "Authorization": f"Bearer {anon}",
                 "Content-Type": "application/json"},
        data=json.dumps({"p_secret": secret, "p_state": state, "p_date": date_iso,
                         "p_max_in": max_in, "p_features": bands}),
        timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"{r.status_code}: {r.text[:300]}")


def process_date(date_str, states, base, anon, secret):
    date_iso = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    vals = fetch_mesh(date_str)
    if vals is None:
        print(f"{date_iso}: no MESH grid available; skipping.")
        return 0
    cls, inches = classify(vals)
    stored = 0
    for st in states:
        if st not in PERMITTED_STATES:
            print(f"  [skip] {st} not permitted (outside MRMS coverage)")
            continue
        try:
            geom = load_state_geom(st)
            bands, _bbox_max = build_bands(cls, inches, geom)
            if not bands:
                continue
            pts = extract_points(inches, geom)
            if not pts:
                continue   # bands existed but no in-state cells >= floor
            # state max = largest exact in-state cell value (never bbox/offshore)
            max_in = max(p["v"] for p in pts)
            push(base, anon, secret, st, date_iso, bands, max_in)
            push_points(base, anon, secret, st, date_iso, pts)
            stored += 1
            print(f"  {date_iso}  {st:16s} pushed {len(bands)} band(s), max {max_in}\"")
        except Exception as e:
            print(f"  [error] {date_iso} {st}: {e}")
    if stored == 0:
        print(f"{date_iso}: no on-land hail >= 0.75\" in selected state(s).")
    return stored


def main():
    raw = os.environ.get("INGEST_DATE") or \
        (dt.datetime.utcnow().date() - dt.timedelta(days=1)).strftime("%Y%m%d")
    # INGEST_DATE may be a single date or a comma-separated list (for backfill)
    dates = [d.strip() for d in raw.split(",") if d.strip()]
    base = os.environ["SUPABASE_URL"].rstrip("/")
    anon = os.environ["SUPABASE_ANON_KEY"]
    secret = os.environ["INGEST_SECRET"]
    states_env = os.environ.get("STATES")
    states = ([s.strip() for s in states_env.split(",")] if states_env
              else sorted(PERMITTED_STATES))

    print(f"Ingesting {len(dates)} date(s) across {len(states)} state(s)")
    total = 0
    for d in dates:
        total += process_date(d, states, base, anon, secret)
    print(f"Done. {total} state-day(s) written across {len(dates)} date(s).")


if __name__ == "__main__":
    main()
