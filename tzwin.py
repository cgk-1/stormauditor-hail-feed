"""tzwin.py - v4 per-cell LOCAL-DAY support for the StormAuditor feeds.

Archive Phase 5, Stage 3 (2026-10-07). ONE IDENTICAL COPY lives in each feed repo
(stormauditor-hail-feed, -wind-feed, -hazard-engine), like feedguard.py; the md5 of
this file is recorded in every run's FEED_RESULT meta.

DAY CONVENTIONS
  v3 (default, nightly today): a "day" is the local calendar day of the STATE's
     dominant zone (STATE_TZ in each feed) for every cell of that state.
  v4 (DAY_CONVENTION=v4): every grid cell / point uses its OWN zone's local day
     [local 00:00 D, local 00:00 D+1), DST-aware (23 h / 25 h on transition days).
     The zone of a grid cell comes from the vendored zone map data/tz/tz_<grid>.npz
     (timezone-boundary-builder 2026d; docs/tz-foundation.md in the site repo),
     md5-checked against data/tz/MANIFEST.json. Zones with identical UTC windows
     are grouped (CONUS: never more than 5 groups: ET, CT, MT, MST-AZ, PT), each
     group's field is built from the hours/files the state groups already fetch
     (no extra downloads), and each cell takes the value of its own group.
     The archive encoding and the state partition (which state a cell is stored
     under) do not change.

TEST FLAGS (parity tests only; refused unless DRY_RUN=1)
  V4_ZONES=state  every cell uses its STATE's zone (refactor-identity test: with
                  V4_DST=0 the v4 code must reproduce v3 byte for byte).
  V4_DST=0        keep v3's DST-day handling (hail: the 24 h MESH file; HRRR: 24
                  hours) so the identity test isolates the zone logic.

GUARDS (fail loudly, never fall back silently)
  * zone map / manifest md5 mismatch, unexpected shape or zone table;
  * more than MAX_GROUPS distinct windows for one CONUS date;
  * a state-clipped cell (a stored point) whose zone is 0 (no US zone).
"""
import datetime as dt
import hashlib
import json
import os
from zoneinfo import ZoneInfo

import numpy as np

import feedguard as fg

UTC = dt.timezone.utc
HERE = os.path.dirname(os.path.abspath(__file__))
TZ_DIR = os.path.join(HERE, "data", "tz")
MAX_GROUPS = 5          # CONUS: ET, CT, MT, MST (Arizona), PT
NO_GROUP = 255


# ------------------------------------------------------------------ convention
def convention():
    """'v3' (default) or 'v4' from DAY_CONVENTION. Anything else is an error."""
    v = (os.environ.get("DAY_CONVENTION") or "").strip().lower() or "v3"
    if v not in ("v3", "v4"):
        raise fg.ValidationError(f"DAY_CONVENTION={v!r} (use v3 or v4)")
    return v


def test_flags(dry_run):
    """V4 test flags. Non-default values are allowed only in dry runs."""
    zones = (os.environ.get("V4_ZONES") or "real").strip().lower()
    dst = (os.environ.get("V4_DST") or "1").strip()
    if zones not in ("real", "state") or dst not in ("0", "1"):
        raise fg.ValidationError(f"V4_ZONES={zones!r} / V4_DST={dst!r} (use real|state, 1|0)")
    flags = {"zones": zones, "dst": dst == "1"}
    if (zones != "real" or dst != "1") and not dry_run:
        raise fg.ValidationError("V4_ZONES / V4_DST test flags are only allowed with DRY_RUN=1")
    return flags


def module_md5():
    return fg.file_md5(os.path.abspath(__file__))


# ------------------------------------------------------------------ zone maps
_MANIFEST = None


def manifest():
    global _MANIFEST
    if _MANIFEST is None:
        with open(os.path.join(TZ_DIR, "MANIFEST.json")) as fh:
            _MANIFEST = json.load(fh)
        if not isinstance(_MANIFEST.get("zones"), list) or _MANIFEST["zones"][0] != "":
            raise fg.ValidationError("tz MANIFEST.json has no zone table")
    return _MANIFEST


def zone_names():
    """id -> IANA (index 0 = '' = no US zone)."""
    return list(manifest()["zones"])


def zone_id(iana):
    names = zone_names()
    if iana not in names[1:]:
        raise fg.ValidationError(f"zone {iana!r} is not in the zone table")
    return names.index(iana)


class ZoneMap:
    """uint8 zone id per grid cell (0 = no US zone), md5-verified."""

    def __init__(self, grid):
        fname = f"tz_{grid}.npz"
        path = os.path.join(TZ_DIR, fname)
        ent = manifest().get(fname)
        if not ent:
            raise fg.ValidationError(f"{fname} is not listed in MANIFEST.json")
        got = fg.file_md5(path)
        if got != ent["md5_file"]:
            raise fg.ValidationError(f"{fname} md5 {got} != manifest {ent['md5_file']}")
        with np.load(path, allow_pickle=False) as z:
            zone = np.ascontiguousarray(z["zone"])
            zones = [str(x) for x in z["zones"].tolist()]
            inner = str(z["md5"])
        if zone.dtype != np.uint8 or list(zone.shape) != list(ent["shape"]):
            raise fg.ValidationError(f"{fname}: zone array {zone.dtype} {zone.shape} (expected uint8 {ent['shape']})")
        zb = hashlib.md5(zone.tobytes()).hexdigest()
        if zb != ent["md5_zone_bytes"] or zb != inner:
            raise fg.ValidationError(f"{fname}: zone bytes md5 {zb} != manifest {ent['md5_zone_bytes']}")
        if zones != zone_names():
            raise fg.ValidationError(f"{fname}: zone table differs from MANIFEST.json")
        if int(zone.max()) >= len(zones):
            raise fg.ValidationError(f"{fname}: zone id {int(zone.max())} outside the table")
        self.grid, self.zone, self.names, self.md5 = grid, zone, zones, zb


_MAPS = {}


def zone_map(grid):
    if grid not in _MAPS:
        _MAPS[grid] = ZoneMap(grid)
    return _MAPS[grid]


# ------------------------------------------------------------------ windows
def local_window(iana, date_str):
    """Exact UTC [start, end) of local calendar day D (YYYYMMDD) in iana."""
    tz = ZoneInfo(iana)
    y, m, d = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
    start = dt.datetime(y, m, d, tzinfo=tz)
    end = start + dt.timedelta(days=1)            # wall-clock +1 day (zoneinfo: DST-aware)
    return start.astimezone(UTC), end.astimezone(UTC)


class DayGroups:
    """Zone ids present -> groups of identical UTC windows for one local date.

    groups[g] = (start_utc, end_utc); lut[zone_id] = g (NO_GROUP for ids not
    present / zone 0). Groups are ordered by (start, end)."""

    def __init__(self, date_str, zone_ids, max_groups=MAX_GROUPS):
        names = zone_names()
        zone_ids = sorted({int(z) for z in zone_ids if int(z) != 0})
        win = {z: local_window(names[z], date_str) for z in zone_ids}
        self.groups = sorted(set(win.values()))
        if len(self.groups) > max_groups:
            raise fg.ValidationError(f"{date_str}: {len(self.groups)} distinct local-day windows "
                                     f"(max {max_groups}): {[names[z] for z in zone_ids]}")
        self.lut = np.full(256, NO_GROUP, dtype=np.uint8)
        for z, w in win.items():
            self.lut[z] = self.groups.index(w)
        self.zone_ids = zone_ids
        self.members = {g: [names[z] for z in zone_ids if self.lut[z] == g] for g in range(len(self.groups))}

    def hours(self, g):
        s, e = self.groups[g]
        return int(round((e - s).total_seconds() / 3600))

    def describe(self):
        return [{"start_utc": s.isoformat(), "end_utc": e.isoformat(), "hours": self.hours(g),
                 "zones": self.members[g]} for g, (s, e) in enumerate(self.groups)]


def group_of_window(day_groups, window):
    """Index of the group with exactly this (start, end); ValidationError if none."""
    if window not in day_groups.groups:
        raise fg.ValidationError(f"window {window[0].isoformat()}..{window[1].isoformat()} is not a "
                                 f"group window of this date")
    return day_groups.groups.index(window)


def compose(gmap, fields, fill):
    """Per-cell selection: out[c] = fields[gmap[c]][c]; cells whose group is not
    in `fields` (NO_GROUP, zone 0) get `fill` (an array or scalar)."""
    keys = sorted(fields)
    first = fields[keys[0]]
    out = np.empty_like(first)
    out[...] = fill
    for g in keys:
        m = gmap == g
        out[m] = fields[g][m]
    return out
