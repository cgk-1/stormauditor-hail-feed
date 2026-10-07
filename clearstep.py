"""Clear-step (Archive Phase 5 Stage 4, owner-approved 2026-10-07; Stage 3 report
decision 6). IDENTICAL copy in hail-feed, wind-feed and hazard-engine.

Problem: the ingest RPCs replace what a write re-supplies, state by state. A
state that had rows for a day and is NOT re-supplied (no hail / no wind >= floor
any more, e.g. after a v4 re-date moved its cells to the neighbouring day) keeps
its old day/polygon/point rows forever.

Fix: after a FULLY successful day write on an EXPLICIT-DATE run (repairs,
backfill, re-dates), call the additive RPC hz_day_clear_states_v2 once for the
day. It deletes that source's day rows (days table, polygons, points) for the
states of the run's scope that were NOT written, keeps obs_only wind rescue
rows, re-runs the wind obs rescue for cleared wind states, keeps a copy of
every deleted row in hz_day_clear_log, and un-archives the day through the
existing feed-guard function first when it is archived.

Never called:
  * on scheduled / dispatched runs without a date (the nightly writes new days);
  * on a day with ANY failed scope, error or deferral (partial days keep what
    they have; the run is red anyway and is re-run);
  * in DRY_RUN (the plan is recorded in the day's FEED_RESULT entry as
    "clear_step" instead, so dry runs show exactly what would be cleared);
  * with CLEAR_STEP=0 (escape hatch; the workflows expose it as an input).

The call goes through run._post, NOT run.write: it is not part of the
recorded payload, so payload md5s stay comparable with earlier code.

REDO SHADOW (owner decision 2026-10-07, option C; migration
20261007170000_phase5_stage4_redo_shadow.sql). On the same explicit-date,
non-DRY_RUN runs, before_day() snapshots the (source, day) BEFORE anything is
written (hz_redo_snapshot copies the day's raw + archive rows into the shadow
tables). If the snapshot fails or is refused (disk guard), the day is NOT
written. A held snapshot from an earlier run or from the operator is reused
(status 'exists'): the original pre-redo state is never overwritten. After the
write and the clear-step, postcheck() compares the day's live row counts with
the payload (hz_redo_counts); the shadow is released only when that passes AND
REDO_RELEASE=1. By default it is kept for release after the blind verifier.
REDO_SNAPSHOT=0 is an emergency off switch (never for repairs). Run id:
REDO_RUN_ID, else "<feed>-<GITHUB_RUN_ID>".
"""
import os

RPC = "hz_day_clear_states_v2"


def enabled():
    return (os.environ.get("CLEAR_STEP") or "1").strip() != "0"


_DAY_TABLE = {"HAIL": ("hail_days", "valid_date", {}),
              "ANL": ("wind_days", "valid_date", {"obs_only": "is.false"}),
              "HRRR": None}         # hz_hrrr_meta is not readable with the feeds' anon key


def _existing(run, src, key, cand):
    """Dry-run preview (read-only, best effort): which candidate states have a
    feed day row in the DB now. Points without a day row are not listed here
    (the RPC clears them too)."""
    if not cand:
        return []
    if not (run.base and run.anon):
        return "unknown (no DB credentials in this dry run)"
    if _DAY_TABLE.get(src) is None:
        return f"not previewed ({src} day table is not readable with the feed key)"
    import requests
    table, col, extra = _DAY_TABLE[src]
    try:
        r = requests.get(f"{run.base}/rest/v1/{table}",
                         params=dict({col: f"eq.{key}", "select": "state"}, **extra),
                         headers={"apikey": run.anon, "Authorization": f"Bearer {run.anon}"}, timeout=30)
        if r.status_code >= 300 or not isinstance(r.json(), list):
            return f"unknown (HTTP {r.status_code})"
        return sorted({x["state"] for x in r.json()} & set(cand))
    except Exception as e:
        return f"unknown ({type(e).__name__})"


def after_day(run, key, src, scope_states, written_scopes, *, explicit, max_states=10):
    """Run the clear-step for one day if (and only if) it is allowed.

    src            'HAIL' | 'ANL' | 'HRRR'
    scope_states   the states this run processed for the day (STATES or all permitted)
    written_scopes run.day(key)["written"] (non-state scopes are ignored)
    """
    d = run.day(key)
    scope = sorted(set(scope_states))
    keep = sorted(set(written_scopes) & set(scope))
    cand = sorted(set(scope) - set(keep))
    plan = {"rpc": RPC, "src": src, "scope_states": len(scope), "keep_states": len(keep),
            "candidates": len(cand)}
    if not explicit:
        return None
    if not enabled():
        plan["skipped"] = "CLEAR_STEP=0"
        d["clear_step"] = plan
        run.note(key, f"clear-step {src}: disabled (CLEAR_STEP=0)")
        return None
    if d["status"] in ("error", "deferred") or d["failed"]:
        plan["skipped"] = f"day not fully successful (status {d['status']}, failed {d['failed'][:5]})"
        d["clear_step"] = plan
        run.note(key, f"clear-step {src}: NOT run - {plan['skipped']}")
        return None
    if run.dry_run:
        plan["dry_run"] = True
        plan["would_clear"] = _existing(run, src, key, cand)
        d["clear_step"] = plan
        print(f"  [clear-step] {key} {src}: DRY RUN - would clear {plan['would_clear']} "
              f"(keep {len(keep)}, {len(cand)} candidate state(s) not re-supplied)", flush=True)
        return None
    try:
        r = run._post(RPC, {"p_secret": run.secret, "p_src": src, "p_date": str(key),
                            "p_keep_states": keep, "p_scope_states": scope,
                            "p_max_states": max_states}, 120)
        res = r.json()
    except Exception as e:
        plan["error"] = f"{type(e).__name__}: {e}"
        d["clear_step"] = plan
        run.error(key, "clear-step", f"{src}: {plan['error']} (the day's new rows ARE written; "
                                     f"stale rows of states not re-supplied may remain - re-run the date)")
        return None
    plan["result"] = res
    d["clear_step"] = plan
    cleared = (res or {}).get("cleared_states") or []
    if cleared:
        run.note(key, f"clear-step {src}: cleared stale rows of {cleared} ({(res or {}).get('deleted')})")
    else:
        print(f"  [clear-step] {key} {src}: nothing stale", flush=True)
    return res


# ------------------------------------------------------------------ redo shadow
SNAP_RPC = "hz_redo_snapshot"


def redo_run_id(run):
    rid = (os.environ.get("REDO_RUN_ID") or "").strip()
    return rid or f"{run.feed}-{os.environ.get('GITHUB_RUN_ID') or 'local'}"


def before_day(run, key, src, *, explicit):
    """Snapshot (src, day) before an explicit-date write. True = the day may be written."""
    if not explicit:
        return True
    d = run.day(key)
    plan = {"rpc": SNAP_RPC, "src": src, "run_id": redo_run_id(run)}
    if (os.environ.get("REDO_SNAPSHOT") or "1").strip() == "0":
        plan["skipped"] = "REDO_SNAPSHOT=0"
        d["redo"] = plan
        run.warn(key, f"redo snapshot {src}: DISABLED (REDO_SNAPSHOT=0) - the old rows are not kept")
        return True
    if run.dry_run:
        plan["dry_run"] = True
        d["redo"] = plan
        print(f"  [redo] {key} {src}: DRY RUN - would snapshot the day before writing "
              f"(run id {plan['run_id']})", flush=True)
        return True
    try:
        r = run._post(SNAP_RPC, {"p_secret": run.secret, "p_src": src, "p_date": str(key),
                                 "p_run_id": plan["run_id"],
                                 "p_max_db_gb": float(os.environ.get("REDO_MAX_DB_GB") or 20)}, 120)
        res = r.json() or {}
    except Exception as e:
        plan["error"] = f"{type(e).__name__}: {e}"
        d["redo"] = plan
        run.error(key, "redo-snapshot", f"{src}: {plan['error']} - day NOT written (take the snapshot "
                                        f"through the management API, then re-run)")
        return False
    plan.update({"status": res.get("status"), "snap_id": res.get("snap_id"),
                 "held_run_id": res.get("run_id"), "counts": res.get("counts")})
    d["redo"] = plan
    if res.get("status") not in ("taken", "exists"):
        run.error(key, "redo-snapshot", f"{src}: snapshot {res.get('status')} ({res.get('reason')}, "
                                        f"db {res.get('db_bytes')} bytes) - day NOT written")
        return False
    print(f"  [redo] {key} {src}: snapshot {res.get('status')} (snap {res.get('snap_id')}, "
          f"run {res.get('run_id')}): {res.get('counts')}", flush=True)
    return True


def postcheck(run, key, src, expect):
    """After a fully successful write (+ clear-step): live counts vs the payload.
    expect = {table: expected_rows} (only tables whose count the write fully decides).
    Releases the shadow only when every count matches AND REDO_RELEASE=1."""
    d = run.day(key)
    redo = d.get("redo")
    if not redo or run.dry_run or redo.get("status") not in ("taken", "exists"):
        return None
    if d["status"] in ("error", "deferred") or d["failed"]:
        redo["postcheck"] = "skipped: day not fully successful - shadow KEPT"
        run.note(key, f"redo {src}: day not fully successful - shadow kept (restore with hz_redo_restore)")
        return None
    try:
        live = run._post("hz_redo_counts", {"p_secret": run.secret, "p_src": src, "p_date": str(key)}, 120).json()
    except Exception as e:
        redo["postcheck"] = f"counts unreadable ({type(e).__name__}: {e}) - shadow KEPT"
        run.warn(key, f"redo {src}: {redo['postcheck']}")
        return None
    diffs = {t: {"live": live.get(t), "payload": n} for t, n in expect.items() if live.get(t) != n}
    redo["postcheck"] = {"ok": not diffs, "live": live, "diffs": diffs}
    if diffs:
        run.warn(key, f"redo {src}: live row counts differ from the payload {diffs} - shadow KEPT for review")
        return False
    if (os.environ.get("REDO_RELEASE") or "0").strip() == "1":
        try:
            rel = run._post("hz_redo_release", {"p_secret": run.secret, "p_src": src, "p_date": str(key),
                                                "p_run_id": redo.get("held_run_id") or redo["run_id"]}, 120).json()
            redo["released"] = rel
        except Exception as e:
            run.warn(key, f"redo {src}: release failed ({type(e).__name__}: {e}); shadow kept")
    else:
        run.note(key, f"redo {src}: counts match; shadow kept until the blind verifier passes "
                      f"(release: hz_redo_release / hz_redo_release_run '{redo.get('held_run_id') or redo['run_id']}')")
    return True


def rows(run, key, name):
    """Payload rows recorded for one RPC payload key (e.g. 'ingest_points.p_points')."""
    return run.day(key)["rows"].get(name, 0)
