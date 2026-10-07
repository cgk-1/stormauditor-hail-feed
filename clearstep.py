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
"""
import os

RPC = "hz_day_clear_states_v2"


def enabled():
    return (os.environ.get("CLEAR_STEP") or "1").strip() != "0"


_DAY_TABLE = {"HAIL": ("hail_days", "valid_date", {}),
              "ANL": ("wind_days", "valid_date", {"obs_only": "is.false"}),
              "HRRR": ("hz_hrrr_meta", "date", {})}


def _existing(run, src, key, cand):
    """Dry-run preview (read-only, best effort): which candidate states have a
    feed day row in the DB now. Points without a day row are not listed here
    (the RPC clears them too)."""
    if not cand:
        return []
    if not (run.base and run.anon):
        return "unknown (no DB credentials in this dry run)"
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
