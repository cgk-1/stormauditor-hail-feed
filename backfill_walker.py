#!/usr/bin/env python3
"""
Timeout-proof self-walking backfill for HAIL (v3 local-clock days).

Walks BACKWARD from yesterday to 3 years ago, one LOCAL date at a time, against
a wall-clock budget (default 100 min; job limit 115). Progress is saved to
Supabase after EVERY COMPLETED date via backfill_get/set (key='hail'), so the
next scheduled run resumes exactly where the last stopped.
Each date = ~4 anchor-file downloads covering all 48 states.

2026-10-07 (Archive Phase 5, Stage 1): the walker STOPS on the first date that
did not ingest completely (missing/invalid upstream file, a failed state) and
does NOT move the cursor past it - it used to log the error and advance, which
left permanent silent holes. The run exits non-zero; fix the cause (or record
the date as a known gap) and the next run retries the same date.
This workflow is disabled (Connor); this change only matters if it is re-enabled.

Env: SUPABASE_URL, SUPABASE_ANON_KEY, INGEST_SECRET
Optional: TIME_BUDGET_MIN (default 100), START_DATE/END_DATE (YYYYMMDD), DRY_RUN
"""
import os, time, datetime as dt
import feedguard as fg
import mesh_ingest as m


def walk(run):
    t0 = time.time()
    budget_s = 60 * int((os.environ.get("TIME_BUDGET_MIN") or "100").strip() or "100")

    today = dt.date.today()
    end   = dt.datetime.strptime(os.environ["END_DATE"], "%Y%m%d").date() \
            if os.environ.get("END_DATE") else today - dt.timedelta(days=1)
    start = dt.datetime.strptime(os.environ["START_DATE"], "%Y%m%d").date() \
            if os.environ.get("START_DATE") else today - dt.timedelta(days=1095)  # 3 years

    cur = run.rpc_read("backfill_get", {"p_key": "hail", "p_secret": run.secret}).json()
    cursor = dt.datetime.strptime(cur, "%Y-%m-%d").date() if cur else end + dt.timedelta(days=1)

    states = sorted(m.PERMITTED_STATES)
    done = 0
    print(f"Hail walker start. Budget {budget_s//60} min. Resuming before {cursor}. Floor {start}.")

    while True:
        day = cursor - dt.timedelta(days=1)
        if day < start:
            print(f"Hail backfill COMPLETE: reached {start}.")
            break
        if time.time() - t0 > budget_s:
            print(f"Time budget reached after {done} date(s). Next run resumes before {cursor}.")
            break
        n = m.process_local_date(run, day.strftime("%Y%m%d"), states)
        if run.day(day.isoformat())["status"] == "error":
            print(f"  [STOP] {day} did not ingest completely; cursor stays at {cursor}.")
            break
        print(f"  {day}: {n} state-day(s) written  [{int(time.time()-t0)}s elapsed]")
        if not run.dry_run:
            run._post("backfill_set", {"p_key": "hail", "p_value": day.strftime("%Y-%m-%d"),
                                       "p_secret": run.secret}, 60)
        cursor = day
        done += 1


if __name__ == "__main__":
    try:
        _run = fg.Run("hail-backfill")
    except fg.FeedError as e:
        print(f"::error::{e}")
        raise SystemExit(2)
    fg.main_guard(_run, walk)
