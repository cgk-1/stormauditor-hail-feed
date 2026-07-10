#!/usr/bin/env python3
"""
Timeout-proof self-walking backfill for HAIL (v3 local-clock days).

Walks BACKWARD from yesterday to 3 years ago, one LOCAL date at a time, against
a wall-clock budget (default 100 min; job limit 115). Progress is saved to
Supabase after EVERY date via backfill_get/set (key='hail'), so nothing is ever
lost and the next scheduled run resumes exactly where the last stopped.
Each date = ~4 anchor-file downloads covering all 48 states.

As it walks, it overwrites each state-day with correctly-dated v3 data, so the
old UTC-convention history is progressively replaced newest-first.

Env: SUPABASE_URL, SUPABASE_ANON_KEY, INGEST_SECRET
Optional: TIME_BUDGET_MIN (default 100), START_DATE/END_DATE (YYYYMMDD)
"""
import os, json, time, datetime as dt
import requests
import mesh_ingest as m


def rpc(base, anon, name, payload):
    r = requests.post(f"{base}/rest/v1/rpc/{name}",
        headers={"apikey": anon, "Authorization": f"Bearer {anon}",
                 "Content-Type": "application/json"},
        data=json.dumps(payload), timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"{name} {r.status_code}: {r.text[:200]}")
    return r.json() if r.text else None


def main():
    t0 = time.time()
    budget_s = 60 * int((os.environ.get("TIME_BUDGET_MIN") or "100").strip() or "100")

    base   = os.environ["SUPABASE_URL"].rstrip("/")
    anon   = os.environ["SUPABASE_ANON_KEY"]
    secret = os.environ["INGEST_SECRET"]

    today = dt.date.today()
    end   = dt.datetime.strptime(os.environ["END_DATE"], "%Y%m%d").date() \
            if os.environ.get("END_DATE") else today - dt.timedelta(days=1)
    start = dt.datetime.strptime(os.environ["START_DATE"], "%Y%m%d").date() \
            if os.environ.get("START_DATE") else today - dt.timedelta(days=1095)  # 3 years

    cur = rpc(base, anon, "backfill_get", {"p_key": "hail"})
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
        ds = day.strftime("%Y%m%d")
        try:
            n = m.process_local_date(ds, states, base, anon, secret)
            print(f"  {day}: {n} state-day(s) written  [{int(time.time()-t0)}s elapsed]")
        except Exception as e:
            print(f"  [error] {day}: {e} -- advancing past it")
        rpc(base, anon, "backfill_set", {"p_key": "hail", "p_value": day.strftime("%Y-%m-%d")})
        cursor = day
        done += 1


if __name__ == "__main__":
    main()
