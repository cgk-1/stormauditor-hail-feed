# stormauditor-hail-feed

## Operations (Archive Phase 5 Stage 1, 2026-10-07)
- `DATE=YYYY-MM-DD` or `DATE=START..END` (also `YYYYMMDD`, `a:b`, comma lists; workflow input `date`). Blank = scheduled mode (yesterday + zero-state self-heal of D-2/D-3).
- `DRY_RUN=1` (workflow input `dry_run`): computes everything and writes nothing; per-day row counts and payload md5 go to the job summary, `feed-out/result.json`, and `feed-out/payloads/*.jsonl.gz`.
- The last log line is `FEED_RESULT {json}`: expected vs received anchors and states, states written/empty/failed, and rows plus payload md5 per day. A backfill driver checks it per day.
- Any unexpected input (missing or invalid MESH file, bad state payload, failed cold-guard check, failed RPC) means that state-day is not written, a quarantine record is saved in `feed-out/quarantine/` (uploaded as an artifact), and the job exits non-zero. Complete states are still written.
- MESH comes from IEM, or from AWS `noaa-mrms-pds` when IEM lacks the file (gzip bytes are identical).
- Scheduled runs defer the newest day while an anchor is not published yet. Explicit dates fail instead.
- `COLD_GUARD=0|1` (default 1). Deps are pinned in `requirements.txt`. State boundaries are vendored and md5-checked in `data/`.
- `feedguard.py` is shared verbatim with the wind-feed and hazard-engine repos; keep the copies identical.
