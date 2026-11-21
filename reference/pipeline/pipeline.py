#!/usr/bin/env python3
"""
Shopping Sessions: DuckDB reference implementation of rules.md.

Reads  DATA_DIR    (env, or second-to-last argument; default /data)
Writes OUTPUT_DIR  (env, or last argument; default /output): user_daily/ and campaign_daily/
Temp   SCRATCH_DIR (env; default a fresh temp dir)

DuckDB's CSV reader does not follow the rules' dialect, so every file is read as raw
lines (one VARCHAR column, no quoting) and the dialect is applied in SQL.
"""
import glob
import json
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import duckdb

if len(sys.argv) >= 3:
    DATA, OUT = sys.argv[-2], sys.argv[-1]
else:
    DATA = os.environ.get("DATA_DIR", "/data")
    OUT = os.environ.get("OUTPUT_DIR", "/output")
_tmp = None if os.environ.get("SCRATCH_DIR") else tempfile.TemporaryDirectory(prefix="ref-scratch-")
SCRATCH = os.environ.get("SCRATCH_DIR") or _tmp.name
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 7 * DAY_MS
CR_SENTINEL = b"\x02"  # stands in for \r, which DuckDB's reader always treats as a line end

con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.75))}GB'")
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")


def ms(iso):
    return int(datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()) * 1000


with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)
WS, WE = ms(manifest["window_start"]), ms(manifest["window_end"])


def has_cr(path):
    with open(path, "rb") as f:
        while chunk := f.read(1 << 24):
            if b"\r" in chunk:
                return True
    return False


def readable(path):
    """The file itself, or a scratch copy with every \\r swapped for the sentinel."""
    if not has_cr(path):
        return path
    dst = os.path.join(SCRATCH, "cr", os.path.relpath(path, DATA))
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(path, "rb") as src, open(dst, "wb") as out:
        out.write(src.read().replace(b"\r", CR_SENTINEL))
    return dst


def files(pattern):
    paths = sorted(glob.glob(os.path.join(DATA, pattern)))
    with ThreadPoolExecutor(NCPU) as pool:
        return list(pool.map(readable, paths))


FIELD = '(?:"[^",]*"|[^",]*)'


def lines(paths, nfields):
    """SQL relation `f`: the fields of each well-formed data line, quotes removed, '' for empty."""
    if not paths:
        return f"(SELECT NULL::VARCHAR[] AS f WHERE false)"
    plist = "[" + ",".join("'" + p.replace("'", "''") + "'" for p in paths) + "]"
    pattern = FIELD + "(?:," + FIELD + "){" + str(nfields - 1) + "}"
    return f"""(
        SELECT string_split(replace(replace(l, '"', ''), chr(2), chr(13)), ',') AS f
        FROM read_csv({plist}, columns={{'l': 'VARCHAR'}}, header=true, delim='\\x01',
                      quote='', escape='', new_line='\\n', auto_detect=false, strict_mode=false)
        WHERE regexp_full_match(l, '{pattern}')
    )"""


# Timestamp -> epoch ms, NULL when not a valid F1/F2/F3 (F3 longer than 15 significant
# digits is far outside any window, so it maps to NULL too).
con.execute(r"""
CREATE MACRO leap(y) AS (y % 4 = 0 AND y % 100 <> 0) OR y % 400 = 0;
CREATE MACRO dim(y, m) AS CASE WHEN m = 2 THEN CASE WHEN leap(y) THEN 29 ELSE 28 END
                               WHEN m IN (4, 6, 9, 11) THEN 30 ELSE 31 END;
CREATE MACRO civil_days(y, m, d) AS
    ((y - (m <= 2)::INT) // 400) * 146097
    + ((y - (m <= 2)::INT) % 400) * 365 + ((y - (m <= 2)::INT) % 400) // 4 - ((y - (m <= 2)::INT) % 400) // 100
    + (153 * ((m + 9) % 12) + 2) // 5 + d - 1 - 719468;
CREATE MACRO iso_ms(y, mo, d, h, mi, s, sign, oh, om) AS
    CASE WHEN y >= 1 AND mo BETWEEN 1 AND 12 AND d BETWEEN 1 AND dim(y, mo)
              AND h < 24 AND mi < 60 AND s < 60 AND oh < 24 AND om < 60
         THEN civil_days(y, mo, d)::BIGINT * 86400000 + h * 3600000 + mi * 60000 + s * 1000
              - sign * (oh * 3600000 + om * 60000) END;
CREATE MACRO ts_ms(t) AS CASE
    WHEN regexp_full_match(t, '[0-9]+') THEN
        CASE WHEN length(ltrim(t, '0')) <= 15 THEN try_cast(t AS BIGINT) END
    WHEN regexp_full_match(t, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})') THEN
        iso_ms(substr(t, 1, 4)::INT, substr(t, 6, 2)::INT, substr(t, 9, 2)::INT,
               substr(t, 12, 2)::INT, substr(t, 15, 2)::INT, substr(t, 18, 2)::INT,
               CASE substr(t, 20, 1) WHEN '+' THEN 1 WHEN '-' THEN -1 ELSE 0 END,
               coalesce(try_cast(substr(t, 21, 2) AS INT), 0), coalesce(try_cast(substr(t, 24, 2) AS INT), 0))
END;
""")

# ---- lookups ----
con.execute(f"""CREATE TABLE products AS SELECT f[1] AS product_id, f[2] AS category
                FROM {lines(files('products.csv'), 2)}""")
con.execute(f"""CREATE TABLE campaigns AS SELECT f[1] AS campaign_id, f[2] AS channel
                FROM {lines(files('campaigns.csv'), 2)}""")

# ---- linked IDs: connected components, named by the byte-wise smallest ID ----
con.execute(f"""CREATE TABLE link_pairs AS SELECT DISTINCT f[1] AS a, f[2] AS b
                FROM {lines(files('links/*.csv'), 2)} WHERE f[1] <> '' AND f[2] <> ''""")
# Codes follow byte-wise ID order, so the smallest code in a component is its name.
con.execute("""CREATE TABLE nodes AS
               SELECT id, row_number() OVER (ORDER BY id) AS code
               FROM (SELECT a AS id FROM link_pairs UNION SELECT b FROM link_pairs)""")
con.execute("""CREATE TABLE edges AS
               SELECT DISTINCT x.code AS a, y.code AS b
               FROM (SELECT a AS p, b AS q FROM link_pairs UNION ALL SELECT b, a FROM link_pairs
                     UNION ALL SELECT a, a FROM link_pairs UNION ALL SELECT b, b FROM link_pairs) e
               JOIN nodes x ON x.id = e.p JOIN nodes y ON y.id = e.q""")
con.execute("CREATE TABLE lab AS SELECT code AS id, code AS l FROM nodes")
# Min-label propagation + hooking (a root takes the min any of its members saw) + pointer
# jumping. Every step only lowers labels within a component, so a fixpoint means every edge
# has equal labels, i.e. each node carries its component's minimum code.
rounds = 0
while True:
    rounds += 1
    con.execute("""CREATE OR REPLACE TABLE lab2 AS
                   SELECT e.a AS id, min(lab.l) AS l FROM edges e JOIN lab ON lab.id = e.b GROUP BY e.a""")
    con.execute("""CREATE OR REPLACE TABLE lab2 AS
                   SELECT x.id, least(x.l, h.l) AS l FROM lab2 x LEFT JOIN
                   (SELECT lab.l AS id, min(lab2.l) AS l FROM lab JOIN lab2 USING (id) GROUP BY lab.l) h USING (id)""")
    for _ in range(2):
        con.execute("""CREATE OR REPLACE TABLE lab2 AS
                       SELECT x.id, least(x.l, y.l) AS l FROM lab2 x JOIN lab2 y ON y.id = x.l""")
    changed = con.execute("SELECT count(*) FROM lab2 JOIN lab USING (id) WHERE lab2.l <> lab.l").fetchone()[0]
    con.execute("CREATE OR REPLACE TABLE lab AS SELECT * FROM lab2")
    if changed == 0:
        break
con.execute("""CREATE TABLE person_of AS
               SELECT n.id, m.id AS person FROM lab JOIN nodes n ON n.code = lab.id JOIN nodes m ON m.code = lab.l""")

# ---- events: parse, dedup (smallest ingest_ts), validate, drop late ----
con.execute(f"""
CREATE TABLE events AS
WITH parsed AS (
    SELECT f[1] AS event_id, f[2] AS uid, ts_ms(f[3]) AS ts, ts_ms(f[4]) AS ing, f[5] AS typ, f[6] AS pid, f[7] AS price
    FROM {lines(files('events/*.csv'), 7)}
    WHERE f[1] <> ''
), kept AS (
    SELECT arg_min({{'uid': uid, 'ts': ts, 'ing': ing, 'typ': typ, 'pid': pid, 'price': price}}, ing) AS k
    FROM parsed GROUP BY event_id
), valid AS (
    SELECT k.uid, k.ts, k.typ, k.pid,
           CASE WHEN k.typ = 'purchase' THEN try_cast(k.price AS BIGINT) ELSE 0 END AS price
    FROM kept
    WHERE k.uid <> '' AND k.ts >= {WS} AND k.ts < {WE}
      AND k.typ IN ('view', 'cart', 'purchase') AND k.pid <> ''
      AND (k.typ <> 'purchase' OR (regexp_full_match(k.price, '[0-9]+') AND length(ltrim(k.price, '0')) <= 8
                                   AND try_cast(k.price AS BIGINT) BETWEEN 1 AND 10000000))
      AND k.ing - k.ts <= {DAY_MS}
)
SELECT coalesce(p.person, v.uid) AS person, v.ts, v.typ = 'purchase' AS is_purchase, v.price,
       coalesce(pr.category, 'unknown') AS category
FROM valid v LEFT JOIN person_of p ON p.id = v.uid LEFT JOIN products pr ON pr.product_id = v.pid
""")

# ---- sessions: gap > 30 min between distinct timestamps; equal ts share a session ----
con.execute(f"""
CREATE TABLE ev_sess AS
SELECT person, ts, is_purchase, price, category,
       max(CASE WHEN prev IS NULL OR ts - prev > {GAP_MS} THEN ts END)
           OVER (PARTITION BY person ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS start
FROM (SELECT *, lag(ts) OVER (PARTITION BY person ORDER BY ts) AS prev FROM events)
""")
con.execute(f"""
CREATE TABLE sessions AS
SELECT person, start, (DATE '1970-01-01' + (start // {DAY_MS})::INT) AS day,
       count(*) AS n_events, sum(price) AS revenue, bool_or(is_purchase) AS purchased,
       row_number() OVER (PARTITION BY person ORDER BY start) AS rk
FROM ev_sess GROUP BY person, start
""")

# ---- clicks: parse, validate (dedup is moot: copies are identical) ----
con.execute(f"""
CREATE TABLE clicks AS
SELECT DISTINCT coalesce(p.person, c.uid) AS person, c.ts, c.campaign_id, c.click_id
FROM (SELECT f[1] AS click_id, f[2] AS uid, f[3] AS campaign_id, ts_ms(f[4]) AS ts
      FROM {lines(files('clicks/*.csv'), 4)} WHERE f[1] <> '') c
JOIN campaigns k ON k.campaign_id = c.campaign_id
LEFT JOIN person_of p ON p.id = c.uid
WHERE c.uid <> '' AND c.ts >= {WS - WEEK_MS} AND c.ts < {WE}
""")

# ---- credit: latest click at or before start within 7 days; tie -> smallest click_id ----
con.execute(f"""
CREATE TABLE credited AS
WITH best AS (
    SELECT person, ts, arg_min(campaign_id, click_id) AS campaign_id FROM clicks GROUP BY person, ts
)
SELECT s.person, s.start, s.day, s.n_events, s.revenue, s.purchased,
       CASE WHEN b.ts IS NOT NULL AND s.start - b.ts <= {WEEK_MS} THEN b.campaign_id ELSE 'organic' END AS campaign_id
FROM sessions s ASOF LEFT JOIN best b ON s.person = b.person AND s.start >= b.ts
""")

# ---- assists: each (person, campaign) click covers sessions with start in [ts, ts + 7d].
# Map each click to a session-rank range, merge overlapping ranges per (person, campaign),
# then expand: output size is exactly the assisted (session, campaign) pairs.
con.execute(f"""
CREATE TABLE assisted AS
WITH cc AS (
    SELECT DISTINCT person, campaign_id, ts, ts + {WEEK_MS} AS ts_hi FROM clicks
), hi AS (
    SELECT cc.*, coalesce(s.rk, 0) AS rk_hi
    FROM cc ASOF LEFT JOIN sessions s ON cc.person = s.person AND cc.ts_hi >= s.start
), lohi AS (
    SELECT hi.person, hi.campaign_id, coalesce(s.rk, 0) + 1 AS lo, hi.rk_hi AS hi
    FROM hi ASOF LEFT JOIN sessions s ON hi.person = s.person AND hi.ts > s.start
), ranges AS (
    SELECT *, max(hi) OVER (PARTITION BY person, campaign_id ORDER BY lo, hi
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_hi
    FROM (SELECT DISTINCT * FROM lohi WHERE lo <= hi)  -- distinct (lo, hi): the two windows must see one row order
), islands AS (
    SELECT *, sum(CASE WHEN prev_hi IS NULL OR lo > prev_hi + 1 THEN 1 ELSE 0 END)
                  OVER (PARTITION BY person, campaign_id ORDER BY lo, hi ROWS UNBOUNDED PRECEDING) AS island
    FROM ranges
), merged AS (
    SELECT person, campaign_id, min(lo) AS lo, max(hi) AS hi FROM islands GROUP BY person, campaign_id, island
), pairs AS (
    SELECT person, campaign_id, unnest(range(lo, hi + 1)) AS rk FROM merged
)
SELECT p.campaign_id, s.day, count(*) AS assisted_sessions
FROM pairs p JOIN sessions s ON s.person = p.person AND s.rk = p.rk
GROUP BY p.campaign_id, s.day
""")

# ---- outputs ----
for table in ("user_daily", "campaign_daily"):
    os.makedirs(os.path.join(OUT, table), exist_ok=True)

con.execute(f"""
COPY (
    WITH cat AS (
        SELECT s.person, s.day, e.category, sum(e.price) AS amount
        FROM ev_sess e JOIN sessions s ON s.person = e.person AND s.start = e.start
        WHERE e.is_purchase GROUP BY ALL
    ), top AS (
        SELECT person, day, arg_min(category, (-amount, category)) AS top_category FROM cat GROUP BY ALL
    )
    SELECT s.person AS user_id, s.day, count(*)::BIGINT AS sessions, sum(s.n_events)::BIGINT AS events,
           sum(s.revenue)::BIGINT AS revenue_cents, any_value(t.top_category) AS top_category
    FROM sessions s LEFT JOIN top t ON t.person = s.person AND t.day = s.day
    GROUP BY s.person, s.day
) TO '{os.path.join(OUT, "user_daily", "part-0.parquet")}' (FORMAT parquet)
""")

con.execute(f"""
COPY (
    WITH cr AS (
        SELECT campaign_id, day, count(*) AS sessions, count(*) FILTER (WHERE purchased) AS purchasing_sessions,
               sum(revenue) AS revenue_cents
        FROM credited GROUP BY ALL
    )
    SELECT coalesce(cr.campaign_id, a.campaign_id) AS campaign_id,
           CASE WHEN coalesce(cr.campaign_id, a.campaign_id) = 'organic' THEN 'organic' ELSE k.channel END AS channel,
           coalesce(cr.day, a.day) AS day,
           coalesce(cr.sessions, 0)::BIGINT AS sessions,
           coalesce(cr.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
           coalesce(cr.revenue_cents, 0)::BIGINT AS revenue_cents,
           coalesce(a.assisted_sessions, 0)::BIGINT AS assisted_sessions
    FROM cr FULL OUTER JOIN assisted a ON a.campaign_id = cr.campaign_id AND a.day = cr.day
    LEFT JOIN campaigns k ON k.campaign_id = coalesce(cr.campaign_id, a.campaign_id)
) TO '{os.path.join(OUT, "campaign_daily", "part-0.parquet")}' (FORMAT parquet)
""")
print(f"done: window {manifest['window_start']}..{manifest['window_end']}, link rounds {rounds}", file=sys.stderr)
