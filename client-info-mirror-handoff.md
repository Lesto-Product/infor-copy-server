# client-info — what landed in the mirror

Answers the three ETL asks in `client-info-data-spec.md`. Everything below is
live in `Lesto` on `192.168.1.187\SQLEXPRESS` and refreshes with the normal
jobs (05:00 full, 12:00 partial, both Europe/Sofia). Manual refresh of a single
table: `POST http://<host>:3005/trigger/<key>`.

Repo commits: `6e126af` (extracts + DDL), `9a2937d` (blank-key fix).

## New tables

### `original_tccom110` — BP defaults (key: `ofbp`)

| Column | Meaning |
|---|---|
| `ofbp` | BP id — this is `bpid` in `original_tccom100` |
| `osno` | **SUPPLIER No** — our number in the customer's system |
| `cdec` | delivery terms, BP default (order-level value wins, see below) |
| `cadr` | default address → `original_tccom130.cadr` |
| `stdt`, `endt` | validity, stored as text. `endt = 1970-01-01` is LN's zero date = open |

One open-ended row per BP, so no effectivity filter is needed. 166 rows on the
first load with no duplicate keys — the view is single-company, no `compnr`
filter required.

### `original_tccom139` — city master (key: `ccty` + `cste` + `city`)

| Column | Meaning |
|---|---|
| `ccty`, `cste`, `city` | the composite key |
| `dsca_bg_BG` | the city name |

**The city code alone is not a key.** `BG/PL/00000018` is Плевен while
`BG/VR/00000018` is гр. Враца; `00000176` is Добрич in BG and Tezze sul Brenta
in IT. Always join on all three. Codes are zero-padded 8-char strings
(`'00000018'`, never `'18'`).

Note the column is `city` here but `ccit` in `tccom130`.
The `dim_city` stopgap can be retired.

### `original_tdsls400.cdec` — delivery terms per order

New column on the existing table. Use it in preference to `tccom110.cdec`.

## One behaviour change you need to know about

The sync maps every falsy JDBC value to `NULL`, so LN's empty strings used to
arrive as `NULL`. For the city-join keys that is now normalised back to `''` on
**both** `original_tccom139` (`ccty`, `cste`, `city`) and `original_tccom130`
(`ccty`, `cste`, `ccit`).

Consequence for you: **join with plain `=`, no `ISNULL()` on either side.**
Countries with no state register carry `cste = ''`, not `NULL`, and the two
tables are guaranteed to agree.

This does not apply to non-key columns. `osno`, `cdec`, `telp` and the rest are
still `NULL` when LN has them blank — which is what `COALESCE` in the queries
below relies on.

## Naming trap

`original_tccom130.dsca_bg_BG` is **not** the city. That extract joins
`LN_tcmcs143`, so the column holds the **region / province** name. The city
name is `original_tccom139.dsca_bg_BG`. Both columns are called `dsca_bg_BG` —
alias them in every query or you will silently print the wrong one.

## Query 1 — `GET /sales/client-info?client=<име>`

```sql
DECLARE @client NVARCHAR(200) = N'...';

;WITH bp AS (
    SELECT bpid, LTRIM(RTRIM(nama_bg_BG)) AS name
    FROM dbo.original_tccom100
    WHERE LTRIM(RTRIM(nama_bg_BG)) = @client      -- may match several bpids
),
addr AS (
    -- addresses we have actually shipped to
    SELECT s.ofbp AS bpid, s.stad AS cadr,
           COUNT(*) AS orders_cnt, MAX(s.odat) AS last_order
    FROM dbo.original_tdsls400 s
    JOIN bp ON bp.bpid = s.ofbp
    WHERE s.stad IS NOT NULL
    GROUP BY s.ofbp, s.stad

    UNION ALL

    -- BP default, so BPs with no orders still return an address
    SELECT c.ofbp, c.cadr, 0, NULL
    FROM dbo.original_tccom110 c
    JOIN bp ON bp.bpid = c.ofbp
    WHERE c.cadr IS NOT NULL
),
ranked AS (
    SELECT bpid, cadr,
           SUM(orders_cnt) AS orders_cnt,
           MAX(last_order) AS last_order
    FROM addr
    GROUP BY bpid, cadr
)
SELECT  bp.bpid,
        bp.name                       AS ship_to_name,
        d.osno                        AS supplier_no,
        a.namc_bg_BG                  AS street,
        a.pstc_bg_BG                  AS post_code,
        city.dsca_bg_BG               AS city,          -- tccom139
        a.ccty                        AS country_code,
        a.dsca_bg_BG                  AS region,        -- tcmcs143, NOT the city
        a.telp                        AS phone,
        d.cdec                        AS delivery_terms_default,
        r.orders_cnt, r.last_order
FROM ranked r
JOIN      bp                         ON bp.bpid = r.bpid
LEFT JOIN dbo.original_tccom130 a    ON a.cadr  = r.cadr
LEFT JOIN dbo.original_tccom110 d    ON d.ofbp  = r.bpid
LEFT JOIN dbo.original_tccom139 city ON city.ccty = a.ccty
                                    AND city.cste = a.cste
                                    AND city.city = a.ccit
ORDER BY r.orders_cnt DESC, r.last_order DESC;
```

Keep returning `addresses[]` as the spec says: 7 customers have 2–9 ship-to
addresses, and the name itself is not unique either (INGERSOLL-RAND and
LIEBHERR-LOGISTICS each have 2 bpids), so `?client=` can legitimately span
several BPs.

## Query 2 — `GET /sales/client-info?orders=SLS008622,...`

```sql
SELECT  o.orno,
        LTRIM(RTRIM(bp.nama_bg_BG))   AS ship_to_name,
        d.osno                        AS supplier_no,
        a.namc_bg_BG                  AS street,
        a.pstc_bg_BG                  AS post_code,
        city.dsca_bg_BG               AS city,
        a.ccty                        AS country_code,
        a.dsca_bg_BG                  AS region,
        a.telp                        AS phone,
        COALESCE(o.cdec, d.cdec)      AS delivery_terms      -- order wins
FROM dbo.original_tdsls400 o
JOIN      dbo.original_tccom100 bp   ON bp.bpid = o.ofbp
LEFT JOIN dbo.original_tccom130 a    ON a.cadr  = o.stad
LEFT JOIN dbo.original_tccom110 d    ON d.ofbp  = o.ofbp
LEFT JOIN dbo.original_tccom139 city ON city.ccty = a.ccty
                                    AND city.cste = a.cste
                                    AND city.city = a.ccit
WHERE o.orno IN (N'SLS008622', N'SLS008645');
```

`COALESCE(o.cdec, d.cdec)` is the rule the spec asks for — 1 674 of 4 839
orders carry terms that differ from the BP default. Blank `cdec` arrives as
`NULL`, so `COALESCE` alone is enough.

"Delivery place" stays derived, as the spec concluded: terms + city, e.g.
`DAP Тутракан`. Build it in code after the city guard below, not in SQL —
otherwise a placeholder city name ends up concatenated into the printed line.

## Guards before you print

- **Placeholder city rows.** `BE/BE/00000048` is literally named `"Do not use"`.
  Never print a city name unchecked.
- **`GB/MC/00000213`** is named "Warwick" but the one address using it is
  M24 2DB, Middleton. Known-bad master row.
- **`osno` is missing for ~70 of 89 active customers** (44% of orders). The
  field is empty in LN — nothing in code fixes it. Render the line as absent,
  don't print an empty label.
- **`osno` is not unique.** `2984825`, `91577`, `44056` and `100420` are each
  shared by 2–3 BPs. Fine to read per BP, never use it as a key.
- **Country codes are dirty**: `NL` and `NLD` both occur, `AU` = Austria,
  `TU` = Turkey. Don't map them naively to ISO.

## Run these first

```sql
-- 1. sanity counts
SELECT COUNT(*) FROM dbo.original_tccom139;   -- expect ~250
SELECT COUNT(*) FROM dbo.original_tccom110;   -- expect ~166

-- 2. the join that matters: every in-use city combo must resolve.
--    Expect 0 rows. Anything here is a city we cannot print.
SELECT DISTINCT a.ccty, a.cste, a.ccit
FROM dbo.original_tccom130 a
LEFT JOIN dbo.original_tccom139 c ON c.ccty = a.ccty
                                 AND c.cste = a.cste
                                 AND c.city = a.ccit
WHERE c.city IS NULL
  AND EXISTS (SELECT 1 FROM dbo.original_tdsls400 s WHERE s.stad = a.cadr);

-- 3. osno coverage — confirms the column actually populated
SELECT COUNT(*) AS bps, COUNT(osno) AS with_osno FROM dbo.original_tccom110;

-- 4. cdec: is the backfill done? If cdec is NULL everywhere, ask for a full
--    reload of tdsls400 (it is incremental, closed orders never update).
SELECT COUNT(*) AS orders, COUNT(cdec) AS with_cdec FROM dbo.original_tdsls400;
```

If a join unexpectedly returns nothing, check for trailing spaces first — values
come from LN via JDBC as strings and `CHAR` columns can arrive padded.
`LTRIM(RTRIM())` on the join key settles it.
