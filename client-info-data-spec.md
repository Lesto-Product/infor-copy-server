# GET /sales/client-info — data spec

Everything the print-document needs, where it comes from, and what is still
missing from the mirror. Verified against the live mirror and against LN via
Compass on 2026-08-19. Companion files:
`docs/city-master-verification.sql` (the tccom139 proof),
`docs/dim_city_seed.sql` (stopgap city table),
`docs/single-client-page-analysis.md` (the bpid-vs-name identity problem).

LN company: **`compnr = '201'`**.

## Endpoint shape

```
GET /sales/client-info?client=<име>              -> all known ship-to addresses, ranked
GET /sales/client-info?orders=SLS008622,SLS008645 -> the exact addresses for those orders
```

Always returns `addresses[]`, never a flattened single address:
82 of 89 active customers have exactly one ship-to address, 7 have 2–9
(INVENDA EUROPE 9, KÜHLER 5, INVENDA GROUP 4, ХОВАГ 3, KOMAX/ЕГТ/Г-ПРО 2).
`TRIM(nama_bg_BG)` is not unique either — INGERSOLL-RAND INTERNATIONAL LTD and
LIEBHERR-LOGISTICS GmbH each have 2 bpids — so `?client=` may match several BPs.

## Field map

| Document field | Source | Status |
|---|---|---|
| SUPPLIER No | `tccom110.osno` | ⚠️ needs extract; **only 19/89 active customers have one** |
| ShipTo name | `tccom100.nama_bg_BG` | ✅ live |
| Street | `tccom130.namc_bg_BG` | ✅ live |
| Post code | `tccom130.pstc_bg_BG` | ✅ live |
| City | `tccom139.dsca_bg_BG` via `(ccty, cste, ccit)` | ⚠️ needs extract; seeded locally meanwhile |
| Country | `tccom130.ccty` | ✅ live (codes are dirty: `NL`+`NLD`, `AU`=Austria, `TU`=Turkey) |
| Region / province | `tccom130.dsca_bg_BG` | ✅ live |
| Phone | `tccom130.telp` | ✅ live (`tccom110.telp` is empty) |
| Delivery terms | `tdsls400.cdec`, fallback `tccom110.cdec` | ⚠️ needs extract |
| Delivery place | *(none — it is the destination city)* | ✅ derived: print `cdec` + city, e.g. "DAP Тутракан" |
| Which of several addresses | `tdsls400.stad` per order | ✅ live |

## The joins

```
client name ──> tccom100.bpid
bpid ──ofbp──> tdsls400 ──stad──> tccom130          (address actually shipped to)
bpid ──ofbp──> tccom110.cadr ──> tccom130           (BP default, for BPs with no orders)
tccom130 ──(ccty, cste, ccit)──> tccom139.dsca_bg_BG (city name)
```

Three traps, all confirmed the hard way:

1. **The city key is `(ccty, cste, city)`, never the code alone.** BG `00000018`
   is Плевен under `cste=PL` and гр. Враца under `cste=VR`; the Плевен one is on
   4 addresses we ship to. Same code also crosses countries (`00000176` = Добрич
   in BG, Tezze sul Brenta in IT). All 48 in-use combinations resolve on the
   3-key join with 0 misses.
2. **Column names differ across tables.** The city code is `ccit` in tccom130
   but `city` in tccom139; the BP key is `bpid` in tccom100 but `ofbp` in
   tccom110 and tdsls400.
3. **Codes are zero-padded 8-char strings** (`'00000018'`). The Compass grid
   displays them unpadded (`18`), and `city = '18'` matches nothing.

Never print a fallback city name unchecked: the master contains placeholder
rows, e.g. `BE/BE/00000048` is literally named **"Do not use"**.

## Remaining ETL asks

All three are small, all filtered `compnr = '201'`.

1. **`tccom139` → `original_tccom139`** — columns `ccty, cste, city, dsca_bg_BG`
   (~250 rows). Until it lands, `docs/dim_city_seed.sql` seeds a local
   `dim_city` covering every city code currently in use.
2. **`tccom110` → `original_tccom110`** — columns `ofbp, cdec, osno, cadr,
   stdt, endt`. One open-ended row per BP (`endt` = 1970-01-01 is LN's zero
   date; no effectivity filter needed). Skip `cofc` (sales office = 'MAIN'),
   `incd` and `rdec` (empty).
3. **Add `cdec` to the existing `tdsls400` extract** — 1 674 of 4 839 orders
   carry delivery terms that differ from the BP default, so the order-level
   value is the one to print.

## Open items for the business, not for the code

- **`osno` is missing for 70 of 89 active customers**, covering 44% of orders.
  Biggest holes: ФААК БЪЛГАРИЯ (978 orders), ХУСКВАРНА РУСЕ (191), ЕГТ
  МУЛТИПЛЕЪР (173), INVENDA GROUP (101), INVENDA EUROPE (67). Nothing in code
  can fix this — the field has to be filled in LN.
- **One bad city-master row:** `GB/MC/00000213` is named "Warwick", but the one
  address using it is M24 2DB, Middleton (Greater Manchester).
- **`osno` is not unique** — `2984825`, `91577`, `44056` and `100420` are each
  shared by 2–3 BP records. Fine when read per BP, not usable as a key.
