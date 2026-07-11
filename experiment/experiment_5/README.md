# Experiment 5 — VCB shares-outstanding (KLCP) point-in-time history

**Goal.** Recover VCB's **listed / outstanding share count** ("KLCP đang niêm yết",
"KLCP lưu hành") as a point-in-time series, 2009 → present, so raw price can be
joined to **market cap, turnover and free-float** without look-ahead. This is the
second orthogonal-data piece after experiment_4's disclosure calendar — the lever
the whole study concluded is binding (see `experiment/CONTEXT.md`, "the binding
constraint is DATA").

## The problem

The CafeF data page
([hose/vcb…](https://cafef.vn/du-lieu/hose/vcb-ngan-hang-thuong-mai-co-phan-ngoai-thuong-viet-nam.chn))
shows only the **current** count:

> KLCP đang niêm yết **8,355,675,094** · KLCP lưu hành **8,355,675,094**

There is **no endpoint that serves a time-series** of the share count. But the count
only ever changes on a corporate action, and CafeF exposes the full action log:

```
GET cafef.vn/du-lieu/Ajax/PageNew/LichSuKien.ashx?Symbol=vcb&PageIndex=1&PageSize=500
```

So the history is exactly reconstructable: **anchor on the current count and walk the
events backward**, undoing each share-changing action.

## Which events move the count

| CafeF text | type | effect on shares |
|---|---|---|
| `Cổ tức bằng Cổ phiếu, tỷ lệ 1000:X` | stock dividend | `× (1 + X/1000)` |
| `Thưởng bằng Cổ phiếu, tỷ lệ 100:X` | bonus issue | `× (1 + X/100)` |
| `Bán ưu đãi, tỷ lệ 100:X` | rights issue | `× (1 + X/100)` (assumes full subscription) |
| `Phát hành riêng lẻ N` | private placement | `+ N` |
| `Cổ tức bằng Tiền, …` | cash dividend | no change |

Of VCB's 17 logged sub-events, **7 change the share count**; the 10 cash dividends do not.

## Result — VCB listed shares over time

| effective from | shares outstanding | event |
|---|---:|---|
| ≤ first event | 1,742,397,291 | reconstructed base |
| 2010-12-13 | 2,317,388,397 | rights issue 100:33 |
| 2014-06-17 | 2,664,996,657 | bonus 100:15 |
| 2016-09-08 | 3,597,745,487 | bonus 100:35 |
| 2019-01-03 | 3,708,854,360 | private placement +111,108,873 (GIC/Mizuho) |
| 2021-12-21 | 4,732,498,163 | stock dividend 1000:276 |
| 2023-07-24 | 5,589,080,330 | stock dividend 1000:181 |
| 2025-03-11 | **8,355,675,094** | stock dividend 1000:495 |

**Accuracy.** The ratio walk matches VCB's known post-2016 filings to **<0.001%**; the
few-thousand-share residuals are fractional-share rounding on each stock dividend. For a
to-the-share-exact series, paste verified counts (from HOSE "thay đổi niêm yết" notices)
into `vcb_shares_milestones.csv` — any row there pins that step and overrides the estimate.

The pre-2010 base (`1.74 B`) is the least certain cell: CafeF's log starts 2010-03, so any
2009 IPO/listing event isn't captured. If needed, pin the 2009 listed count via the
milestones file.

## Files

- `scrape_vcb_shares_outstanding.py` — one stdlib script. It fetches from CafeF and
  writes a local `vcb_lichsukien.json` cache (gitignored); a later run reuses it unless
  `--refresh`.
- `vcb_shares_outstanding.csv` — the step series (effective_date, shares_outstanding,
  event_type, event_text, prev_shares, delta_shares, factor).
- `vcb_corporate_actions.csv` — every parsed event, classified (affects_shares flag).
- `vcb_shares_milestones.csv` — optional exact-count overrides (empty by default).

## Point-in-time lookup

`shares_outstanding(d)` = `shares_outstanding` of the most recent row with
`effective_date ≤ d`. Then `market_cap(d) = raw_close(d) × shares_outstanding(d)`
(use **raw**, not adjusted, close — the share count already carries the dilution).

## Re-anchoring / extending

- **Re-anchor** (once a year, after a new action): update `ANCHOR_SHARES` / `ANCHOR_DATE`
  in the script from the CafeF page and re-run.
- **Other tickers:** `LichSuKien.ashx` is generic — `main("fpt")` etc. works; only the
  anchor is VCB-specific. Rights-issue subscription assumptions may need per-ticker care.
