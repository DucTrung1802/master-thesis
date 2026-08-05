# Fundamental Analysis Indicators — catalog & the price×financials plan

> Scope: the fundamental (valuation / profitability / balance-sheet) indicators we
> plan to compute for VN stocks, and the design of the new silver table that joins
> **daily price** to **quarterly financials** correctly (point-in-time, on
> `publish_date`). Grounded in what our data can actually produce — see
> [CONTEXT.md](CONTEXT.md) for the ETL those inputs come from. Right now only the
> `bank` template (VCB) is parsed, so the worked examples are bank-specific; the
> *universal* ratios below apply to every template once `corp` / `securities` /
> `insurance` are parsed.

## 0. What raw material we have

Coverage below is out of **78** quarters (the full VCB grid Q4-2006…Q1-2026; 72 carry
a `publish_date`, the 6 earliest do not).

| Input | Where it lives | Coverage (VCB, /78) |
|---|---|---|
| Adjusted close, OHLC, volume | `silver.stocks_basic` (`close_adjust` …) | daily, full |
| **Shares outstanding** (scanned) | `silver.cafef_financials_bank.shares_outstanding` | 63/78 |
| **Shares issued** (published) | `…cafef_financials_bank.shares_issued` | 62/78 |
| Net income after tax | `silver.cafef_financials_bank.income_statement_xiii_loi_nhuan_sau_thue` | 67/78 |
| Pre-tax profit | `…income_statement_xi_tong_loi_nhuan_truoc_thue` | 71/78 |
| Net interest income | `…income_statement_i_thu_nhap_lai_thuan` | 71/78 |
| Total operating income (bank "revenue") | `…income_statement_tong_thu_nhap_hoat_dong` | 25/78 ⚠️ sparse |
| Operating expense | `…income_statement_viii_chi_phi_hoat_dong` | 68/78 |
| Basic EPS (as filed) | `…income_statement_lai_co_ban_tren_co_phieu_dong_1_co_phieu` | 25/78 ⚠️ sparse |
| Total equity | `…balance_sheet_viii_von_chu_so_huu` | 71/78 |
| Charter capital (paid-in) | `…balance_sheet_viii_1_a_von_dieu_le` | 67/78 |
| Total assets | `…balance_sheet_tong_tai_san` | 71/78 |
| Customer loans / deposits | `…balance_sheet_vi_cho_vay_khach_hang` / `iii_tien_gui_cua_khach_hang` | 65 / 69 |
| `publish_date` (when the quarter went public) | `silver.cafef_financials_bank.publish_date` | 72/78 |

> ⚠️ In the **combined** `silver.cafef_financials_bank` the line items are
> **report-prefixed** (`income_statement_…`, `balance_sheet_…`, `cash_flow_…`); the
> per-report tables (`cafef_financials_bank_<report>`) carry the bare name. The share
> counts and `publish_date` are the exception — unprefixed, right after the keys.

**Shares outstanding is now a STORED column** (added 2026-07-20). It is scanned straight
off the filing's "Vốn cổ phần" note — `shares_outstanding` (đang lưu hành) and
`shares_issued` (đã phát hành) — so **P/E, P/B, market cap and every per-share ratio use
the real count**, not a proxy. VCB Q4-2019 = 3,708,877,448; the series is monotone
1.21bn→8.36bn across its known capital events. See [CONTEXT.md](CONTEXT.md) (bronze/silver
financials) for how the scan works.

> **Fallback where the scan is null** (16/78 quarters — a cafef/missing quarter, or a note
> that would not read): VN listed shares have a fixed **par value of ₫10,000**, so
> `shares_outstanding ≈ charter_capital (viii_1_a_von_dieu_le) / 10_000`. Cross-checked on
> VCB Q4-2025: `83,556,751,000,000 / 10,000 = 8,355,675,100` — matches the scanned
> ~8.36 bn. So the estimate agrees with the scan and is a safe backfill; it counts
> *charter* shares (ignores treasury `viii_1_d_co_phieu_quy`) and steps only on a capital
> change, which is exactly when it should. **Prefer the scanned column; use the estimate
> only to fill its gaps.**

## 1. The indicator catalog

Legend for **Basis**: **U** = universal (works for any template from equity / net
income / assets), **B** = bank-specific, **D** = needs the share count (the stored
`shares_outstanding`, scanned from the filing; par-value estimate only backfills its
gaps), **F** = as-filed value exists but is sparsely populated (prefer the computed form).

### 1a. Valuation (price ÷ fundamental) — the headline ratios

| Indicator | Formula (our columns) | Basis | Coverage |
|---|---|---|---|
| **Market cap** | `close_adjust × shares_outstanding` | U, D | 63/78 |
| **EPS (TTM)** | Σ last 4 quarters `xiii_loi_nhuan_sau_thue` ÷ `shares_outstanding` | U, D | ~62/78 |
| **P/E (TTM)** | `close_adjust ÷ EPS_ttm` | U, D | ~62/78 |
| **P/E (as-filed)** | `close_adjust ÷ (4 × lai_co_ban_tren_co_phieu…)` | F | 25/78 ⚠️ |
| **BVPS** (book value/share) | `viii_von_chu_so_huu ÷ shares_outstanding` | U, D | 63/78 |
| **P/B** | `close_adjust ÷ BVPS` | U, D | 63/78 |
| **P/S** (price/sales) | `market_cap ÷ (TTM tong_thu_nhap_hoat_dong)` | B, D | 25/78 ⚠️ |
| **Earnings yield** | `1 ÷ P/E` = `EPS_ttm ÷ close_adjust` | U, D | ~62/78 |
| **Dividend yield** | `dividend_per_share ÷ close_adjust` | — | ⚠️ *needs a dividend feed we don't ingest yet (see §4)* |

> Bank note: **P/S is unusual for banks** (they have no "sales"); `tong_thu_nhap_hoat_dong`
> (total operating income = net interest + fee + trading income) is the closest analog.
> For non-bank templates P/S will use net revenue instead. **EV/EBITDA is intentionally
> omitted for banks** — enterprise value and EBITDA are not meaningful for a bank's
> balance sheet.

### 1b. Profitability & returns

| Indicator | Formula | Basis |
|---|---|---|
| **ROE** | `TTM net_income ÷ avg(viii_von_chu_so_huu)` | U |
| **ROA** | `TTM net_income ÷ avg(tong_tai_san)` | U |
| **Net profit margin** | `xiii_loi_nhuan_sau_thue ÷ tong_thu_nhap_hoat_dong` | B (U w/ revenue) |
| **Pre-tax margin** | `xi_tong_loi_nhuan_truoc_thue ÷ tong_thu_nhap_hoat_dong` | B |
| **Effective tax rate** | `1 − (xiii ÷ xi_tong_loi_nhuan_truoc_thue)` | U |
| **NIM** (net interest margin) | `i_thu_nhap_lai_thuan ÷ avg(tong_tai_san)` | B |
| **Cost-to-income (CIR)** | `−viii_chi_phi_hoat_dong ÷ tong_thu_nhap_hoat_dong` | B |

> `avg(...)` = average of this quarter's and the year-ago quarter's balance (a stock
> item averaged over the period the flow item spans) — standard for ROE/ROA/NIM.

### 1c. Balance-sheet structure & bank health

| Indicator | Formula | Basis |
|---|---|---|
| **Equity multiplier** (leverage) | `tong_tai_san ÷ viii_von_chu_so_huu` | U |
| **Equity/assets ratio** | `viii_von_chu_so_huu ÷ tong_tai_san` | U |
| **LDR** (loan-to-deposit) | `vi_cho_vay_khach_hang ÷ iii_tien_gui_cua_khach_hang` | B |
| **Loans/assets** | `vi_cho_vay_khach_hang ÷ tong_tai_san` | B |
| **Deposits/assets** | `iii_tien_gui_cua_khach_hang ÷ tong_tai_san` | B |
| **CASA-ish, LLR, NPL** | *require deposit-mix / loan-quality lines not reliably parsed* | B ⚠️ later |

### 1d. Growth (period-over-period)

| Indicator | Formula |
|---|---|
| **Earnings growth (YoY)** | `net_income_t ÷ net_income_{t−4q} − 1` |
| **Revenue/op-income growth (YoY)** | `tong_thu_nhap_hoat_dong_t ÷ …_{t−4q} − 1` |
| **Equity / book-value growth (YoY)** | `viii_von_chu_so_huu_t ÷ …_{t−4q} − 1` |
| **Asset growth (YoY)** | `tong_tai_san_t ÷ …_{t−4q} − 1` |

**Recommended first cut** (all computable today, high coverage, template-universal
except where noted): market cap, P/E (TTM), P/B, earnings yield, ROE, ROA, equity
multiplier, effective tax rate, earnings-growth-YoY, equity-growth-YoY — plus the
bank set NIM, CIR, LDR when the template is `bank`.

## 2. The new table — `silver.stocks_fundamental` (design)

**Goal:** a **daily** panel = every `silver.stocks_basic` row + the fundamental
indicators above, where each day carries the **most recently published** quarter's
figures. Keyed `(exchange, ticker, date)` like `stocks_basic`.

### 2a. The join is an as-of merge on `publish_date` — NOT on the period

This is the whole point of the task ("change on publish date"). Quarterly figures are
only knowable **from the day they are published**, and `publish_date` lags the period
end by weeks-to-months (VCB Q4-2025 covers the quarter ending 31 Dec 2025 but was
published 27 Mar 2026). So:

```
for each (exchange, ticker) price series ordered by date:
    attach the financials row with the greatest publish_date <= date
    (pandas.merge_asof, direction="backward", by=[exchange,ticker], on=date↔publish_date)
```

- A fundamental value **steps** on its `publish_date` and holds flat until the next
  publish — so the same P/E denominator applies to every trading day of the ~3-month
  window until fresh figures drop. This is correct point-in-time behaviour and gives a
  model **zero look-ahead**.
- Days before the first `publish_date` for a ticker get NULL fundamentals (no filing
  was public yet) — kept, not dropped.
- Rows in `cafef_financials_bank` whose `publish_date` is NULL (the 6 early
  un-dated quarters) are **excluded from the as-of key** — a fact with no public date
  can't be pinned to a day.

### 2b. Build sketch

```
price = silver.stocks_basic                       # daily, (exchange,ticker,date)
fin   = silver.cafef_financials_bank              # quarterly, + publish_date + shares_*
        → shares = shares_outstanding (scanned); where null, fall back to
                   balance_sheet_viii_1_a_von_dieu_le / 10000 (par-value estimate)
        → compute TTM sums (net income, op income) over trailing 4 quarters
        → compute the §1 indicators that DON'T need price (EPS_ttm, BVPS, ROE, ROA,
          margins, leverage, growth) at the quarterly grain
        → drop rows with publish_date IS NULL, sort by publish_date
merge_asof(price, fin, by=[exchange,ticker], left_on=date, right_on=publish_date,
           direction="backward")
        → then the price-dependent ratios (market cap, P/E, P/B, P/S, yields) are
          computed row-wise from close_adjust × the as-of fundamentals
save → silver.stocks_fundamental   (drop-old-first; PK (exchange,ticker,date))
```

- **Per-share ratios use `close_adjust`** (the fully dividend-adjusted close), so P/E
  and P/B are on the same adjusted basis the returns use. (If we ever want the
  *unadjusted* market cap in VND we'd multiply raw close × shares — noted, not needed
  for cross-sectional ranking.)
- **TTM vs single-quarter:** valuation uses **trailing-twelve-month** earnings (Σ 4
  quarters) so P/E isn't 4× noisier off one quarter; a quarter with a missing line
  makes that TTM window NULL rather than wrong.
- **Template-agnostic:** the indicator functions read canonical inputs (net income,
  equity, assets, revenue, shares); a small per-template column map points them at the
  right line id, so `corp` / `securities` / `insurance` slot in without new ratio code.

### 2c. Open decisions (to settle before implementing)

1. **Grain of the output** — daily (join onto every price day, big but directly
   usable in the daily model) **vs** quarterly (one row per publish, smaller, joined
   to price later). Recommendation: **daily**, to match `stocks_basic` / `gold`.
2. **Dividend yield** needs a dividend-per-share series. We have a documented CafeF
   dividend path (raw/adjusted ratio — memory `reference-cafef-endpoints`) but don't
   ingest it yet. Ship without dividend yield first; add when the dividend feed lands.
3. **Bank-only vs universal columns** — emit the universal ratios for all tickers and
   the bank block only where `template='bank'` (NULL elsewhere), or split into two
   tables. Recommendation: one wide table, bank columns NULL off-template (mirrors how
   `stocks_basic` carries CafeF columns that taper by history).
4. **Gold** — whether these ratios live in silver only, or also get TA-style
   treatment in gold. Likely silver-only (they're already model-ready features).

## 3. Why these and not the "textbook" list

Standard screens (Graham, Piotroski F-score, Altman Z) assume a **non-financial**
firm — current ratio, inventory turnover, working capital, EBITDA. Those line items
don't exist on a **bank** chart of accounts (a bank has no inventory or current/long
-term split), which is exactly why the bronze layer keeps the four templates apart
(see [CONTEXT.md](CONTEXT.md) §4). The catalog above is the subset that (a) is
meaningful for the template we have and (b) our parsed lines actually populate.

## 4. Not yet possible (documented gaps)

- **Dividend yield / payout ratio** — no dividend-per-share feed ingested (§2c-2).
- **NPL, loan-loss coverage, CASA** — depend on loan-quality / deposit-mix lines that
  aren't reliably parsed for `bank` yet.
- **EV/EBITDA, current ratio, quick ratio, inventory/receivable turnover** — not
  applicable to banks; will apply to the `corp` template once parsed.
- **Anything per-share before ~2010** — charter capital / equity have gaps in deep
  history (65–71 of 72), so early-quarter ratios will be NULL rather than guessed.
