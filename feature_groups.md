# Feature Groups (standardized)

> ⚠️ **Reference table, not an operating document.** Root registers:
> [CLAUDE.md](CLAUDE.md) (map) · [RUNBOOK.md](RUNBOOK.md) (run) ·
> [ISSUES.md](ISSUES.md) (broken) · [TODO.md](TODO.md) (next).

Canonical taxonomy for every feature produced by `train_test_creator` (the assembled
`feature_df`, ~1,250 columns before selection). Group names are `snake_case`; the
`ta_*` prefix namespaces all technical-indicator families (so "all TA" = name starts
with `ta_`). Each row gives a programmatic membership rule.

## Groups

| Group | Category | Description | Membership rule (token = first `_`-segment unless noted) |
|---|---|---|---|
| `raw_ohlcv` | market | Raw daily market data | name ∈ {`open`,`high`,`low`,`close`,`volume`} |
| `macro_economy` | macro | Domestic (Vietnam) economic series — GDP, CPI, PPI, M0/M1/M2, FDI, employment… | starts with `economy_` |
| `macro_bonds` | macro | Domestic (Vietnam) government bond yields (1y–10y) | starts with `bonds_` |
| `macro_global` | macro | **Reserved / not yet in data** — foreign & global series (US indices, commodities, FX) | (to define when wired in; e.g. `snp_500_*`, `oil_price_*`, `exchange_rate_*`) |
| `calendar` | calendar | Datetime cyclical encodings & flags | `*_sin`/`*_cos` on date parts; or token ∈ {`day`,`month`,`year`,`quarter`,`week`,`is`} |
| `ta_overlap` | technical | Moving averages / overlap studies | token ∈ {`sma`,`ema`,`dema`,`tema`,`trima`,`wma`,`kama`,`t3`,`midpoint`,`midprice`,`bbands`,`sar`} (incl. `close_<ma>_*`) |
| `ta_momentum` | technical | Momentum / oscillators | token ∈ {`adx`,`plus_di`,`minus_di`,`di`,`trend`,`aroon`,`bop`,`cci`,`cmo`,`macd`,`mfi`,`mom`,`ppo`,`roc`,`rsi`,`stoch`,`stoch_rsi`,`trix`,`ultosc`,`willr`} |
| `ta_volume` | technical | Volume indicators | token ∈ {`ad`,`adosc`,`obv`} |
| `ta_volatility` | technical | Volatility indicators | token ∈ {`atr`,`natr`,`trange`} |
| `ta_cycle` | technical | Hilbert-transform cycle indicators | starts with `ht_` |
| `ta_price_transform` | technical | Price transforms | token ∈ {`avgprice`,`medprice`,`typprice`,`wclprice`} |
| `derived_price` | derived | Engineered price features | `return_*`, `range_hl`, `body_oc`, `volatility_*`, `*_roll_mean/std/min/max_*` |

## Scaling class (orthogonal — cross-cuts all groups)

Assigned during normalization, independent of the group above.

| Class | Description | Rule |
|---|---|---|
| `continuous` | StandardScaler-scaled | default for numeric features |
| `bounded` | Left unscaled | `*_sin`/`*_cos` (∈ [−1, 1]) or binary `0/1` flags (crossovers, threshold flags, calendar flags) |

## Notes

- **TA derived sub-features:** each indicator expands into many columns — the raw value plus
  `_slope`, `_acceleration`, `_dist`/`_dist_abs`/`_dist_pct`, threshold flags (`_gt_70`,
  `_lt_30`, `_extreme`…), `_signal`, `_hist`, `_crossover_up/dn`, `_strength`, `_direction`,
  and pairwise distances between periods (e.g. `close_kama_100_200_dist`). ~30 indicators →
  ~1,000+ columns.
- **Macro is currently domestic only:** all macro columns are Vietnam (`vn` prefix). Foreign /
  global series (S&P 500, Dow Jones, NASDAQ, NYSE, oil, gold, USD/VND) exist in the gold
  `macroeconomics` schema but are **not** joined into the unified table yet — they would
  populate `macro_global`.
