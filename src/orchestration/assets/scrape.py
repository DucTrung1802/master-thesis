# src\orchestration\assets\scrape.py
"""THE LANDING LAYER — every scraper, from the network to `raw_data/`. No database.

This is the whole of `main.py`'s scraping half as an asset graph. Nothing here touches
PostgreSQL; the bronze ingests that read these folders live in `assets/bronze.py` and
are a separate, later concern by design — a landing layer that is correct on disk is
the precondition for everything downstream, and it can be built and re-run without a
database at all.

```
trading_view_links[9 asset classes]
   │
   ├─(same partition)─► trading_view_data[9 asset classes]
   │
   ├─► trading_view_collected_links        ⚠️ NOTHING READS THIS (see below)
   │
   └─(the `stocks` partition ONLY = the ticker UNIVERSE)
           ├─► cafef_{price,order_stats,foreign,prop_trading,insider_txn}
           ├─► cafef_news
           └─► simplize_{stocks,industry}

cafef_pdfs[per ticker] ─► cafef_financials[per ticker]   (VN100 list; NO TradingView dep)
cafef_index_{price,order_stats,foreign,prop_trading}     (fixed 6-index universe)
gics_structure                                           (independent)
```

⚠️ **The edges are read out of the code, not out of the docs — and the docs were
misleading.** What every consumer actually reads:

| consumer | reads | so it depends on |
|---|---|---|
| `CafeFScraper.get_stock_symbols` | `raw_data/trading_view/links/**stocks**/**/*.csv` | `trading_view_links`, partition `stocks` |
| `SimplizeScraper.get_stock_symbols` | the same directory | the same |
| `CafeFNewsScraper.scrape_all_news` | `get_stock_symbols()` | the same |
| `_add_generic_link_data_tasks` | `links/<its own asset class>/…` | `trading_view_links`, **same partition** |
| `CafeFPdfScraper` | `CAFEF_PDF_TICKERS` (repo-root `vn100.csv`) | **nothing** |
| `CafeFIndexScraper` | its own `INDEXES` class constant | **nothing** |

⚠️ **`collected_links` IS NOT THE UNIVERSE.** It was described that way in this repo's
notes and in an earlier version of this file, and it is wrong: the only two references
to `collected_links` anywhere outside this package are the code that WRITES it and its
switch key. Nothing reads it. The universe is `links/stocks/`. The asset is kept —
`main.py` still runs the aggregation phase and the CSV is on disk — but it is a
leaf, not a hub.

⚠️ **Every asset verifies what landed** (`_common.landed`) and fails if a scraper wrote
nothing. The scrapers still swallow their own errors — Phase 0 fixed the preprocessor,
not this layer — so without the check a failed download is a green asset.
"""

from typing import Callable, List

from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetKey,
    Config,
    MaterializeResult,
    MetadataValue,
    SpecificPartitionsPartitionMapping,
    StaticPartitionsDefinition,
    asset,
)


class FinancialsConfig(Config):
    """Run config for the statement parse, settable per-run from the UI.

    ⚠️ `skip_existing=False` is the AUTHORITATIVE run, not merely the slow one. Skipping
    makes every run a subset run, which switches `sane` — the magnitude guard that
    compares a figure against neighbouring quarters — to failing open. That guard is what
    caught ACB's Q1-2024 carrying Q1-2023's PBT. Default True for "fill the gaps"; set
    False whenever the PARSER itself has changed.
    """

    skip_existing: bool = True

from orchestration._bootstrap import bootstrap

bootstrap()

import os

from utils.constants import (
    CAFEF_FINANCIALS_TICKERS,
    CAFEF_PDF_TICKERS,
    CAFEF_RAW_DATA_DIR,
    GICS_RAW_DATA_DIR,
    SIMPLIZE_RAW_DATA_DIR,
    TRADING_VIEW_RAW_DATA_DIR,
)
from web_scraper.cafef_financials import FinancialsBuilder
from web_scraper.cafef_index_scraper import CafeFIndexScraper
from web_scraper.cafef_news_scraper import CafeFNewsScraper
from web_scraper.cafef_pdf_scraper import CafeFPdfScraper
from web_scraper.cafef_scraper import CafeFScraper
from web_scraper.gics_scraper import GicsScraper
from web_scraper.simplize_scraper import SimplizeScraper
from web_scraper.trading_view_scraper import TradingViewScraper

from orchestration.assets._common import landed, split_ticker_key, ticker_key
from orchestration.resources import RepoLogger, SwitchConfig

assets: List[Callable] = []

TV_LINKS = AssetKey(["raw", "trading_view_links"])

# ⚠️ THE UNIVERSE IS ONE PARTITION, NOT THE WHOLE ASSET. `get_stock_symbols()` globs
# `links/stocks/**/*.csv` and nothing else, so CafeF and Simplize need the `stocks`
# partition and are indifferent to the other eight. `SpecificPartitionsPartitionMapping`
# says exactly that; the default for an unpartitioned asset depending on a partitioned
# one is ALL partitions, which would claim CafeF needs forex and bonds links to run.
UNIVERSE = AssetDep(
    asset=TV_LINKS,
    partition_mapping=SpecificPartitionsPartitionMapping(["stocks"]),
)


# ══════════════════════════════════════════════════════════════════════════════
# TradingView — the universe, then the OHLCV
# ══════════════════════════════════════════════════════════════════════════════
# PARTITIONED BY ASSET CLASS, not by the 320 switch leaves. The leaves below an asset
# class (country / stock_type / sector) are PARAMETERS the scraper's own task adders
# read out of switch_config.json — 320 Dagster partitions would just re-encode that
# JSON in a second place, and be unusable in the UI. Nine is the granularity a person
# actually re-runs at ("redo forex").
TV_ASSET_CLASSES = StaticPartitionsDefinition(
    [
        "stocks",
        "funds",
        "futures",
        "forex",
        "crypto",
        "indices",
        "bonds",
        "economy",
        "options",
    ]
)

# The two steps that are known no-ops, so `landed()` must not treat an empty folder as a
# failure: TradingView's crypto DATA step is not implemented (`_add_crypto_data_tasks`
# is a stub) and `_add_options_data_tasks` returns 0.
TV_DATA_MAY_BE_EMPTY = {"crypto", "options"}


def _tv(logger, switches: SwitchConfig, phase: str, asset_class: str):
    """A TradingView scraper whose RUN-PLAN ancestors are forced on for `phase`.

    See `SwitchConfig.build_unblocked` — without this the committed
    `"web_scraper": false` would make every adder enumerate zero tasks and the asset
    would succeed having scraped nothing.
    """
    handler = switches.build_unblocked(
        logger,
        "web_scraper",
        "web_scraper/trading_view",
        f"web_scraper/trading_view/{phase}",
        f"web_scraper/trading_view/{phase}/{asset_class}",
    )
    return TradingViewScraper(logger=logger, switch_handler=handler, power=100)


@asset(
    name="trading_view_links",
    key_prefix=["raw"],
    group_name="trading_view",
    compute_kind="selenium",
    partitions_def=TV_ASSET_CLASSES,
    # ⚠️ `resource: browser` caps this to ONE running step (see definitions.EXECUTOR).
    # SCRAPER_MAX_CONCURRENT_BROWSERS is an IN-PROCESS semaphore, so N processes means
    # 8N Chrome instances and N× the global navigation stagger against TradingView.
    op_tags={"resource": "browser"},
    description=(
        "Symbol links per (country, type, sector) leaf → "
        "raw_data/trading_view/links/<asset>/…  One partition per asset class; the "
        "dimension leaves come from switch_config.json. Replaces "
        "`web_scraper/trading_view/links/<asset>`."
    ),
)
def trading_view_links(
    context: AssetExecutionContext, repo_logger: RepoLogger, switches: SwitchConfig
) -> MaterializeResult:
    asset_class = context.partition_key
    logger = repo_logger.build()
    scraper = _tv(logger, switches, "links", asset_class)

    adder = getattr(scraper, f"_add_{_ADDER_STEM[asset_class]}_links_tasks")
    scraper._thread_manager.remove_all_tasks()
    queued = adder()
    context.log.info(f"{asset_class}: queued {queued} link task(s)")
    scraper._thread_manager.execute()

    meta = landed(f"{TRADING_VIEW_RAW_DATA_DIR}/links/{asset_class}")
    meta["tasks_queued"] = queued
    return MaterializeResult(metadata=meta)


# `_add_<stem>_links_tasks` — the method stem is not always the partition key
# (`stocks` → `_add_stock_links_tasks`, singular; `indices` → `_add_indices_…`).
_ADDER_STEM = {
    "stocks": "stock",
    "funds": "fund",
    "futures": "futures",
    "forex": "forex",
    "crypto": "crypto",
    "indices": "indices",
    "bonds": "bonds",
    "economy": "economy",
    "options": "options",
}


@asset(
    name="trading_view_collected_links",
    key_prefix=["raw"],
    group_name="trading_view",
    compute_kind="python",
    deps=[TV_LINKS],
    description=(
        "Dedups every link CSV into one "
        "raw_data/trading_view/collected_links/all_links_<date>.csv. "
        "⚠️ NOTHING READS THIS — it is a leaf, not the universe. The only references to "
        "`collected_links` outside this package are the code that writes it and its "
        "switch key; the universe CafeF/Simplize read is `links/stocks/`. Kept because "
        "main.py still runs the phase and the CSV is on disk."
    ),
)
def trading_view_collected_links(
    context: AssetExecutionContext, repo_logger: RepoLogger, switches: SwitchConfig
) -> MaterializeResult:
    logger = repo_logger.build()
    scraper = _tv(logger, switches, "collected_links", "")
    scraper.aggregate_trading_view_links()
    return MaterializeResult(
        metadata=landed(f"{TRADING_VIEW_RAW_DATA_DIR}/collected_links")
    )


@asset(
    name="trading_view_data",
    key_prefix=["raw"],
    group_name="trading_view",
    compute_kind="selenium",
    partitions_def=TV_ASSET_CLASSES,
    # Same partitions definition ⇒ IDENTITY mapping: the `forex` data partition needs the
    # `forex` links partition and no other. `_add_generic_link_data_tasks` reads
    # `links/<its own sub_parts>/`, never the aggregate.
    deps=[TV_LINKS],
    op_tags={"resource": "browser"},
    description=(
        "OHLCV per symbol by driving the chart widget → "
        "raw_data/trading_view/data/<asset>/…  Reads its OWN asset class's link CSVs. "
        "⚠️ SKIPS symbols already on disk (delete a CSV to re-fetch); before that skip "
        "existed this asset re-scraped all 4,675 links every run, ~10.4 h of pure "
        "8-second navigation stagger. Replaces `web_scraper/trading_view/data/<asset>`."
    ),
)
def trading_view_data(
    context: AssetExecutionContext, repo_logger: RepoLogger, switches: SwitchConfig
) -> MaterializeResult:
    asset_class = context.partition_key
    logger = repo_logger.build()
    scraper = _tv(logger, switches, "data", asset_class)

    adder = getattr(scraper, f"_add_{_ADDER_STEM[asset_class]}_data_tasks")
    scraper._thread_manager.remove_all_tasks()
    queued = adder()
    context.log.info(f"{asset_class}: queued {queued} data task(s)")
    scraper._thread_manager.execute()

    meta = landed(
        f"{TRADING_VIEW_RAW_DATA_DIR}/data/{asset_class}",
        require=asset_class not in TV_DATA_MAY_BE_EMPTY,
    )
    meta["tasks_queued"] = queued
    return MaterializeResult(metadata=meta)


assets += [trading_view_links, trading_view_collected_links, trading_view_data]


# ══════════════════════════════════════════════════════════════════════════════
# CafeF — the five per-stock daily/event tabs
# ══════════════════════════════════════════════════════════════════════════════
# One asset per tab, unpartitioned: the scraper already fans out over ~777 tickers on
# its own 16-thread pool and `skip_existing=True` makes a re-run cheap, so a partition
# per ticker would only take that parallelism away.
CAFEF_TABS = [
    ("price", "scrape_all_price", "price", "OHLC, raw+adj close, matched/negotiated volume (2009→)"),
    ("order_stats", "scrape_all_order_stats", "order_stats", "order-placement stats — buy vs sell order counts/volume (2010→)"),
    ("foreign", "scrape_all_foreign", "foreign", "foreign buy/sell/net flow, room, own% (2012→)"),
    ("prop_trading", "scrape_all_prop_trading", "prop_trading", "proprietary-desk trades (2023→; 431 of 777 tickers have any)"),
    ("insider_txn", "scrape_all_insider_txn", "insider_txn", "insider / major-shareholder transactions — EVENT-based (2008→)"),
]


def _build_cafef_tab_asset(tab: str, method: str, folder: str, what: str):
    @asset(
        name=f"cafef_{tab}",
        key_prefix=["raw"],
        group_name="cafef",
        compute_kind="requests",
        deps=[UNIVERSE],
        description=f"CafeF {what} → raw_data/cafef/{folder}/. Replaces `web_scraper/cafef/{tab}`.",
    )
    def _scrape(
        context: AssetExecutionContext, repo_logger: RepoLogger, switches: SwitchConfig
    ) -> MaterializeResult:
        logger = repo_logger.build()
        scraper = CafeFScraper(logger=logger, switch_handler=switches.build(logger))
        # The per-tab batch method, never `scrape()` — that re-reads the switch config
        # and would let the JSON veto a materialisation Dagster was asked for.
        getattr(scraper, method)()
        return MaterializeResult(metadata=landed(f"{CAFEF_RAW_DATA_DIR}/{folder}"))

    return _scrape


assets += [_build_cafef_tab_asset(*spec) for spec in CAFEF_TABS]


# ══════════════════════════════════════════════════════════════════════════════
# CafeF market indices — the same four tabs, six fixed codes
# ══════════════════════════════════════════════════════════════════════════════
# ⚠️ NO UPSTREAM. An index has no TradingView link CSV, so the universe is the fixed
# `INDEXES` list on the class and this branch runs before TV ever has.
CAFEF_INDEX_TABS = [
    ("price", "scrape_all_index_price", "index_price"),
    ("order_stats", "scrape_all_index_order_stats", "index_order_stats"),
    ("foreign", "scrape_all_index_foreign", "index_foreign"),
    ("prop_trading", "scrape_all_index_prop_trading", "index_prop_trading"),
]


def _build_cafef_index_asset(tab: str, method: str, folder: str):
    @asset(
        name=f"cafef_index_{tab}",
        key_prefix=["raw"],
        group_name="cafef_index",
        compute_kind="requests",
        description=(
            f"CafeF index `{tab}` tab for all 6 market indices → raw_data/cafef/{folder}/. "
            f"Replaces `web_scraper/cafef_index/{tab}`. No upstream — fixed universe."
        ),
    )
    def _scrape(
        context: AssetExecutionContext, repo_logger: RepoLogger, switches: SwitchConfig
    ) -> MaterializeResult:
        logger = repo_logger.build()
        scraper = CafeFIndexScraper(
            logger=logger, switch_handler=switches.build(logger)
        )
        getattr(scraper, method)()
        return MaterializeResult(metadata=landed(f"{CAFEF_RAW_DATA_DIR}/{folder}"))

    return _scrape


assets += [_build_cafef_index_asset(*spec) for spec in CAFEF_INDEX_TABS]


# ══════════════════════════════════════════════════════════════════════════════
# CafeF news — the company-news / disclosure event stream
# ══════════════════════════════════════════════════════════════════════════════
@asset(
    name="cafef_news",
    key_prefix=["raw"],
    group_name="cafef",
    compute_kind="requests",
    deps=[UNIVERSE],
    description=(
        "Headline feed + article bodies + filing pdf_url → raw_data/cafef/news/. "
        "~405k rows / ~495 MB over the full universe, ~2 h at 8 ticker-threads × 8 "
        "article-workers. Replaces `web_scraper/cafef/news`."
    ),
)
def cafef_news(
    context: AssetExecutionContext, repo_logger: RepoLogger, switches: SwitchConfig
) -> MaterializeResult:
    logger = repo_logger.build()
    scraper = CafeFNewsScraper(logger=logger, switch_handler=switches.build(logger))
    scraper.scrape_all_news()
    return MaterializeResult(metadata=landed(f"{CAFEF_RAW_DATA_DIR}/news"))


assets.append(cafef_news)


# ══════════════════════════════════════════════════════════════════════════════
# CafeF filings — download the PDFs, then parse them (both PER TICKER)
# ══════════════════════════════════════════════════════════════════════════════
# ⚠️ THESE TWO ARE PARTITIONED BY TICKER AND THE OTHERS ARE NOT, because these are the
# only steps whose COST IS PER TICKER and large: ~1.0-1.7 GB of download each, and
# ~2.4 h of OCR each. Today scoping them means editing CAFEF_PDF_TICKERS /
# CAFEF_FINANCIALS_TICKERS in constants.py and reading logs/app.log to find out what
# happened; as partitions, one ticker is one re-runnable unit with its own record.
PDF_TICKERS = StaticPartitionsDefinition(
    [ticker_key(e, s) for e, s in CAFEF_PDF_TICKERS]
)
FINANCIALS_TICKERS = StaticPartitionsDefinition(
    [ticker_key(e, s) for e, s in CAFEF_FINANCIALS_TICKERS]
)


@asset(
    name="cafef_pdfs",
    key_prefix=["raw"],
    group_name="cafef_filings",
    compute_kind="requests",
    partitions_def=PDF_TICKERS,
    # ⚠️ NO TradingView dep. Unlike every other CafeF scraper this one does not call
    # `get_stock_symbols()` — its universe is CAFEF_PDF_TICKERS, read from the repo-root
    # `vn100.csv`, and the partition key names the ticker outright.
    description=(
        "One ticker's filing archive → raw_data/cafef/pdfs/{index/<EX>_<SYM>.csv, "
        "files/<EX>_<SYM>/*.pdf}. ⚠️ ~1.0-1.7 GB PER PARTITION. Replaces "
        "`web_scraper/cafef/pdfs` + the CAFEF_PDF_TICKERS list."
    ),
)
def cafef_pdfs(
    context: AssetExecutionContext, repo_logger: RepoLogger, switches: SwitchConfig
) -> MaterializeResult:
    exchange, symbol = split_ticker_key(context.partition_key)
    logger = repo_logger.build()
    scraper = CafeFPdfScraper(logger=logger, switch_handler=switches.build(logger))
    scraper.scrape_pdfs(exchange, symbol)

    folder = f"{CAFEF_RAW_DATA_DIR}/pdfs/files/{exchange}_{symbol}"
    meta = landed(folder, pattern="*.pdf", count_rows=False)
    meta["megabytes"] = round(
        sum(os.path.getsize(os.path.join(folder, f)) for f in os.listdir(folder))
        / 1e6,
        1,
    )
    return MaterializeResult(metadata=meta)


@asset(
    name="cafef_financials_templates",
    key_prefix=["raw"],
    group_name="cafef_filings",
    compute_kind="requests",
    # ⚠️ READS raw_data/simplize/industry.csv. The load-bearing part — the template — is
    # FINGERPRINTED over the network (`detect_template` reads the chart of accounts CafeF
    # renders for the ticker), but the sector / industry_group columns beside it come from
    # Simplize. The read is `if os.path.exists(...)`-guarded, so without it the file is
    # still written and those columns are silently BLANK — which is exactly the
    # GICS-vs-fingerprint disagreement templates.csv exists to make visible (HVA sits in
    # the securities group and files corporate).
    deps=[AssetKey(["raw", "simplize_industry"])],
    description=(
        "raw_data/cafef/financials/templates.csv — ticker → which of the 4 charts of "
        "accounts holds it. ⚠️ SEPARATE, UNPARTITIONED ASSET ON PURPOSE: "
        "`build_templates_index` REWRITES the file from exactly the symbols handed to "
        "it rather than upserting, so building it inside a per-ticker partition would "
        "leave a one-row file naming only the ticker that ran last — which is how VCB "
        "lost its row when ACB was parsed alone. Built once, for the whole list."
    ),
)
def cafef_financials_templates(
    context: AssetExecutionContext, repo_logger: RepoLogger
) -> MaterializeResult:
    logger = repo_logger.build()
    FinancialsBuilder.build_templates_index(
        list(CAFEF_FINANCIALS_TICKERS), logger=logger
    )
    return MaterializeResult(
        metadata=landed(
            f"{CAFEF_RAW_DATA_DIR}/financials", pattern="templates.csv"
        )
    )


@asset(
    name="cafef_financials",
    key_prefix=["raw"],
    group_name="cafef_filings",
    compute_kind="ocr",
    partitions_def=FINANCIALS_TICKERS,
    deps=[AssetKey(["raw", "cafef_financials_templates"])],
    # ⚠️ `resource: gpu` caps this to ONE running step: the OCR runs onnxruntime-gpu on a
    # 4 GB RTX 3050, and two partitions is VRAM exhaustion.
    op_tags={"resource": "gpu"},
    description=(
        "One ticker's PDF archive → the three statement CSVs under "
        "raw_data/cafef/financials/statements/<template>/<report>/. LOCAL ONLY — no "
        "network. ⚠️ ~2.4 h PER PARTITION on a full parse; SKIPS years already read "
        "`pdf` in all 3 statements, so a gap-filling re-run is minutes. Set "
        "`skip_existing: false` in config for an authoritative full re-parse. "
        "Replaces `web_scraper/cafef/financials`."
    ),
)
def cafef_financials(
    context: AssetExecutionContext,
    config: FinancialsConfig,
    repo_logger: RepoLogger,
) -> MaterializeResult:
    exchange, symbol = split_ticker_key(context.partition_key)
    logger = repo_logger.build()
    builder = FinancialsBuilder(logger=logger)

    # ⚠️ PRECONDITIONS LIVE IN `preflight`, NOT HERE. It checks the template, the three
    # charts of accounts, the PDF index and the archive — and `build()` calls it itself,
    # so main.py and a notebook get the same protection. An earlier version of this asset
    # duplicated those checks inline, which is how the mechanism would have rotted: the
    # orchestrator would have been the only caller that failed early, and the 2.4 h
    # silent-empty path would have stayed open everywhere else.
    #
    # The PDF archive is checked here rather than declared as a Dagster dep on
    # `cafef_pdfs`: that asset is partitioned over VN100 and this one over the two tickers
    # actually parsed, and a cross-partition-set edge would either demand all 100 PDF
    # partitions or misrepresent the 2-ticker set as 100.
    template = builder.preflight(exchange, symbol)
    context.log.info(f"{exchange}:{symbol} preflight OK -> template '{template}'")

    # `build`, not `build_all` — one ticker, and an exception propagates so THIS
    # partition goes red on its own. `build_all` catches per ticker by design (see
    # cafef_financials.py) because it is main.py's batch path.
    counts = builder.build(exchange, symbol, skip_existing=config.skip_existing)

    meta = landed(
        f"{CAFEF_RAW_DATA_DIR}/financials/statements", pattern=f"*_{exchange}_{symbol}.csv"
    )
    meta["statement_rows"] = MetadataValue.json(counts)
    return MaterializeResult(metadata=meta)


assets += [cafef_pdfs, cafef_financials_templates, cafef_financials]


# ══════════════════════════════════════════════════════════════════════════════
# Simplize — the validated daily backbone, and the industry map
# ══════════════════════════════════════════════════════════════════════════════
@asset(
    name="simplize_stocks",
    key_prefix=["raw"],
    group_name="simplize",
    compute_kind="requests",
    deps=[UNIVERSE],
    description=(
        "Fully dividend-adjusted OHLC, true volume, foreign flow + room → "
        "raw_data/simplize/stocks/. The PRIMARY daily panel (~2.7 M rows). "
        "Replaces `web_scraper/simplize/stocks`."
    ),
)
def simplize_stocks(
    context: AssetExecutionContext, repo_logger: RepoLogger, switches: SwitchConfig
) -> MaterializeResult:
    logger = repo_logger.build()
    scraper = SimplizeScraper(logger=logger, switch_handler=switches.build(logger))
    scraper.scrape_all_stocks()
    return MaterializeResult(metadata=landed(f"{SIMPLIZE_RAW_DATA_DIR}/stocks"))


@asset(
    name="simplize_industry",
    key_prefix=["raw"],
    group_name="simplize",
    compute_kind="requests",
    deps=[UNIVERSE],
    description=(
        "Per-ticker GICS-based industry → raw_data/simplize/industry.csv. Feeds the "
        "GICS tree on silver.stocks_basic. Replaces `web_scraper/simplize/industry`."
    ),
)
def simplize_industry(
    context: AssetExecutionContext, repo_logger: RepoLogger, switches: SwitchConfig
) -> MaterializeResult:
    logger = repo_logger.build()
    scraper = SimplizeScraper(logger=logger, switch_handler=switches.build(logger))
    scraper.scrape_all_industries()
    return MaterializeResult(
        metadata=landed(SIMPLIZE_RAW_DATA_DIR, pattern="industry.csv")
    )


assets += [simplize_stocks, simplize_industry]


# ══════════════════════════════════════════════════════════════════════════════
# GICS — the reference taxonomy (independent of everything)
# ══════════════════════════════════════════════════════════════════════════════
@asset(
    name="gics_structure",
    key_prefix=["raw"],
    group_name="gics",
    compute_kind="requests",
    description=(
        "MSCI's official GICS structure xlsx → raw_data/gics/gics_2023_official.csv "
        "(163 sub-industries). No upstream. Replaces `web_scraper/gics/structure`."
    ),
)
def gics_structure(
    context: AssetExecutionContext, repo_logger: RepoLogger, switches: SwitchConfig
) -> MaterializeResult:
    logger = repo_logger.build()
    handler = switches.build_unblocked(
        logger, "web_scraper", "web_scraper/gics", "web_scraper/gics/structure"
    )
    # GicsScraper has no per-step public method, only the switch-gated `scrape()`, so
    # this is the one place the switch is unblocked rather than bypassed.
    GicsScraper(logger=logger, switch_handler=handler).scrape()
    return MaterializeResult(
        metadata=landed(GICS_RAW_DATA_DIR, pattern="gics_2023_official.csv")
    )


assets.append(gics_structure)
