# src\main.py

from logger.logger import LogType, Logger
from web_scraper.trading_view_scraper import TradingViewScraper
from web_scraper.cafef_scraper import CafeFScraper
from web_scraper.cafef_index_scraper import CafeFIndexScraper
from web_scraper.cafef_news_scraper import CafeFNewsScraper
from web_scraper.cafef_pdf_scraper import CafeFPdfScraper
from web_scraper.cafef_financials import FinancialsBuilder
from web_scraper.simplize_scraper import SimplizeScraper
from web_scraper.gics_scraper import GicsScraper
from data_preprocessor.data_preprocessor import DataPreprocessor
from data_postprocessor.data_postprocessor import DataPostprocessor, MarketIndexConfig
from utils.switch_handler import SwitchHandler
from utils.constants import LOG_FILE_BASE
from utils.enums import *
from plyer import notification


def main():
    my_logger = Logger(file_name=LOG_FILE_BASE, level=LogType.INFO)
    my_logger.log_info("START")

    my_switch_handler = SwitchHandler(logger=my_logger)

    # ── Web scraping ──────────────────────────────────────────────────────
    # TradingView first: it produces the stock link CSVs that CafeF derives its
    # ticker universe from.
    TradingViewScraper(
        logger=my_logger, switch_handler=my_switch_handler, power=100
    ).scrape()

    # CafeF fills the per-stock fields TradingView lacks (raw/adjusted close,
    # matched/negotiated volume, foreign flow); depends on the links above.
    CafeFScraper(logger=my_logger, switch_handler=my_switch_handler).scrape()

    # The same CafeF tabs for the six MARKET INDICES (VNINDEX, VN30, VN100, HNX,
    # HNX30, UPCOM). Independent of the TradingView links — an index has no link CSV,
    # so its universe is the fixed list on the class and this can run standalone.
    CafeFIndexScraper(logger=my_logger, switch_handler=my_switch_handler).scrape()

    # CafeF company news / disclosure feed — an event stream, not a daily series, so it
    # is a separate scraper on its own leaf (`web_scraper/cafef/news`).
    CafeFNewsScraper(logger=my_logger, switch_handler=my_switch_handler).scrape()

    # ── CafeF financial statements: download, then parse ──────────────────────
    # Two stages that must run in this order and are deliberately separate leaves —
    # the download is network-bound and ~1.7 GB per ticker, the parse is CPU/OCR-bound
    # and ~2.4 h per ticker, and the parse can be re-run over an archive already on
    # disk (which is the common case after a parser fix).
    CafeFPdfScraper(logger=my_logger, switch_handler=my_switch_handler).scrape()

    # Reads the LOCAL archive above — no network. Not a BaseScraper, so it is driven by
    # its own switch-aware batch entry point rather than `scrape()`.
    FinancialsBuilder.build_all(logger=my_logger, switch_handler=my_switch_handler)

    # Simplize: the validated backbone for the daily panel — fully-adjusted OHLC,
    # true volume, and foreign flow (2009→) — also derives its universe from the
    # TradingView links above.
    SimplizeScraper(logger=my_logger, switch_handler=my_switch_handler).scrape()

    # GICS reference taxonomy from MSCI (independent of the others).
    GicsScraper(logger=my_logger, switch_handler=my_switch_handler).scrape()

    my_data_preprocessor = DataPreprocessor(
        logger=my_logger,
        switch_handler=my_switch_handler,
    )
    my_data_preprocessor.ingest_bronze_data()
    my_data_preprocessor.ingest_silver_data()
    my_data_preprocessor.ingest_gold_data()

    # stock_code_list = STOCK_CODES_TO_BE_EXPORTED_TO_GOLD_DB

    # my_data_postprocessor = DataPostprocessor(
    #     logger=my_logger,
    #     switch_handler=my_switch_handler,
    #     stock_list=stock_code_list,
    #     include_macroeconomics=False,  # commented out for now
    #     market_index_configs=[
    #         MarketIndexConfig(index_code="VNINDEX", prefix="vnindex"),
    #         MarketIndexConfig(index_code="HNX-INDEX", prefix="hnx_index"),
    #         MarketIndexConfig(index_code="UPCOM-INDEX", prefix="upcom_index"),
    #     ],
    # )
    # my_data_postprocessor.export_unified_dataframe()

    # for stock_code in stock_code_list:
    #     my_logger.log_info(f"Exporting unified dataframe for stock code: {stock_code}")
    #     my_data_postprocessor.export_unified_dataframe(stock_code=stock_code)

    notification.notify(
        title="Complete main.py",
        message="main.py has completed execution.",
        timeout=5,  # seconds
    )


if __name__ == "__main__":
    main()
