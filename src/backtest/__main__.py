# src\backtest\__main__.py
"""`python -m backtest --run <run_id> [--ticker VCB] [--top-k 15] [--split test]`."""

from backtest.run import main

if __name__ == "__main__":
    main()
