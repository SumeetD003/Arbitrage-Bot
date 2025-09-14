# Titan v4 – Cointegration-Based Pairs Trading Bot

![Trading Bot Banner](https://img.shields.io/badge/Trading-Bot-blue?style=for-the-badge)
*Automated statistical arbitrage strategy using Interactive Brokers (IBKR)*


##  Overview

**Titan v4** is an asynchronous, fully automated **pairs trading system** built on Python.
It leverages **cointegration tests** to identify stock pairs with strong mean-reverting relationships and executes trades through **Interactive Brokers (IBKR)** using the `ib_insync` library.

The bot continuously:

1. Connects to IBKR TWS or Gateway.
2. Fetches market & historical data.
3. Identifies cointegrated stock pairs.
4. Enters and manages long/short spread positions.
5. Logs all trades into session-based CSV reports.


##  Key Features

* **Cointegration-based Pairs Selection** (via `statsmodels`).
* **Dynamic Trade Execution** with `ib_insync`.
* **Risk Management**: Z-score thresholds, stop-loss, max holding period.
* **Async Event Loop** for real-time trading.
* **Session Logs**: Each run creates its own folder with trade history.
* **Supports Equities & Forex** (pre-loaded with symbols like `AAPL`, `MSFT`, `EURUSD`, etc.).



## Strategy Flow

flowchart TD
    A[Start Bot] --> B[Connect to IBKR API]
    B --> C[Fetch Account Balance & Symbols]
    C --> D[Formation Period: Historical Data]
    D --> E[Analyze Pairs (Cointegration, Half-life, Sharpe)]
    E --> F[Select Top Pairs]
    F --> G[Trading Period Active?]
    G -->|Yes| H[Fetch Latest Bars]
    H --> I[Generate Signals]
    I --> J[Open/Close Positions]
    J --> K[Update Trade Log CSV]
    G -->|No| D
    K --> G
```

##  Installation

###  Clone Repository

```bash
git clone https://github.com/yourusername/titan-v4.git
cd titan-v4
```

###  Install Dependencies

```bash
pip install -r requirements.txt
```

**Requirements**:

* Python 3.9+
* `ib_insync`
* `pandas`
* `numpy`
* `statsmodels`
* `joblib`
* `pytz`

### Setup IBKR TWS / Gateway

* Install [IBKR TWS](https://www.interactivebrokers.com/en/trading/ib-api.php) or IB Gateway.
* Enable API access:
  `Configure > API > Settings > Enable ActiveX and Socket Clients`
* Default connection:

  * Host: `127.0.0.1`
  * Port: `7497`
  * Client ID: `1`


##  Running the Bot

```bash
python Titan\ v4.py
```

### Interactive Prompt

On startup, the bot will ask for a timeframe:
Select the timeframe:
1: 1 min
2: 5 mins
3: 15 mins
4: 30 mins
5: 1 hour
6: 1 day
Enter the number corresponding to the desired timeframe:


##  Risk Management

* **Entry Threshold**: Z-score ±2
* **Exit Threshold**: Z-score ±0.5
* **Stop-loss**: 50% of spread deviation
* **Risk per Trade**: 1% of account balance
* **Max Holding Period**: 10,000 bars

##  Disclaimer

This project is provided **for educational purposes only**.
Trading involves substantial risk — **use at your own risk**.
Always test thoroughly in **paper trading mode** before live deployment.


