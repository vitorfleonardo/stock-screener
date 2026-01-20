# 📈 Automated Stock Screener & FII Updater

This project is a Python-based automation tool designed to fetch quotes for Brazilian Stocks (B3) and Real Estate Investment Trusts (FIIs) using the **Yahoo Finance API**. It processes the data and automatically updates a specified **Google Sheets** spreadsheet.

The project is configured to run automatically every weekday via **GitHub Actions**, ensuring your investment dashboard is always up-to-date without manual intervention.

## 🎯 Objective

To provide a robust, serverless solution for tracking personal investment portfolios by:

1.  Fetching batch market data efficiently using `yfinance`.
2.  Cleaning and formatting data with `pandas`.
3.  Pushing the results to a cloud spreadsheet using `gspread` and Google APIs.
4.  Automating the execution pipeline using CI/CD principles.

## 📂 Project Structure

```text
.
├── .github/workflows/
│   └── update.yaml          # GitHub Actions workflow (Cron Job)
├── load_to_sheets.py        # Helper module for Google Sheets authentication & upload
├── main.py                  # Entry point: defines the portfolio and runs the logic
├── requirements.txt         # Python dependencies
├── .env                     # Local environment variables (ignored by Git)
└── credentials.json         # GCP Service Account key (ignored by Git)
```
