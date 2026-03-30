# Google Trends Earnings Screener

A live web app that uses Google Trends search interest as a demand signal to predict next-quarter revenue for consumer-facing public companies. Identifies top long and short trading candidates with backtested models.

## How It Works

1. Fetches 12-month Google Trends data for 41 consumer-facing Russell 1000 companies
2. Scores trend momentum (short/medium/long-term + slope)
3. Deep-validates with 5-year trends vs quarterly revenue correlation
4. Walk-forward backtests 3 regression models per company
5. Predicts next-quarter revenue for top 5 longs and top 5 shorts

## Run Locally

```bash
pip install -r requirements.txt
python app.py
```

Then open http://localhost:5000 and click **Refresh All Data**.

## Deploy to Render

1. Push this repo to GitHub
2. Go to [render.com](https://render.com) -> New -> Web Service
3. Connect your GitHub repo
4. Render auto-detects render.yaml -- just click Deploy

## Tech Stack

- **Backend:** Flask + gunicorn
- **Data:** pytrends (Google Trends), yfinance (financials/prices)
- **Models:** scipy linear regression, walk-forward backtesting
- **Frontend:** Vanilla JS + Chart.js (dark theme)

## Disclaimer

This is a screening tool for research purposes only, not investment advice.
