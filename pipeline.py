"""
Consolidated data pipeline: Google Trends + Yahoo Finance + regression backtesting.
Runs the full analysis and returns JSON-ready data for the dashboard.
Uses direct Yahoo Finance API with crumb auth (works from cloud IPs).
"""

import json
import time
import random
import math
import requests as req_lib
import numpy as np
from datetime import datetime, timedelta
from pytrends.request import TrendReq
from scipy import stats

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# Step 0: Company definitions
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
COMPANIES = {
    "AAPL": ("Apple iPhone", "Apple", "Technology"),
    "TSLA": ("Tesla", "Tesla", "Auto / EV"),
    "AMZN": ("Amazon shopping", "Amazon", "E-Commerce"),
    "GOOG": ("Google", "Alphabet", "Technology"),
    "META": ("Instagram", "Meta Platforms", "Social Media"),
    "NFLX": ("Netflix", "Netflix", "Streaming"),
    "DIS": ("Disney+", "Walt Disney", "Entertainment"),
    "SPOT": ("Spotify", "Spotify", "Streaming"),
    "ROKU": ("Roku", "Roku", "Streaming"),
    "SNAP": ("Snapchat", "Snap Inc", "Social Media"),
    "PINS": ("Pinterest", "Pinterest", "Social Media"),
    "RBLX": ("Roblox", "Roblox", "Gaming"),
    "TTWO": ("GTA 6", "Take-Two Interactive", "Gaming"),
    "EA": ("EA Sports", "Electronic Arts", "Gaming"),
    "NKE": ("Nike shoes", "Nike", "Retail / Apparel"),
    "LULU": ("Lululemon", "Lululemon", "Retail / Apparel"),
    "TGT": ("Target store", "Target", "Retail"),
    "WMT": ("Walmart", "Walmart", "Retail"),
    "COST": ("Costco", "Costco", "Retail"),
    "SBUX": ("Starbucks", "Starbucks", "Restaurants"),
    "MCD": ("McDonalds", "McDonald's", "Restaurants"),
    "CMG": ("Chipotle", "Chipotle", "Restaurants"),
    "DPZ": ("Dominos pizza", "Domino's", "Restaurants"),
    "DASH": ("DoorDash", "DoorDash", "Delivery"),
    "UBER": ("Uber", "Uber", "Ride-hailing"),
    "ABNB": ("Airbnb", "Airbnb", "Travel"),
    "BKNG": ("Booking.com", "Booking Holdings", "Travel"),
    "EXPE": ("Expedia", "Expedia", "Travel"),
    "LLY": ("Mounjaro", "Eli Lilly", "Pharma / GLP-1"),
    "NVO": ("Ozempic", "Novo Nordisk", "Pharma / GLP-1"),
    "HIMS": ("Hims", "Hims & Hers", "Telehealth"),
    "PTON": ("Peloton", "Peloton", "Fitness"),
    "PYPL": ("PayPal", "PayPal", "Fintech"),
    "SOFI": ("SoFi", "SoFi Technologies", "Fintech"),
    "COIN": ("Coinbase", "Coinbase", "Crypto"),
    "RIVN": ("Rivian", "Rivian", "Auto / EV"),
    "LCID": ("Lucid Motors", "Lucid Group", "Auto / EV"),
    "ENPH": ("Enphase solar", "Enphase Energy", "Solar"),
    "NVDA": ("Nvidia GPU", "Nvidia", "AI / Chips"),
    "CRM": ("Salesforce", "Salesforce", "Cloud / SaaS"),
    "PLTR": ("Palantir", "Palantir", "AI / Data"),
}


def safe_nan(v):
    """Return None if NaN, else float."""
    if v is None:
        return None
    try:
        if math.isnan(v):
            return None
    except TypeError:
        pass
    return float(v)


def log(msg, callback=None):
    """Print and optionally send to a progress callback."""
    print(msg, flush=True)
    if callback:
        callback(msg)


MAX_RETRIES = 4


def new_pytrends():
    """Create a fresh TrendReq session (new cookies)."""
    return TrendReq(hl='en-US', tz=360, retries=3, backoff_factor=1.0)


def fetch_trends_with_retry(pytrends_obj, terms, timeframe, log_fn, progress_cb):
    """Fetch Google Trends with retry + exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            if attempt > 0:
                pytrends_obj = new_pytrends()
            pytrends_obj.build_payload(terms, timeframe=timeframe, geo='US')
            data = pytrends_obj.interest_over_time()
            return pytrends_obj, data
        except Exception as e:
            wait = (attempt + 1) * 15 + random.uniform(5, 15)
            log_fn(f"    Trends retry {attempt+1}/{MAX_RETRIES}: {e} (waiting {wait:.0f}s)", progress_cb)
            time.sleep(wait)
    return pytrends_obj, Nonettempt > 0:
                pytrends_obj = new_pytrends()
            pytrends_obj.build_payload(terms, timeframe=timeframe, geo='US')
            data = pytrends_obj.interest_over_time()
            return pytrends_obj, data
        except Exception as e:
            wait = (attempt + 1) * 15 + random.uniform(5, 15)
            log_fn(f"    Trends retry {attempt+1}/{MAX_RETRIES}: {e} (waiting {wait:.0f}s)", progress_cb)
            time.sleep(wait)
    return pytrends_obj, None


# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# Yahoo Finance direct API (bypasses yfinance library rate limits)
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

class YahooFinanceDirect:
    """Direct Yahoo Finance API access using crumb authentication.
    Works reliably from cloud/datacenter IPs where yfinance gets 429'd."""

    def __init__(self):
        self.session = req_lib.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        self.crumb = None
        self._init_crumb()

    def _init_crumb(self):
        """Get authentication crumb + cookies from Yahoo."""
        try:
            self.session.get('https://fc.yahoo.com', allow_redirects=True, timeout=10)
            r = self.session.get('https://query2.finance.yahoo.com/v1/test/getcrumb', timeout=10)
            if r.status_code == 200:
                self.crumb = r.text.strip()
        except Exception:
            self.crumb = None

    def refresh_crumb(self):
        """Get a fresh crumb if the old one expires."""
        self.session.cookies.clear()
        self._init_crumb()

    def get_quote_summary(self, ticker, modules=None):
        """Fetch quote summary data (financials, key stats, etc)."""
        if modules is None:
            modules = ['financialData', 'incomeStatementHistoryQuarterly',
                       'incomeStatementHistory', 'defaultKeyStatistics',
                       'summaryDetail']

        for attempt in range(3):
            if not self.crumb:
                self.refresh_crumb()
            if not self.crumb:
                time.sleep(2)
                continue

            mod_str = '&modules='.join(modules)
            url = f'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules={mod_str}&crumb={self.crumb}'

            try:
                r = self.session.get(url, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    result = data.get('quoteSummary', {}).get('result', [])
                    if result:
                        return result[0]
                elif r.status_code == 401:
                    self.refresh_crumb()
                    time.sleep(1)
                    continue
                elif r.status_code == 429:
                    time.sleep(5 * (attempt + 1))
                    continue
            except Exception:
                time.sleep(2)

        return None

    def get_chart(self, ticker, range_str='5y', interval='1mo'):
        """Fetch price history using v8 chart endpoint."""
        url = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range={range_str}&interval={interval}'
        try:
            r = self.session.get(url, timeout=15)
            if r.status_code == 200:
                data = r.json()
                result = data.get('chart', {}).get('result', [])
                if result:
                    return result[0]
        except Exception:
            pass
        return None

    def get_quarterly_revenue(self, ticker):
        """Get quarterly revenue series as (dates, values) tuples."""
        data = self.get_quote_summary(ticker, ['incomeStatementHistoryQuarterly'])
        if not data:
            return [], []

        stmts = (data.get('incomeStatementHistoryQuarterly', {})
                     .get('incomeStatementHistory', []))

        dates = []
        vals = []
        for s in reversed(stmts):  # oldest first
            rev = s.get('totalRevenue', {}).get('raw')
            date_epoch = s.get('endDate', {}).get('raw')
            if rev and date_epoch:
                dates.append(datetime.utcfromtimestamp(date_epoch))
                vals.append(float(rev))

        return dates, vals

    def get_quarterly_financials(self, ticker):
        """Get quarterly revenue, gross profit, operating income."""
        data = self.get_quote_summary(ticker, ['incomeStatementHistoryQuarterly'])
        if not data:
            return []

        stmts = (data.get('incomeStatementHistoryQuarterly', {})
                     .get('incomeStatementHistory', []))

        records = []
        for s in reversed(stmts):
            date_epoch = s.get('endDate', {}).get('raw')
            if not date_epoch:
                continue
            dt = datetime.utcfromtimestamp(date_epoch)
            records.append({
                'date': dt,
                'revenue': s.get('totalRevenue', {}).get('raw'),
                'gross_profit': s.get('grossProfit', {}).get('raw'),
                'op_income': s.get('operatingIncome', {}).get('raw'),
            })

        return records

    def get_stock_info(self, ticker):
        """Get key stock info (price, market cap, PE, etc)."""
        data = self.get_quote_summary(ticker, ['financialData', 'defaultKeyStatistics', 'summaryDetail'])
        if not data:
            return {}

        fd = data.get('financialData', {})
        ks = data.get('defaultKeyStatistics', {})
        sd = data.get('summaryDetail', {})

        return {
            'currentPrice': fd.get('currentPrice', {}).get('raw'),
            'marketCap': sd.get('marketCap', {}).get('raw', 0),
            'trailingPE': sd.get('trailingPE', {}).get('raw'),
            'forwardPE': sd.get('forwardPE', {}).get('raw') or ks.get('forwardPE', {}).get('raw'),
            'totalRevenue': fd.get('totalRevenue', {}).get('raw'),
        }

    def get_price_history(self, ticker, range_str='1y', interval='1wk'):
        """Get price history as (dates, prices) lists."""
        chart = self.get_chart(ticker, range_str, interval)
        if not chart:
            return [], []

        timestamps = chart.get('timestamp', [])
        closes = chart.get('indicators', {}).get('adjclose', [{}])[0].get('adjclose', [])
        if not closes:
            closes = chart.get('indicators', {}).get('quote', [{}])[0].get('close', [])

        dates = []
        prices = []
        for ts, p in zip(timestamps, closes):
            if p is not None:
                dates.append(datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d'))
                prices.append(float(p))

        return dates, prices


def run_pipeline(progress_cb=None):
    """
    Runs the full pipeline end-to-end. Returns a dict with:
      - 'companies': list of company data dicts (the JS data)
      - 'n_longs': int
      - 'n_shorts': int
      - 'n_sig': int
      - 'avg_err': float
      - 'updated': ISO timestamp
    """
    pytrends = new_pytrends()
    yf_api = YahooFinanceDirect()

    if not yf_api.crumb:
        log("  Warning: Could not get Yahoo Finance crumb, retrying...", progress_cb)
        time.sleep(3)
        yf_api = YahooFinanceDirect()

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # Step 1: Fetch 12-month Google Trends
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    log("Step 1/6: Fetching 12-month Google Trends...", progress_cb)
    trends_1y = {}
    tickers = list(COMPANIES.keys())

    for i in range(0, len(tickers), 4):
        batch = tickers[i:i+4]
        terms = [COMPANIES[t][0] for t in batch]
        pytrends, data = fetch_trends_with_retry(pytrends, terms, 'today 12-m', log, progress_cb)
        if data is not None and not data.empty:
            for tk, term in zip(batch, terms):
                if term in data.columns:
                    trends_1y[tk] = {
                        "search_term": term,
                        "name": COMPANIES[tk][1],
                        "sector": COMPANIES[tk][2],
                        "dates": [d.strftime('%Y-%m-%d') for d in data.index],
                        "values": data[term].tolist()
                    }
        time.sleep(random.uniform(5, 10))

    log(f"  Got 12m trends for {len(trends_1y)} companies", progress_cb)

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # Step 2: Score trends (composite momentum)
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    log("Step 2/6: Scoring trend momentum...", progress_cb)
    scored = {}
    for ticker, info in trends_1y.items():
        values = info['values']
        n = len(values)
        if n < 12:
            continue

        recent_4w = np.mean(values[-4:])
        prior_4w = np.mean(values[-8:-4])
        short_momentum = ((recent_4w - prior_4w) / max(prior_4w, 1)) * 100

        recent_q = np.mean(values[-13:])
        prior_q = np.mean(values[-26:-13]) if n >= 26 else np.mean(values[:13])
        medium_momentum = ((recent_q - prior_q) / max(prior_q, 1)) * 100

        if n >= 52:
            recent_half = np.mean(values[-26:])
            prior_half = np.mean(values[:26])
            long_momentum = ((recent_half - prior_half) / max(prior_half, 1)) * 100
        else:
            long_momentum = medium_momentum

        recent_13 = values[-13:]
        x = np.arange(13)
        slope = np.polyfit(x, recent_13, 1)[0]
        avg_level = np.mean(recent_13)
        norm_slope = (slope / max(avg_level, 1)) * 100

        composite = (
            short_momentum * 0.25 +
            medium_momentum * 0.35 +
            long_momentum * 0.20 +
            norm_slope * 15 * 0.20
        )

        if composite > 10:
            signal = "STRONG LONG"
        elif composite > 3:
            signal = "LONG"
        elif composite < -10:
            signal = "STRONG SHORT"
        elif composite < -3:
            signal = "SHORT"
        else:
            signal = "NEUTRAL"

        scored[ticker] = {
            **info,
            "composite_score": round(composite, 1),
            "signal": signal
        }

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # Step 3: Deep validation â 5-year trends + revenue correlation
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    log("Step 3/6: Deep validation (5-year trends + revenue)...", progress_cb)
    deep_results = {}

    for tk in list(scored.keys()):
        info = scored[tk]
        search_term = info['search_term']

        try:
            # Get quarterly revenue via direct Yahoo API
            financials = yf_api.get_quarterly_financials(tk)
            if not financials or len(financials) < 4:
                log(f"  {tk} skip: insufficient revenue data", progress_cb)
                continue

            rev_dates = [f['date'] for f in financials if f['revenue']]
            rev_vals = [float(f['revenue']) for f in financials if f['revenue']]
            if len(rev_vals) < 4:
                continue

            # 5-year trends with retry
            pytrends, td5 = fetch_trends_with_retry(pytrends, [search_term], 'today 5-y', log, progress_cb)
            if td5 is None or td5.empty:
                continue
            trend_vals_5y = td5[search_term].tolist()
            trend_dates_5y = list(td5.index)
            time.sleep(random.uniform(5, 10))

        except Exception as e:
            log(f"  {tk} skip: {e}", progress_cb)
            time.sleep(2)
            continue

        # Map trends to quarters
        q_trends_same = []
        q_revs = []
        q_dates_str = []

        for i, (rd, rv) in enumerate(zip(rev_dates, rev_vals)):
            q_end = rd
            q_start = q_end - timedelta(days=90)
            q_trend = [trend_vals_5y[j] for j, td_dt in enumerate(trend_dates_5y) if q_start <= td_dt <= q_end]
            if q_trend:
                q_trends_same.append(np.mean(q_trend))
                q_revs.append(rv)
                q_dates_str.append(rd.strftime('%Y-%m-%d'))

        n_q = len(q_revs)
        if n_q < 4:
            continue

        # Correlations
        corr_contemp = safe_nan(float(np.corrcoef(q_trends_same, q_revs)[0, 1]))

        # Prior quarter trends
        q_trends_prior = [None] + q_trends_same[:-1]
        valid_lead = [(p, r) for p, r in zip(q_trends_prior, q_revs) if p is not None]
        corr_leading = None
        if len(valid_lead) >= 4:
            lp, lr = zip(*valid_lead)
            corr_leading = safe_nan(float(np.corrcoef(lp, lr)[0, 1]))

        # Changes correlation
        corr_changes = None
        dir_acc = None
        if n_q >= 5:
            t_chg = np.diff(q_trends_same)
            r_chg_pct = [((q_revs[i+1] - q_revs[i]) / abs(q_revs[i])) * 100 if q_revs[i] != 0 else 0 for i in range(len(q_revs)-1)]
            corr_changes = safe_nan(float(np.corrcoef(t_chg, r_chg_pct)[0, 1]))
            if len(t_chg) >= 3:
                correct = sum(1 for tc, rc in zip(t_chg, r_chg_pct) if (tc > 0 and rc > 0) or (tc < 0 and rc < 0))
                dir_acc = round(correct / len(t_chg) * 100, 0)

        # Price correlation
        corr_price = None
        try:
            _, prices_5y = yf_api.get_price_history(tk, '5y', '1mo')
            if len(prices_5y) >= 20:
                min_len = min(len(trend_vals_5y), len(prices_5y))
                corr_price = safe_nan(float(np.corrcoef(trend_vals_5y[-min_len:], prices_5y[-min_len:])[0, 1]))
        except:
            pass

        # Pass/fail
        passes = False
        if corr_contemp and corr_contemp > 0.4:
            passes = True
        if corr_leading and corr_leading > 0.4:
            passes = True
        if corr_changes and corr_changes > 0.35:
            passes = True
        if dir_acc and dir_acc >= 60:
            passes = True
        if corr_price and abs(corr_price) >= 0.4:
            passes = True

        if passes:
            deep_results[tk] = {
                'name': info['name'],
                'sector': info['sector'],
                'search_term': search_term,
                'n_quarters': n_q,
                'corr_contemp': corr_contemp,
                'corr_leading': corr_leading,
                'corr_changes': corr_changes,
                'dir_accuracy': dir_acc,
                'corr_price_5y': corr_price,
            }

    log(f"  {len(deep_results)} companies passed deep validation", progress_cb)

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # Step 4: Select top 5 longs and top 5 shorts
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    log("Step 4/6: Selecting top longs and shorts...", progress_cb)
    passed = {k: v for k, v in deep_results.items() if k in scored}
    by_score = sorted(passed.items(), key=lambda x: scored[x[0]]['composite_score'], reverse=True)

    top_longs = [t for t, _ in by_score if scored[t]['composite_score'] > 3][:5]
    top_shorts = [t for t, _ in by_score if scored[t]['composite_score'] < -3][-5:]
    top_shorts.reverse()

    targets = top_longs + top_shorts
    if not targets:
        targets = [t for t, _ in by_score[:5]] + [t for t, _ in by_score[-5:]]
        top_longs = targets[:5]
        top_shorts = targets[5:]

    log(f"  Longs: {top_longs}  Shorts: {top_shorts}", progress_cb)

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # Step 5: Backtest + predict for targets
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    log("Step 5/6: Running backtests and predictions...", progress_cb)
    results = {}

    for ticker in targets:
        info = deep_results[ticker]
        search_term = info['search_term']

        try:
            # Get financial data
            financials = yf_api.get_quarterly_financials(ticker)
            stock_info = yf_api.get_stock_info(ticker)

            if not financials or len(financials) < 4:
                continue

            rev_dates = [f['date'] for f in financials if f['revenue']]
            rev_vals = [float(f['revenue']) for f in financials if f['revenue']]

            # 5-year trends with retry
            pytrends, td5 = fetch_trends_with_retry(pytrends, [search_term], 'today 5-y', log, progress_cb)
            if td5 is None or td5.empty:
                log(f"  {ticker} backtest skip: could not fetch 5y trends", progress_cb)
                continue
            trend_vals_5y = td5[search_term].tolist()
            trend_dates_5y = list(td5.index)
            time.sleep(random.uniform(5, 10))

            # Recent 3 months with retry
            pytrends, rd = fetch_trends_with_retry(pytrends, [search_term], 'today 3-m', log, progress_cb)
            recent_vals = rd[search_term].tolist() if rd is not None and not rd.empty else []
            recent_dates = [d.strftime('%Y-%m-%d') for d in rd.index] if rd is not None and not rd.empty else []
            time.sleep(random.uniform(5, 10))

        except Exception as e:
            log(f"  {ticker} backtest skip: {e}", progress_cb)
            continue

        # Map to quarters
        quarterly_data = []
        for i, f in enumerate(financials):
            if not f['revenue']:
                continue
            rdt = f['date']
            rval = float(f['revenue'])
            q_end = rdt
            q_start = q_end - timedelta(days=90)
            q_trend = [trend_vals_5y[j] for j, tdt in enumerate(trend_dates_5y) if q_start <= tdt <= q_end]
            gp_val = float(f['gross_profit']) if f.get('gross_profit') else None
            oi_val = float(f['op_income']) if f.get('op_income') else None
            if q_trend:
                quarterly_data.append({
                    'date': rdt.strftime('%Y-%m-%d'),
                    'revenue': rval,
                    'revenue_m': round(rval / 1e6, 1),
                    'gross_profit_m': round(gp_val / 1e6, 1) if gp_val else None,
                    'op_income_m': round(oi_val / 1e6, 1) if oi_val else None,
                    'gross_margin': round((gp_val / rval) * 100, 1) if gp_val and rval else None,
                    'op_margin': round((oi_val / rval) * 100, 1) if oi_val and rval else None,
                    'avg_trend': round(np.mean(q_trend), 2),
                })

        if len(quarterly_data) < 4:
            continue

        X = np.array([q['avg_trend'] for q in quarterly_data])
        Y = np.array([q['revenue'] for q in quarterly_data])
        X_lag = np.array([quarterly_data[i-1]['avg_trend'] if i > 0 else X[0] for i in range(len(X))])

        # Walk-forward backtest
        backtest = []
        for split in range(3, len(quarterly_data)):
            Xt, Yt, Xlt = X[:split], Y[:split], X_lag[:split]
            s1, i1, r1, p1, _ = stats.linregress(Xt, Yt)
            pred_simple = s1 * X[split] + i1
            if split >= 3:
                s2, i2, _, _, _ = stats.linregress(Xlt[1:], Yt[1:])
            else:
                s2, i2 = s1, i1
            pred_lag = s2 * X_lag[split] + i2
            pred_combo = (pred_simple + pred_lag) / 2
            actual = Y[split]
            backtest.append({
                'quarter': quarterly_data[split]['date'],
                'actual_m': round(actual / 1e6, 1),
                'pred_simple_m': round(pred_simple / 1e6, 1),
                'pred_lag_m': round(pred_lag / 1e6, 1),
                'pred_combo_m': round(pred_combo / 1e6, 1),
                'err_simple': round(((pred_simple - actual) / actual) * 100, 1),
                'err_lag': round(((pred_lag - actual) / actual) * 100, 1),
                'err_combo': round(((pred_combo - actual) / actual) * 100, 1),
                'train_size': split,
            })

        if not backtest:
            continue

        avg_err = {m: np.mean([abs(b[f'err_{m}']) for b in backtest]) for m in ['simple', 'lag', 'combo']}
        best_model = min(avg_err, key=avg_err.get)
        best_err = avg_err[best_model]

        # Final model on all data
        s_f, i_f, r_f, p_f, _ = stats.linregress(X, Y)
        s_l, i_l, _, _, _ = stats.linregress(X_lag[1:], Y[1:])

        current_q_trend = np.mean(recent_vals) if recent_vals else X[-1]
        last_q_trend = X[-1]

        preds = {
            'simple': s_f * current_q_trend + i_f,
            'lag': s_l * last_q_trend + i_l,
        }
        preds['combo'] = (preds['simple'] + preds['lag']) / 2
        pred_next = preds[best_model]

        bt_errors = [b[f'err_{best_model}'] for b in backtest]
        error_std = np.std(bt_errors) if len(bt_errors) > 1 else best_err
        margin = best_err + error_std

        last_rev = rev_vals[-1]
        yoy_rev = rev_vals[-4] if len(rev_vals) >= 4 else rev_vals[0]

        recent_gm = [q['gross_margin'] for q in quarterly_data[-3:] if q.get('gross_margin')]
        recent_om = [q['op_margin'] for q in quarterly_data[-3:] if q.get('op_margin')]
        avg_gm = np.mean(recent_gm) if recent_gm else None
        avg_om = np.mean(recent_om) if recent_om else None

        pred_gp = pred_next * (avg_gm / 100) if avg_gm else None
        pred_oi = pred_next * (avg_om / 100) if avg_om else None

        # Price history
        price_dates_1y, prices_1y = yf_api.get_price_history(ticker, '1y', '1wk')

        results[ticker] = {
            'name': info['name'],
            'sector': info['sector'],
            'search_term': search_term,
            'composite_score': scored[ticker]['composite_score'],
            'signal': scored[ticker]['signal'],
            'quarterly_data': quarterly_data,
            'backtest': backtest,
            'best_model': best_model,
            'avg_backtest_error': round(best_err, 1),
            'error_std': round(error_std, 1),
            'r_squared': round(r_f**2, 3),
            'p_value': round(p_f, 4),
            'current_q_trend': round(float(current_q_trend), 1),
            'recent_trend_vals': recent_vals[-13:],
            'recent_trend_dates': recent_dates[-13:],
            'prediction': {
                'revenue_m': round(pred_next / 1e6, 1),
                'low_m': round(pred_next * (1 - margin / 100) / 1e6, 1),
                'high_m': round(pred_next * (1 + margin / 100) / 1e6, 1),
                'last_actual_m': round(last_rev / 1e6, 1),
                'implied_qoq_pct': round(((pred_next - last_rev) / abs(last_rev)) * 100, 1),
                'implied_yoy_pct': round(((pred_next - yoy_rev) / abs(yoy_rev)) * 100, 1),
                'pred_gross_profit_m': round(pred_gp / 1e6, 1) if pred_gp else None,
                'pred_op_income_m': round(pred_oi / 1e6, 1) if pred_oi else None,
                'assumed_gross_margin': round(float(avg_gm), 1) if avg_gm else None,
                'assumed_op_margin': round(float(avg_om), 1) if avg_om else None,
            },
            'current_price': stock_info.get('currentPrice'),
            'market_cap_b': round(stock_info.get('marketCap', 0) / 1e9, 1),
            'pe': round(stock_info.get('trailingPE', 0), 1) if stock_info.get('trailingPE') else None,
            'fwd_pe': round(stock_info.get('forwardPE', 0), 1) if stock_info.get('forwardPE') else None,
            'prices_1y': prices_1y,
            'price_dates_1y': price_dates_1y,
            'trend_dates_5y': [d.strftime('%Y-%m-%d') for d in trend_dates_5y],
            'trend_vals_5y': trend_vals_5y,
            'all_preds': {k: round(v / 1e6, 1) for k, v in preds.items()},
            'all_errors': {k: round(v, 1) for k, v in avg_err.items()},
        }

        log(f"  {ticker}: {best_model} model, {best_err:.1f}% error, pred ${pred_next/1e6:.0f}M", progress_cb)

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # Step 6: Package for frontend
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    log("Step 6/6: Packaging results...", progress_cb)

    all_companies = [{'ticker': tk, **v} for tk, v in results.items()]
    longs = sorted([c for c in all_companies if c['composite_score'] > 0], key=lambda x: x['composite_score'], reverse=True)
    shorts = sorted([c for c in all_companies if c['composite_score'] <= 0], key=lambda x: x['composite_score'])

    js_data = []
    for c in longs + shorts:
        js_data.append({
            "tk": c["ticker"], "nm": c["name"], "sec": c["sector"],
            "st": c["search_term"], "cs": c["composite_score"], "sig": c["signal"],
            "qd": c["quarterly_data"], "bt": c["backtest"],
            "bm": c["best_model"], "bte": c["avg_backtest_error"],
            "estd": c["error_std"], "r2": c["r_squared"], "pv": c["p_value"],
            "cqt": c["current_q_trend"],
            "rtv": c.get("recent_trend_vals", []), "rtd": c.get("recent_trend_dates", []),
            "pr": c["prediction"],
            "cp": c["current_price"], "mcb": c["market_cap_b"],
            "pe": c.get("pe"), "fpe": c.get("fwd_pe"),
            "p1y": c.get("prices_1y", []), "pd1y": c.get("price_dates_1y", []),
            "td5y": c.get("trend_dates_5y", []), "tv5y": c.get("trend_vals_5y", []),
            "ap": c.get("all_preds", {}), "ae": c.get("all_errors", {}),
        })

    n_longs = len(longs)
    n_shorts = len(shorts)
    n_sig = sum(1 for c in all_companies if c['p_value'] < 0.05)
    avg_err_all = round(sum(c['avg_backtest_error'] for c in all_companies) / max(len(all_companies), 1), 1)

    output = {
        'companies': js_data,
        'n_longs': n_longs,
        'n_shorts': n_shorts,
        'n_sig': n_sig,
        'avg_err': avg_err_all,
        'updated': datetime.utcnow().isoformat() + 'Z',
    }

    log(f"Pipeline complete â {len(js_data)} companies", progress_cb)
    return output


if __name__ == '__main__':
    result = run_pipeline()
    with open('data.json', 'w') as f:
        json.dump(result, f)
    print(f"Saved {len(result['companies'])} companies to data.json")
