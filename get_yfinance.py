import yfinance as yf
import pandas as pd
import time
import logging

# =============================
# Logging setup
# =============================
logging.basicConfig(
    filename="scraper.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
console = logging.getLogger("console")
console.setLevel(logging.INFO)
console.addHandler(logging.StreamHandler())

# =============================
# Formatting helpers
# =============================
def format_number(n):
    if n is None:
        return None
    try:
        n = float(n)
    except (ValueError, TypeError):
        return n
    if abs(n) >= 1_000_000_000_000:
        return f"{n/1_000_000_000_000:.2f} T"
    elif abs(n) >= 1_000_000_000:
        return f"{n/1_000_000_000:.2f} B"
    elif abs(n) >= 1_000_000:
        return f"{n/1_000_000:.2f} M"
    else:
        return f"{n:.2f}"

def format_percent(p):
    if p is None:
        return None
    try:
        return f"{float(p):.2f}%" if p > 1 else f"{float(p) * 100:.2f}%"
    except (ValueError, TypeError):
        return p

def parse_major_holders(ticker):
    """Extract major holder percentages: institutional vs insider/retail."""
    try:
        t = yf.Ticker(ticker)
        major = t.major_holders
        if major is None or major.empty:
            return pd.Series({"Insider (%)": None, "Institutional (%)": None, "Float / Retail (%)": None})

        major_dict = dict(zip(major.iloc[:,0], major.iloc[:,1]))
        insider = major_dict.get("Insiders (%)", None)
        inst = major_dict.get("Institutions", None)

        def pct_to_float(val):
            if val is None:
                return None
            return float(str(val).strip('%'))

        insider_pct = pct_to_float(insider)
        inst_pct = pct_to_float(inst)
        float_pct = None
        if inst_pct is not None:
            float_pct = 100 - inst_pct if insider_pct is None else 100 - inst_pct - insider_pct

        return pd.Series({
            "Insider (%)": insider_pct,
            "Institutional (%)": inst_pct,
            "Float / Retail (%)": float_pct
        })
    except Exception as e:
        logging.warning(f"Failed to parse major holders for {ticker}: {e}")
        return pd.Series({"Insider (%)": None, "Institutional (%)": None, "Float / Retail (%)": None})

def clean_info(raw_df: pd.DataFrame) -> pd.DataFrame:
    fields = {
        "Ticker": "_ticker",
        "Name": "shortName",
        "Sector": "sector",
        "Industry": "industry",
        "Current Price": "currentPrice",
        "52W High": "fiftyTwoWeekHigh",
        "52W Low": "fiftyTwoWeekLow",
        "Market Cap": "marketCap",
        "P/E (Trailing)": "trailingPE",
        "P/E (Forward)": "forwardPE",
        "Beta": "beta",
        "Dividend Yield": "dividendYield",
        "Price to Book": "priceToBook",
        "EPS": "trailingEps",
        "Revenue": "totalRevenue",
        "Gross Margins": "grossMargins",
        "Profit Margins": "profitMargins",
        "Insider (%)": "Insider (%)",
        "Institutional (%)": "Institutional (%)",
        "Float / Retail (%)": "Float / Retail (%)",
    }

    df = pd.DataFrame()
    for col, raw_key in fields.items():
        df[col] = raw_df[raw_key] if raw_key in raw_df.columns else None

    # Apply formatting
    df["Market Cap"] = df["Market Cap"].apply(format_number)
    df["Revenue"] = df["Revenue"].apply(format_number)
    df["Dividend Yield"] = df["Dividend Yield"].apply(format_percent)
    df["Gross Margins"] = df["Gross Margins"].apply(format_percent)
    df["Profit Margins"] = df["Profit Margins"].apply(format_percent)

    return df

# =============================
# Main
# =============================
if __name__ == "__main__":
    # Load tickers
    try:
        tickers_df = pd.read_csv("idx_tickers.csv")
        tickers = tickers_df["Ticker"].dropna().astype(str).tolist()
    except FileNotFoundError:
        logging.warning("⚠️ No idx_tickers.csv found. Using hardcoded list.")
        tickers = ["BBCA.JK", "TLKM.JK", "BMRI.JK", "BBRI.JK"]

    logging.info(f"Fetching data for {len(tickers)} tickers...")

    raw_rows = []
    for i, t in enumerate(tickers, 1):
        logging.info(f"[{i}/{len(tickers)}] Fetching {t} ...")
        try:
            ticker_obj = yf.Ticker(t)
            info = ticker_obj.info
            info["_ticker"] = t

            # Add ownership summary
            ownership = parse_major_holders(t)
            info.update(ownership.to_dict())

            raw_rows.append(info)
        except Exception as e:
            logging.warning(f"Failed {t}: {e}")
            raw_rows.append({"_ticker": t})

        time.sleep(1.5)  # rate limit

    raw_df = pd.DataFrame(raw_rows)
    clean_df = clean_info(raw_df)

    # Save to Excel
    with pd.ExcelWriter("idx_yfinance_with_ownership_summary.xlsx", engine="openpyxl") as writer:
        raw_df.to_excel(writer, sheet_name="Raw_Info", index=False)
        clean_df.to_excel(writer, sheet_name="Cleaned_Info", index=False)

    # Also save CSV
    raw_df.to_csv("idx_yfinance_with_ownership_summary.csv", index=False)

    logging.info("✅ Done. Excel and CSV saved with ownership summary included.")
    print("✅ Done. Raw_Info and Cleaned_Info now include ownership columns.")