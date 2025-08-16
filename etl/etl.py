import pandas as pd, os, json, datetime as dt

# Output path inside the repo (served by GitHub Pages)
OUT_PATH = os.path.join("docs", "data", "dataset.json")

# Example public dataset (replace later with your own source)
SRC = "https://raw.githubusercontent.com/vega/vega-datasets/master/data/stocks.csv"

def run():
    df = pd.read_csv(SRC)
    # Make a tiny demo metric: average price per month for a single symbol
    df["date"] = pd.to_datetime(df["date"])
    sym = "AAPL"
    dfx = df[df["symbol"] == sym].copy()
    dfx = dfx.set_index("date").resample("7D")["price"].mean().dropna().reset_index()
    dfx["date"] = dfx["date"].dt.strftime("%Y-%m-%d")
    dfx = dfx.tail(30)  # keep recent points small
    rows = [{"date": r["date"], "value": float(r["price"])} for _, r in dfx.iterrows()]

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(rows)} points to {OUT_PATH}")

if __name__ == "__main__":
    run()
