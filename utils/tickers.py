import pandas as pd


def get_snp500_tickers():
    df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
    return df["Symbol"].to_list()


if __name__ == "__main__":
    tickers = get_snp500_tickers()
    print(tickers)