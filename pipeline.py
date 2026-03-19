import csv
import datetime
from binance.client import Client


class KlineProcessor:
    def __str__(self):
        return f"<KlineProcessor symbol='{self._symbol}' count='{len(self._klines)}'>"

    def __init__(self, symbol:str, headers: list, klines: list):
        self._headers = headers
        self._klines = klines
        self._symbol = symbol

    def to_dict_list(self):
        return [dict(zip(self._headers, k)) for k in self._klines]

    def to_csv(self, file_path: str) -> str:
        import os
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(self._headers)
            writer.writerows(self._klines)
        return file_path

    def to_list(self):
        return self._klines

    def to_dataframe(self):
        import pandas as pd
        df = pd.DataFrame(self._klines, columns=self._headers)

        # Convert types immediately
        numeric_cols = ["Open", "High", "Low", "Close", "Volume",
                        "QuoteAssetVolume", "TakerBuyBaseVolume",
                        "TakerBuyQuoteVolume"]
        df[numeric_cols] = df[numeric_cols].astype(float)

        # Convert timestamps
        df["OpenTime"] = pd.to_datetime(df["OpenTime"], unit="ms")
        df["CloseTime"] = pd.to_datetime(df["CloseTime"], unit="ms")

        return df


class Pipeline:
    def __init__(self, symbol: str, interval: str):
        self._symbol = symbol
        self._interval = interval
        self._client = None
        self._headers = [
            "OpenTime", "Open", "High", "Low", "Close", "Volume",
            "CloseTime", "QuoteAssetVolume", "NumberOfTrades",
            "TakerBuyBaseVolume", "TakerBuyQuoteVolume", "Ignore"
        ]

    def __enter__(self):
        self._client = Client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            self._client.close_connection()
        return False

    def _get_klines(self, start: str, end: str):
        try:
            return self._client.get_historical_klines(
                self._symbol, self._interval,
                start_str=start, end_str=end
            )
        except Exception as e:
            raise RuntimeError(f"Failed to fetch klines for {self._symbol}: {e}")

    def get_data(self, days: int) -> KlineProcessor:
        if days <= 0:
            raise ValueError(f"days must be positive, got {days}")
        end = datetime.date.today()
        start = end - datetime.timedelta(days=days)
        klines = self._get_klines(str(start), str(end))
        return KlineProcessor(self._symbol, self._headers, klines)

    def get_last_5_years(self) -> KlineProcessor:
        return self.get_data(365 * 5)

    def get_last_month(self) -> KlineProcessor:
        return self.get_data(30)

    def get_last_week(self) -> KlineProcessor:
        return self.get_data(7)


with Pipeline("BTCUSDT","1h") as pipeline:
    file_path = f"./dataset/{datetime.date.today()}.csv"
    print(pipeline.get_last_5_years().to_csv(file_path))