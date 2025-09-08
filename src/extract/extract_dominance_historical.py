import os
import sys
import time
import requests
import gc
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pymongo import WriteConcern

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.configs.config_variable import DATA_CRAWL_CONFIG, MONGO_CONFIG
from src.log.logger_setup import LoggerSetup
from src.configs.config_mongo import MongoDBConfig


class ExtractBTCDominanceHistorical:
    def __init__(self):
        self.logger = LoggerSetup.logger_setup("Extract BTC Dominance Historical")
        mongo_config = MongoDBConfig()
        self.mongo_client = mongo_config.get_client()
        self.collection = (
            self.mongo_client.get_database(DATA_CRAWL_CONFIG.get("db"))
            .get_collection(DATA_CRAWL_CONFIG.get("collection"))
            .with_options(write_concern=WriteConcern(w=1, j=False))
        )

        self.logger.info(
            f'Connect MongoDB with host: {MONGO_CONFIG.get("host")} and with port:{MONGO_CONFIG.get("port")} '
            f"Database name: {DATA_CRAWL_CONFIG.get("db")} and Collection name: {DATA_CRAWL_CONFIG.get("collection")}"
        )

        self.batch_size = 500
        self.request_delay = 0.2

        # requests session with retries
        self.session = requests.Session()
        retries = Retry(
            total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504]
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def round_timestamp_to_minute(self, timestamp_ms):
        # Round down to minute (remove seconds)
        dt = datetime.fromtimestamp(timestamp_ms / 1000)
        rounded_dt = dt.replace(second=0, microsecond=0)
        return int(rounded_dt.timestamp() * 1000), rounded_dt.strftime(
            "%Y-%m-%d %H:%M:00"
        )

    def insert_batch_with_memory_control(self, batch_data):
        try:
            if batch_data:
                result = self.collection.insert_many(batch_data, ordered=False)
                self.logger.info(f"Inserted {len(result.inserted_ids)} documents")
                del batch_data
                gc.collect()
                return len(result.inserted_ids)
        except Exception as e:
            self.logger.error(f"Error inserting batch: {str(e)}")
            del batch_data
            gc.collect()
        return 0

    def get_all_historical_data(self, symbol="BTCUSDT", interval="1m"):
        limit = 1000
        end_time = None
        total_inserted = 0
        batch_data = []

        self.logger.info("Starting full historical data extraction...")

        while True:
            url = "https://api.binance.com/api/v3/klines"
            params = {"symbol": symbol, "interval": interval, "limit": limit}

            if end_time:
                params["endTime"] = end_time

            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                klines = response.json()

                if not klines or len(klines) == 0:
                    break

                for kline in klines:
                    timestamp_ms, datetime_str = self.round_timestamp_to_minute(
                        int(kline[6])
                    )
                    ohlcv_data = {
                        "open": float(kline[1]),
                        "high": float(kline[2]),
                        "low": float(kline[3]),
                        "close": float(kline[4]),
                        "volume": float(kline[5]),
                        "datetime": datetime_str,
                        "timestamp_ms": timestamp_ms,
                    }
                    batch_data.append(ohlcv_data)

                    if len(batch_data) >= self.batch_size:
                        inserted_count = self.insert_batch_with_memory_control(
                            batch_data
                        )
                        total_inserted += inserted_count
                        batch_data = []

                end_time = klines[0][0] - 1
                time.sleep(self.request_delay)

                if total_inserted % 10000 == 0 and total_inserted > 0:
                    self.logger.info(f"Processed {total_inserted} records...")

            except Exception as e:
                self.logger.error(f"Error fetching historical data: {str(e)}")
                break

        if batch_data:
            inserted_count = self.insert_batch_with_memory_control(batch_data)
            total_inserted += inserted_count

        self.logger.info(f"Completed! Total {total_inserted} records")

    def get_recent_historical_data(self, symbol="BTCUSDT", interval="1m", days=30):
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)

        limit = 1000
        current_end_time = end_time
        total_inserted = 0
        batch_data = []

        self.logger.info(f"Starting data extraction for {days} days...")

        while current_end_time > start_time:
            url = "https://api.binance.com/api/v3/klines"
            params = {
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
                "endTime": current_end_time,
                "startTime": start_time,
            }

            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                klines = response.json()

                if not klines or len(klines) == 0:
                    break

                for kline in klines:
                    if int(kline[0]) < start_time:
                        break
                    timestamp_ms, datetime_str = self.round_timestamp_to_minute(
                        int(kline[6])
                    )
                    ohlcv_data = {
                        "open": float(kline[1]),
                        "high": float(kline[2]),
                        "low": float(kline[3]),
                        "close": float(kline[4]),
                        "volume": float(kline[5]),
                        "datetime": datetime_str,
                        "timestamp_ms": timestamp_ms,
                    }
                    batch_data.append(ohlcv_data)

                    if len(batch_data) >= self.batch_size:
                        inserted_count = self.insert_batch_with_memory_control(
                            batch_data
                        )
                        total_inserted += inserted_count
                        batch_data = []

                current_end_time = klines[0][0] - 1
                time.sleep(self.request_delay)

            except Exception as e:
                self.logger.error(f"Error fetching historical data: {str(e)}")
                break

        if batch_data:
            inserted_count = self.insert_batch_with_memory_control(batch_data)
            total_inserted += inserted_count

        self.logger.info(
            f"Completed! Loaded {days} days - Total {total_inserted} records"
        )

    def get_latest_data(self, symbol="BTCUSDT", interval="1m", limit=1000):
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}

        self.logger.info(f"Starting extraction of {limit} latest records...")

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            klines = response.json()

            batch_data = []
            for kline in klines:
                timestamp_ms, datetime_str = self.round_timestamp_to_minute(
                    int(kline[6])
                )
                ohlcv_data = {
                    "open": float(kline[1]),
                    "high": float(kline[2]),
                    "low": float(kline[3]),
                    "close": float(kline[4]),
                    "volume": float(kline[5]),
                    "datetime": datetime_str,
                    "timestamp_ms": timestamp_ms,
                }
                batch_data.append(ohlcv_data)

            if batch_data:
                inserted_count = self.insert_batch_with_memory_control(batch_data)
                self.logger.info(f"Completed! Total {inserted_count} records")

        except Exception as e:
            self.logger.error(f"Error fetching latest data: {str(e)}")

    def get_historical_trades_1s(
        self, symbol="BTCUSDT", start_time=None, end_time=None
    ):
        """Backfill per-second OHLCV by aggregating aggTrades into 1-second buckets.

        This iterates over aggTrades and groups them by second (timestamp // 1000).
        """
        url = "https://api.binance.com/api/v3/aggTrades"
        limit = 1000
        params = {"symbol": symbol, "limit": limit}
        if start_time:
            params["startTime"] = int(start_time)
        if end_time:
            params["endTime"] = int(end_time)

        batch = []
        total = 0
        last_second = None
        bucket = []

        def flush_bucket(sec, trades):
            if not trades:
                return None
            prices = [t[4] for t in trades]
            qtys = [t[5] for t in trades]
            open_p = prices[0]
            close_p = prices[-1]
            high_p = max(prices)
            low_p = min(prices)
            volume = sum(qtys)
            timestamp_ms = sec * 1000
            dt = datetime.fromtimestamp(sec).strftime("%Y-%m-%d %H:%M:%S")
            return {
                "open": float(open_p),
                "high": float(high_p),
                "low": float(low_p),
                "close": float(close_p),
                "volume": float(volume),
                "datetime": dt,
            }

        while True:
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                trades = response.json()
                if not trades:
                    break

                for t in trades:
                    # aggTrades structure: [a, p, q, f, l, T, m] or dict depending on endpoint
                    # For safety, accept both dict and list
                    if isinstance(t, dict):
                        trade_time = int(t.get("T"))
                        price = float(t.get("p"))
                        qty = float(t.get("q"))
                    else:
                        # array: [aggId, price, qty, firstId, lastId, timestamp, wasBuyerMaker]
                        trade_time = int(t[5])
                        price = float(t[1])
                        qty = float(t[2])

                    sec = trade_time // 1000
                    if last_second is None:
                        last_second = sec

                    if sec != last_second:
                        o = flush_bucket(last_second, bucket)
                        if o:
                            batch.append(o)
                            if len(batch) >= self.batch_size:
                                inserted = self.insert_batch_with_memory_control(batch)
                                total += inserted
                                batch = []
                        bucket = []
                        last_second = sec

                    bucket.append((None, price, qty, None, price, qty, trade_time))

                # advance params to next window
                # use last trade time as endTime - 1
                last_trade_time = (
                    int(trades[-1][5])
                    if isinstance(trades[-1], list)
                    else int(trades[-1].get("T"))
                )
                params["startTime"] = last_trade_time + 1
                time.sleep(0.2)

            except Exception as e:
                self.logger.error(f"Error fetching aggTrades: {str(e)}")
                break

        # flush remaining bucket
        o = flush_bucket(last_second, bucket)
        if o:
            batch.append(o)

        if batch:
            inserted = self.insert_batch_with_memory_control(batch)
            total += inserted

        self.logger.info(f"Completed aggTrades -> 1s OHLCV. Inserted: {total}")


if __name__ == "__main__":
    extract_dominance = ExtractBTCDominanceHistorical()

    mode = "recent"
    days = 30

    if mode == "all":
        print("WARNING: This will download ALL historical data from 2017!")
        print("Có thể mất vài giờ và tạo hàng triệu records.")
        confirm = input("Bạn có chắc chắn? (yes/no): ")
        if confirm.lower() == "yes":
            extract_dominance.get_all_historical_data()
        else:
            print("Cancelled.")
    elif mode == "recent":
        extract_dominance.get_recent_historical_data(days=days)
    elif mode == "latest":
        extract_dominance.get_latest_data()
