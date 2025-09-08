import os
import sys
import threading
import time
from datetime import datetime

from pymongo import WriteConcern

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import json
from websocket import WebSocketApp
from src.configs.config_variable import DATA_CRAWL_CONFIG, MONGO_CONFIG
from src.log.logger_setup import LoggerSetup
from src.configs.config_mongo import MongoDBConfig


import os
import sys
import threading
import time
import json
from datetime import datetime

from pymongo import WriteConcern

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from websocket import WebSocketApp
from src.configs.config_variable import DATA_CRAWL_CONFIG, MONGO_CONFIG
from src.log.logger_setup import LoggerSetup
from src.configs.config_mongo import MongoDBConfig


class ExtractBTCDominanceRealtime:
    def __init__(self):
        self.logger = LoggerSetup.logger_setup("Extract BTC Dominance Realtime")
        mongo_config = MongoDBConfig()
        self.url = DATA_CRAWL_CONFIG.get("url")
        self.mongo_client = mongo_config.get_client()
        self.collection = (
            self.mongo_client.get_database(DATA_CRAWL_CONFIG.get("db"))
            .get_collection(DATA_CRAWL_CONFIG.get("collection"))
            .with_options(write_concern=WriteConcern(w=1, j=False))
        )

        self.logger.info(
            f'Connect MongoDB with host: {MONGO_CONFIG.get("host")} and with port:{MONGO_CONFIG.get("port")} '
            f"Database name: {DATA_CRAWL_CONFIG.get('db')} and Collection name: {DATA_CRAWL_CONFIG.get('collection')}"
        )

        self.ws = None
        self.running = False

        # Batch for fast inserts of per-second records
        self.batch = []
        self.batch_lock = threading.Lock()
        self.batch_size = 20
        self.batch_timer = None
        # Flush frequently for near-real-time (seconds)
        self.batch_interval = 1

        # Buffer to keep per-second records for minute aggregation
        self.per_second_buffer = []
        self.last_minute = None

        # Separate batch for minute aggregations
        self.minute_batch = []

        # track last received data time for heartbeat
        self.last_data_time = time.time()

    def round_timestamp_to_minute(self, timestamp_ms):
        dt = datetime.fromtimestamp(timestamp_ms / 1000)
        rounded = dt.replace(second=0, microsecond=0)
        return int(rounded.timestamp() * 1000), rounded.strftime("%Y-%m-%d %H:%M:00")

    def timestamp_to_second_str(self, timestamp_ms):
        dt = datetime.fromtimestamp(timestamp_ms / 1000)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def aggregate_minute(self, records):
        if not records:
            return None
        opens = [r["open"] for r in records]
        highs = [r["high"] for r in records]
        lows = [r["low"] for r in records]
        closes = [r["close"] for r in records]
        volumes = [r["volume"] for r in records]
        minute_dt = records[0]["datetime"][:16] + ":00"
        # compute timestamp_ms for the minute (from minute_dt)
        try:
            minute_ts = int(
                datetime.strptime(minute_dt, "%Y-%m-%d %H:%M:%S").timestamp() * 1000
            )
        except Exception:
            minute_ts = None

        return {
            "open": float(opens[0]),
            "high": float(max(highs)),
            "low": float(min(lows)),
            "close": float(closes[-1]),
            "volume": float(sum(volumes)),
            "datetime": minute_dt,
            "timestamp_ms": minute_ts,
        }

    def insert_batch(self):
        with self.batch_lock:
            # insert per-second batch
            if self.batch:
                try:
                    result = self.collection.insert_many(self.batch, ordered=False)
                    self.logger.info(
                        f"Inserted batch of {len(result.inserted_ids)} per-second documents"
                    )
                    self.batch.clear()
                except Exception as e:
                    self.logger.error(f"Error inserting per-second batch: {str(e)}")
                    self.batch.clear()

            # insert minute aggregation batch
            if self.minute_batch:
                try:
                    result = self.collection.insert_many(
                        self.minute_batch, ordered=False
                    )
                    self.logger.info(
                        f"Inserted batch of {len(result.inserted_ids)} minute aggregation documents"
                    )
                    self.minute_batch.clear()
                except Exception as e:
                    self.logger.error(f"Error inserting minute batch: {str(e)}")
                    self.minute_batch.clear()

    def schedule_batch_insert(self):
        # schedule recurring flush using a timer
        def _run():
            if not self.running:
                return
            try:
                self.insert_batch()
            finally:
                # reschedule
                self.batch_timer = threading.Timer(self.batch_interval, _run)
                self.batch_timer.daemon = True
                self.batch_timer.start()

        # start the recurring timer
        self.batch_timer = threading.Timer(self.batch_interval, _run)
        self.batch_timer.daemon = True
        self.batch_timer.start()

    def on_open(self, ws):
        self.logger.info("Websocket connected opened")
        self.last_data_time = time.time()
        # start periodic flush
        self.schedule_batch_insert()
        # start heartbeat thread
        threading.Thread(target=self.ensure_data_flow, daemon=True).start()

    def ensure_data_flow(self):
        # If no real message received within a few seconds, push a lightweight heartbeat
        while self.running:
            try:
                time.sleep(1)
                now = time.time()
                if now - self.last_data_time > 3:
                    with self.batch_lock:
                        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        heartbeat = {
                            "open": 0.0,
                            "high": 0.0,
                            "low": 0.0,
                            "close": 0.0,
                            "volume": 0.0,
                            "datetime": now_str,
                            "is_heartbeat": True,
                        }
                        self.batch.append(heartbeat)
                        self.logger.debug(f"Added heartbeat record at {now_str}")
                        # update last_data_time so we don't spam
                        self.last_data_time = now
            except Exception as e:
                self.logger.error(f"Error in ensure_data_flow: {e}")

    def on_error(self, ws, error):
        self.logger.error(f"Websocket connected error with error: {error}")

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            if isinstance(data, dict) and "k" in data:
                k = data["k"]
                ts = int(k.get("T", k.get("t", int(time.time() * 1000))))
                second_str = self.timestamp_to_second_str(ts)

                ohlcv_data = {
                    "open": float(k.get("o", 0.0)),
                    "high": float(k.get("h", 0.0)),
                    "low": float(k.get("l", 0.0)),
                    "close": float(k.get("c", 0.0)),
                    "volume": float(k.get("v", 0.0)),
                    "datetime": second_str,
                }

                current_minute = second_str[:16]

                with self.batch_lock:
                    self.batch.append(ohlcv_data)
                    self.per_second_buffer.append(ohlcv_data)
                    self.last_data_time = time.time()

                    if self.last_minute is None:
                        self.last_minute = current_minute

                    if current_minute != self.last_minute:
                        # minute changed -> aggregate previous minute
                        try:
                            minute_agg = self.aggregate_minute(self.per_second_buffer)
                            if minute_agg:
                                self.minute_batch.append(minute_agg)
                                self.logger.debug(
                                    f"Queued minute aggregation for {minute_agg.get('datetime')}"
                                )
                        finally:
                            self.per_second_buffer = []
                            self.last_minute = current_minute

                    if len(self.batch) >= self.batch_size:
                        # flush immediately if batch size reached
                        self.insert_batch()

        except Exception as e:
            self.logger.error(f"Can't format data: {str(e)}")

    def on_close(self, ws, close_status, close_message):
        self.logger.info(f"Websocket closed: {close_status} - {close_message}")
        self.running = False
        if self.batch_timer:
            try:
                self.batch_timer.cancel()
            except Exception:
                pass
        self.insert_batch()

    def stop(self):
        self.running = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        if self.batch_timer:
            try:
                self.batch_timer.cancel()
            except Exception:
                pass
        self.insert_batch()

    def start(self):
        self.logger.info("Start extract BTC Dominance realtime ....")
        self.running = True
        try:
            self.ws = WebSocketApp(
                url=self.url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )
            # Blocking call - will return when closed
            self.ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            self.logger.error(f"Error in extract realtime: {str(e)}")
        finally:
            self.stop()


if __name__ == "__main__":
    extract_dominance = ExtractBTCDominanceRealtime()
    print("Starting realtime data extraction...")
    extract_dominance.start()

    def on_error(self, ws, error):
        self.logger.error(f"Websocket connected error with error: {error}")

    def on_message(self, ws, message):
        try:
            self.last_data_time = time.time()  # Cập nhật thời gian nhận dữ liệu cuối
            data = json.loads(message)
            if isinstance(data, dict) and "k" in data:
                data = data["k"]
                ts = int(data["T"])
                # per-second datetime string
                second_str = self.timestamp_to_second_str(ts)
                ohlcv_data = {
                    "open": float(data["o"]),
                    "high": float(data["h"]),
                    "low": float(data["l"]),
                    "close": float(data["c"]),
                    "volume": float(data["v"]),
                    "datetime": second_str,
                }

                current_minute = second_str[:16]  # YYYY-MM-DD HH:MM

                with self.batch_lock:
                    # append to batch for fast insertion
                    self.batch.append(ohlcv_data)
                    # append to per-second buffer for minute aggregation
                    self.per_second_buffer.append(ohlcv_data)

                    # If minute changed, flush per-second buffer and optionally insert minute aggregate
                    if self.last_minute is None:
                        self.last_minute = current_minute

                    if current_minute != self.last_minute:
                        # flush the batch (contains recent seconds) first
                        self.insert_batch()
                        # aggregate previous minute
                        try:
                            if len(self.per_second_buffer) >= 1:
                                # all records in buffer belong to previous minute; perform aggregation
                                minute_agg = self.aggregate_minute(
                                    self.per_second_buffer
                                )
                                if minute_agg:
                                    # Add to minute batch instead of inserting immediately
                                    self.minute_batch.append(minute_agg)
                                    self.logger.info(
                                        f"Added minute aggregation for {minute_agg.get('datetime')} to batch"
                                    )
                        finally:
                            # clear buffer and set last_minute to current
                            self.per_second_buffer = []
                            self.last_minute = current_minute

                    if len(self.batch) >= self.batch_size:
                        self.insert_batch()

        except Exception as e:
            self.logger.error(f"Can't format data: {str(e)}")

    def on_close(self, ws, close_status, close_message):
        self.logger.info(f"Websocket closed: {close_status} - {close_message}")
        self.running = False
        if self.batch_timer:
            self.batch_timer.cancel()
        self.insert_batch()

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()
        if self.batch_timer:
            self.batch_timer.cancel()
        self.insert_batch()

    def start(self):
        self.logger.info("Start extract BTC Dominance realtime ....")
        try:
            self.ws = WebSocketApp(
                url=self.url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )
            self.ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            self.logger.error(f"Error in extract: {str(e)}")
        finally:
            self.stop()


if __name__ == "__main__":
    extract_dominance = ExtractBTCDominanceRealtime()
    print("Starting realtime data extraction...")
    extract_dominance.start()
