import os
import sys
import time
import threading
import requests
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.configs.config_mongo import MongoDBConfig
from src.configs.config_variable import DATA_CRAWL_CONFIG, TELEGRAM_CONFIG
from src.log.logger_setup import LoggerSetup


class TelegramMonitor:
    def __init__(self):
        self.logger = LoggerSetup.logger_setup("Telegram Monitor")

        mongo_config = MongoDBConfig()
        self.mongo_client = mongo_config.get_client()
        self.collection = self.mongo_client.get_database(
            DATA_CRAWL_CONFIG.get("db")
        ).get_collection(DATA_CRAWL_CONFIG.get("collection"))

        self.bot_token = TELEGRAM_CONFIG.get("bot_token")
        self.chat_id = TELEGRAM_CONFIG.get("chat_id")
        self.check_interval = TELEGRAM_CONFIG.get("check_interval", 30)
        self.data_timeout = TELEGRAM_CONFIG.get("data_timeout", 60)

        self.running = False
        self.last_alert_time = None
        self.alert_cooldown = 300

        self.logger.info(
            f"Telegram Monitor initialized with check interval: {self.check_interval}s"
        )

    def send_telegram_message(self, message):
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            data = {"chat_id": self.chat_id, "text": message, "parse_mode": "HTML"}

            response = requests.post(url, data=data, timeout=10)
            response.raise_for_status()

            self.logger.info("Telegram message sent successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to send telegram message: {str(e)}")
            return False

    def check_recent_data(self):
        try:
            current_time = datetime.utcnow()
            cutoff_time = current_time - timedelta(seconds=self.data_timeout)
            cutoff_ts_ms = int(cutoff_time.timestamp() * 1000)

            # Try fast path: count by timestamp_ms if present
            try:
                recent_count = self.collection.count_documents(
                    {"timestamp_ms": {"$gte": cutoff_ts_ms}}
                )
                if recent_count > 0:
                    self.logger.debug(
                        f"Found {recent_count} records by timestamp_ms in last {self.data_timeout}s"
                    )
                    return True
            except Exception:
                # ignore and fallback
                pass

            # Fallback: get the latest document and inspect its time fields
            latest = (
                self.collection.find_one(sort=[("timestamp_ms", -1)])
                or self.collection.find_one(sort=[("datetime", -1)])
                or self.collection.find_one(sort=[("_id", -1)])
            )

            if not latest:
                self.logger.debug("No documents found in collection at all")
                return False

            # If timestamp_ms exists, compare directly
            if "timestamp_ms" in latest:
                ts = int(latest["timestamp_ms"])
                delta = (int(datetime.utcnow().timestamp() * 1000) - ts) / 1000.0
                self.logger.debug(f"Latest timestamp_ms: {ts} ({delta:.1f}s ago)")
                return ts >= cutoff_ts_ms

            # If datetime string exists, try parse (common format)
            if "datetime" in latest and isinstance(latest["datetime"], str):
                try:
                    dt = datetime.strptime(latest["datetime"], "%Y-%m-%d %H:%M:%S")
                except Exception:
                    try:
                        dt = datetime.fromisoformat(latest["datetime"])
                    except Exception:
                        self.logger.debug(
                            "Latest document has datetime but cannot parse"
                        )
                        return False
                delta = (datetime.utcnow() - dt).total_seconds()
                self.logger.debug(f"Latest datetime field: {dt} ({delta:.1f}s ago)")
                return delta <= self.data_timeout

            # As last resort, treat as no recent data
            self.logger.debug("Latest document has no recognizable timestamp field")
            return False

        except Exception as e:
            self.logger.error(f"Error checking recent data: {str(e)}")
            return False

    def should_send_alert(self):
        if self.last_alert_time is None:
            return True

        time_since_last_alert = time.time() - self.last_alert_time
        return time_since_last_alert >= self.alert_cooldown

    def format_alert_message(self):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"""
<b>BTC Dominance Data Alert</b>

Time: {current_time}
No data received in the last {self.data_timeout} seconds

Database: {DATA_CRAWL_CONFIG.get("db")}
Collection: {DATA_CRAWL_CONFIG.get("collection")}

Please check the data extraction process!
        """
        return message.strip()

    def format_recovery_message(self):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"""
<b>BTC Dominance Data Recovery</b>

Time: {current_time}
Data is now flowing normally

System has recovered successfully!
        """
        return message.strip()

    def monitor_loop(self):
        self.logger.info("Starting data monitoring...")
        data_was_missing = False

        while self.running:
            try:
                has_recent_data = self.check_recent_data()

                if not has_recent_data:
                    if not data_was_missing and self.should_send_alert():
                        alert_message = self.format_alert_message()
                        if self.send_telegram_message(alert_message):
                            self.last_alert_time = time.time()
                        data_was_missing = True

                    self.logger.warning(
                        f"No recent data found in last {self.data_timeout} seconds"
                    )

                else:
                    if data_was_missing:
                        recovery_message = self.format_recovery_message()
                        self.send_telegram_message(recovery_message)
                        data_was_missing = False
                        self.logger.info("Data flow recovered")

                time.sleep(self.check_interval)

            except Exception as e:
                self.logger.error(f"Error in monitor loop: {str(e)}")
                time.sleep(self.check_interval)

    def start(self):
        if not self.bot_token or not self.chat_id:
            self.logger.error("Telegram bot token or chat ID not configured")
            return False

        self.running = True
        self.monitor_thread = threading.Thread(target=self.monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

        self.logger.info("Telegram monitor started")
        return True

    def stop(self):
        self.running = False
        self.logger.info("Telegram monitor stopped")

    def test_connection(self):
        test_message = f"Test message from BTC Dominance Monitor\nTime: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        return self.send_telegram_message(test_message)


if __name__ == "__main__":
    monitor = TelegramMonitor()

    print("Testing Telegram connection...")
    if monitor.test_connection():
        print("Telegram connection successful!")
        monitor.start()

        try:
            print("Monitor running... Press Ctrl+C to stop")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopping monitor...")
            monitor.stop()
    else:
        print("Telegram connection failed!")

# End of file
