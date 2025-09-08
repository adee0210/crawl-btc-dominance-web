import os
import sys
import threading
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from extract.extract_dominance_realtime import ExtractBTCDominanceRealtime
from extract.extract_dominance_historical import ExtractBTCDominanceHistorical
from tele_bot.tele_message import TelegramMonitor
from log.logger_setup import LoggerSetup
from configs.config_variable import EXTRACT_CONFIG, TELEGRAM_CONFIG


class BTCDominanceMain:
    def __init__(self):
        self.logger = LoggerSetup.logger_setup("BTC Dominance Main")
        self.realtime_extractor = None
        self.historical_extractor = None
        self.telegram_monitor = None
        self.running = False
        self.realtime_thread = None
        self.historical_thread = None
        self.telegram_thread = None

    def start_realtime_thread(self):
        try:
            self.logger.info("Starting realtime data extraction...")
            self.realtime_extractor = ExtractBTCDominanceRealtime()
            self.realtime_extractor.start()
        except Exception as e:
            self.logger.error(f"Error in realtime extraction: {str(e)}")

    def start_historical_thread(self):
        try:
            self.historical_extractor = ExtractBTCDominanceHistorical()

            historical_days = EXTRACT_CONFIG.get("historical_days", 30)

            if historical_days == "all":
                self.logger.info("Starting full historical data extraction...")
                self.historical_extractor.get_all_historical_data()
            else:
                self.logger.info(
                    f"Starting historical data extraction for {historical_days} days..."
                )
                self.historical_extractor.get_recent_historical_data(
                    days=historical_days
                )

        except Exception as e:
            self.logger.error(f"Error in historical extraction: {str(e)}")

    def start_telegram_monitor(self):
        try:
            self.logger.info("Starting telegram monitor...")
            self.telegram_monitor = TelegramMonitor()
            self.telegram_monitor.start()
        except Exception as e:
            self.logger.error(f"Error in telegram monitor: {str(e)}")

    def run(self):
        self.logger.info("Starting BTC Dominance Main...")
        self.running = True

        realtime_enabled = EXTRACT_CONFIG.get("realtime_enabled", False)
        historical_enabled = EXTRACT_CONFIG.get("historical_enabled", False)
        telegram_enabled = TELEGRAM_CONFIG.get("monitor_enabled", False)
        run_parallel = EXTRACT_CONFIG.get("run_parallel", False)

        if not realtime_enabled and not historical_enabled:
            self.logger.warning("Both realtime and historical extraction are disabled")
            return

        try:
            if run_parallel:
                threads = []

                if realtime_enabled:
                    self.realtime_thread = threading.Thread(
                        target=self.start_realtime_thread
                    )
                    self.realtime_thread.daemon = True
                    self.realtime_thread.start()
                    threads.append(self.realtime_thread)
                    self.logger.info("Realtime extraction started in parallel")

                if historical_enabled:
                    self.historical_thread = threading.Thread(
                        target=self.start_historical_thread
                    )
                    self.historical_thread.daemon = True
                    self.historical_thread.start()
                    threads.append(self.historical_thread)
                    self.logger.info("Historical extraction started in parallel")

                if telegram_enabled:
                    self.telegram_thread = threading.Thread(
                        target=self.start_telegram_monitor
                    )
                    self.telegram_thread.daemon = True
                    self.telegram_thread.start()
                    threads.append(self.telegram_thread)
                    self.logger.info("Telegram monitor started in parallel")

                print("BTC Dominance extraction started. Press Ctrl+C to stop...")

                while self.running:
                    time.sleep(1)

            else:
                if historical_enabled:
                    self.start_historical_thread()

                if realtime_enabled:
                    self.start_realtime_thread()

                if telegram_enabled:
                    self.start_telegram_monitor()

        except KeyboardInterrupt:
            self.logger.info("Stopping BTC Dominance extraction...")
            self.stop()
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            self.stop()

    def stop(self):
        self.running = False

        if self.realtime_extractor:
            self.realtime_extractor.stop()

        if self.telegram_monitor:
            self.telegram_monitor.stop()

        self.logger.info("BTC Dominance extraction stopped")


if __name__ == "__main__":
    app = BTCDominanceMain()
    app.run()
