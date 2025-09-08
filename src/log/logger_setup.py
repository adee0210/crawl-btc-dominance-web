import logging
from logging.handlers import RotatingFileHandler
import os


class LoggerSetup:
    _handlers = None

    @staticmethod
    def logger_setup(name: str, log_file: str = "main.log", level: int = logging.INFO):
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_path = os.path.join(root_dir, log_file)

        # formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(processName)s - %(levelname)s - %(name)s - %(message)s"
        )

        # handlers
        file_handlers = RotatingFileHandler(
            filename=log_path, maxBytes=10 * 1024 * 1024, encoding="utf-8"
        )
        file_handlers.setFormatter(formatter)

        console_handlers = logging.StreamHandler()
        console_handlers.setFormatter(formatter)

        logger = logging.getLogger(name)
        handlers = [file_handlers, console_handlers]

        if not logger.handlers:
            for h in handlers:
                logger.addHandler(h)

        logger.propagate = False
        logger.setLevel(level=level)
        return logger
