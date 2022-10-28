import logging
import sys
import logging.handlers

logger = logging.getLogger("overwatch")
logger.setLevel(level=logging.INFO)

log_formatter = logging.Formatter(
    fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

console_handler = logging.StreamHandler(stream=sys.stdout)
console_handler.setFormatter(log_formatter)
console_handler.setLevel(level=logging.INFO)
logger.addHandler(console_handler)

rfh = logging.handlers.RotatingFileHandler(
    filename="overwatch.log", maxBytes=5 * 1024 * 1024, backupCount=2
)
rfh.setFormatter(log_formatter)
rfh.setLevel(level=logging.INFO)
logger.addHandler(rfh)
