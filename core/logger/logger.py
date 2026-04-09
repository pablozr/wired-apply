import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(funcName)s() | %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
