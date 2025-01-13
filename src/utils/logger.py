from loguru import logger
import sys
import os

logger.remove(0)
logger.add(
    sink=sys.stdout,
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    colorize=True,
)
app_logger = logger
