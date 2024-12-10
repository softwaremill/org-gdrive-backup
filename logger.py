from loguru import logger
import sys
import os

logger.remove(0)
logger.add(sys.stdout, level=os.getenv("LOG_LEVEL", "INFO").upper())

app_logger = logger