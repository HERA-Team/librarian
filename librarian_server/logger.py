"""
Logging setup. Use this as 'from logger import log'
"""

import logging as log

from .settings import server_settings

logging_level = log.getLevelName(server_settings.log_level)

log.basicConfig(
    encoding="utf-8",
    level=logging_level,
    format="(%(module)s:%(funcName)s) [%(asctime)s] {%(levelname)s}:%(message)s",
)

log.debug("Logging set up.")