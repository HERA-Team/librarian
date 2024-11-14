"""
Logging setup. Use this as 'from logger import log'
"""

import loguru

from .settings import server_settings

log_settings = server_settings.log_settings

log_settings.setup_logs(server_settings.displayed_site_name)
loguru.logger.debug("Logging set up.")

log = loguru.logger
