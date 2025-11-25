import logging
import sys
from config import settings


def setup_logging() -> None:
	"""Configure root logger for the application."""
	level_name = getattr(settings, "log_level", "INFO")
	level = getattr(logging, level_name.upper(), logging.INFO)

	# Basic configuration for root logger
	logging.basicConfig(
		level=level,
		format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
		stream=sys.stdout,
	)

	# Optionally tune noisy loggers here
	logging.getLogger("urllib3").setLevel(logging.WARNING)
	logging.getLogger("requests").setLevel(logging.WARNING)
