# config.py

import os
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def _env_bool(name: str, default: bool = False) -> bool:
	"""Read a boolean value from environment variables."""
	raw = os.getenv(name)
	if raw is None:
		return default
	return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
	"""Read an integer value from environment variables."""
	raw = os.getenv(name)
	if raw is None:
		return default
	try:
		return int(raw)
	except ValueError:
		return default


@dataclass(frozen=True)
class Settings:
	"""Central application settings loaded from environment variables."""

	# App / Flask
	port: int = _env_int("NETRECON_PORT", 5000)
	flask_debug: bool = _env_bool("NETRECON_DEBUG", False)

	# Logging
	log_level: str = os.getenv("NETRECON_LOG_LEVEL", "INFO")

	# GeoIP database paths
	geoip_city_db: Path = Path(
		os.getenv(
			"NETRECON_GEOIP_CITY_DB",
			BASE_DIR / "data" / "GeoLite2-City.mmdb",
		)
	)
	geoip_asn_db: Path = Path(
		os.getenv(
			"NETRECON_GEOIP_ASN_DB",
			BASE_DIR / "data" / "GeoLite2-ASN.mmdb",
		)
	)
	country_meta_path: Path = Path(
		os.getenv(
			"NETRECON_COUNTRY_META_PATH",
			BASE_DIR / "data" / "country_meta.json",
		)
	)

	# Domain resolver config
	domain_resolution_enabled: bool = _env_bool(
		"NETRECON_DOMAIN_RESOLUTION_ENABLED", True
	)
	reverse_dns_enabled: bool = _env_bool(
		"NETRECON_REVERSE_DNS_ENABLED", True
	)
	peeringdb_scrape_enabled: bool = _env_bool(
		"NETRECON_PEERINGDB_SCRAPE_ENABLED", True
	)

	dns_timeout_seconds: float = float(
		os.getenv("NETRECON_DNS_TIMEOUT_SECONDS", "2.0")
	)
	http_timeout_seconds: float = float(
		os.getenv("NETRECON_HTTP_TIMEOUT_SECONDS", "5.0")
	)

	reverse_dns_cache_size: int = _env_int(
		"NETRECON_REVERSE_DNS_CACHE_SIZE", 4096
	)
	peeringdb_cache_size: int = _env_int(
		"NETRECON_PEERINGDB_CACHE_SIZE", 2048
	)

	# Redis configuration
	redis_url: str = os.getenv("NETRECON_REDIS_URL", "redis://localhost:6379/0")

	# Rate limiting
	rate_limit_enabled: bool = _env_bool("NETRECON_RATE_LIMIT_ENABLED", True)
	rate_limit_requests_per_window: int = _env_int(
		"NETRECON_RATE_LIMIT_REQUESTS", 10
	)  # default: 60 requests
	rate_limit_window_seconds: int = _env_int(
		"NETRECON_RATE_LIMIT_WINDOW_SECONDS", 60
	)  # default: per 60 seconds


settings = Settings()
