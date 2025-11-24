import ipaddress
import socket
from functools import lru_cache
from urllib.parse import urlparse

from config import settings

import requests
from bs4 import BeautifulSoup

# Domain resolution configuration
DOMAIN_RESOLUTION_ENABLED = settings.domain_resolution_enabled
REVERSE_DNS_ENABLED = settings.reverse_dns_enabled
PEERINGDB_SCRAPE_ENABLED = settings.peeringdb_scrape_enabled

# Timeouts
DEFAULT_DNS_TIMEOUT = settings.dns_timeout_seconds  # seconds
DEFAULT_HTTP_TIMEOUT = settings.http_timeout_seconds  # seconds

# Cache sizes from settings
REVERSE_DNS_CACHE_SIZE = settings.reverse_dns_cache_size
PEERINGDB_CACHE_SIZE = settings.peeringdb_cache_size

# Set a global default timeout for socket operations
socket.setdefaulttimeout(DEFAULT_DNS_TIMEOUT)


def _normalize_domain(value: str | None) -> str | None:
	"""Normalize a URL or hostname to a bare domain (e.g. https://www.ovhcloud.com -> ovhcloud.com)."""
	if not value:
		return None

	value = value.strip()
	if not value:
		return None

	# If there is no scheme, prepend one so urlparse can handle it
	if "://" not in value:
		value = "http://" + value

	parsed = urlparse(value)
	host = parsed.hostname or parsed.path
	if not host:
		return None

	# Strip common "www." prefix
	if host.startswith("www."):
		host = host[4:]

	return host.lower()


@lru_cache(maxsize=REVERSE_DNS_CACHE_SIZE)
def _reverse_dns_cached(ip: str) -> str | None:
	"""Cached reverse DNS resolver for IP -> domain."""
	try:
		hostname, aliases, _ = socket.gethostbyaddr(ip)
	except (socket.herror, socket.gaierror):
		# No PTR record or DNS failure
		return None
	except Exception as e:
		print(f"[!] Reverse DNS error for IP {ip}: {e}")
		return None

	if not hostname:
		return None

	parts = hostname.split(".")
	if len(parts) >= 2:
		return ".".join(parts[-2:]).lower()

	return hostname.lower()


@lru_cache(maxsize=PEERINGDB_CACHE_SIZE)
def _fetch_peeringdb_website_html_cached(asn: int) -> str | None:
	"""Cached PeeringDB website extraction for an ASN using HTML scraping."""
	url = f"https://www.peeringdb.com/asn/{asn}"
	try:
		resp = requests.get(url, timeout=DEFAULT_HTTP_TIMEOUT)
	except Exception as e:
		print(f"[!] PeeringDB request failed for ASN {asn}: {e}")
		return None

	if resp.status_code != 200:
		print(f"[-] PeeringDB returned status {resp.status_code} for ASN {asn}")
		return None

	try:
		soup = BeautifulSoup(resp.text, "html.parser")
	except Exception as e:
		print(f"[!] Failed to parse PeeringDB HTML for ASN {asn}: {e}")
		return None

	# This selector is based on current PeeringDB markup and may break if the site changes.
	div = soup.find(
		"div",
		{
			"class": "view_value col-8 col-sm-7 col-md-8",
			"data-edit-name": "website",
		},
	)
	if not div:
		return None

	a = div.find("a")
	href = None
	if a:
		href = a.get("href") or a.get_text(strip=True)
	else:
		href = div.get_text(strip=True)

	return href or None


def resolve_domain_for_ip(ip: str, asn_number: int | None = None) -> str | None:
	"""Resolve a best-effort domain for an IP using multiple strategies.

	Priority:
		1. Skip private/loopback/reserved IPs entirely
		2. Reverse DNS (PTR) with caching
		3. PeeringDB website for ASN with caching
	"""
	if not DOMAIN_RESOLUTION_ENABLED:
		return None

	# Skip internal/non-routable IPs to avoid useless lookups
	try:
		ip_obj = ipaddress.ip_address(ip)
		if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_reserved or ip_obj.is_link_local:
			return None
	except ValueError:
		# If IP is somehow invalid, bail out
		return None

	# 1) Reverse DNS
	if REVERSE_DNS_ENABLED:
		rdns_domain = _reverse_dns_cached(ip)
		if rdns_domain:
			return rdns_domain

	# 2) PeeringDB HTML website scrape
	if PEERINGDB_SCRAPE_ENABLED and asn_number is not None:
		website_url = _fetch_peeringdb_website_html_cached(asn_number)
		if website_url:
			normalized = _normalize_domain(website_url)
			if normalized:
				return normalized

	return None
