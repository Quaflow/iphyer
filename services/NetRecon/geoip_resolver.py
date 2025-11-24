import json
import ipaddress
import socket
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from domain_resolver import resolve_domain_for_ip


from config import settings
import geoip2.database
import geoip2.errors

# Base directory
BASE_DIR = Path(__file__).resolve().parent

# Database paths
GEOIP_CITY_DB = settings.geoip_city_db
GEOIP_ASN_DB = settings.geoip_asn_db
COUNTRY_META_FILE = settings.country_meta_path

# Load databases lazily once
city_reader = geoip2.database.Reader(str(GEOIP_CITY_DB))
asn_reader = geoip2.database.Reader(str(GEOIP_ASN_DB))

# Load optional country metadata
try:
	with COUNTRY_META_FILE.open("r", encoding="utf-8") as f:
		COUNTRY_META = json.load(f)
except FileNotFoundError:
	print(f"[!] Country metadata file not found: {COUNTRY_META_FILE}")
	COUNTRY_META = {}
print(f"[+] Loaded country metadata for {len(COUNTRY_META)} countries.")


def _lookup_connection(ip: str) -> dict | None:
	"""Resolve ASN data (ISP, ASN, route, domain) for an IP."""
	try:
		asn = asn_reader.asn(ip)
	except geoip2.errors.AddressNotFoundError:
		return None
	except Exception as e:
		print(f"[!] ASN lookup error for IP {ip}: {e}")
		return None

	asn_number = asn.autonomous_system_number

	# Use shared domain resolver module
	domain = resolve_domain_for_ip(ip, asn_number)

	connection: dict = {
		"asn": asn_number,
		"org": asn.autonomous_system_organization,
		"isp": asn.autonomous_system_organization,
		"route": str(asn.network),
	}

	if domain:
		connection["domain"] = domain

	return connection

def _reverse_dns(ip: str) -> str | None:
	"""Resolve a best-effort domain name for an IP using reverse DNS (PTR)."""
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

	# Convert full hostname to a bare domain (e.g. x.y.google.com -> google.com)
	parts = hostname.split(".")
	if len(parts) >= 2:
		return ".".join(parts[-2:]).lower()

	return hostname.lower()

def _lookup_domain(ip: str, asn_number: int | None = None) -> str | None:
	"""Resolve domain using reverse DNS first, then PeeringDB website as fallback."""
	# 1) Try reverse DNS
	domain = _reverse_dns(ip)
	if domain:
		return domain

	# 2) Try PeeringDB website if ASN is known
	if asn_number is not None:
		#Calculate time taken to fetch PeeringDB website
		if DEBUG_MODE:
			start_time = datetime.now()
		website_url = _fetch_peeringdb_website_html(asn_number)
		if DEBUG_MODE:
			end_time = datetime.now()
			elapsed_time = (end_time - start_time).total_seconds()
			print(f"[+] PeeringDB website fetch for ASN {asn_number} took {elapsed_time:.2f} seconds.")
		if website_url:
			normalized = _normalize_domain(website_url)
			if normalized:
				return normalized

	return None


def _build_timezone_info(tz_name: str | None) -> dict | None:
	"""Build a rich timezone object similar to ipwhois.io."""
	if not tz_name:
		return None

	try:
		tz = ZoneInfo(tz_name)
	except Exception as e:
		print(f"[!] Failed to load timezone {tz_name}: {e}")
		return {"id": tz_name}

	now = datetime.now(tz)

	offset_td: timedelta | None = now.utcoffset()
	if offset_td is None:
		offset_td = timedelta(0)

	offset_seconds = int(offset_td.total_seconds())
	is_dst = bool(now.dst() and now.dst().total_seconds() != 0)

	# Offset formatted as +HH:MM or -HH:MM
	sign = "+" if offset_seconds >= 0 else "-"
	abs_total = abs(offset_seconds)
	hours = abs_total // 3600
	minutes = (abs_total % 3600) // 60
	utc_str = f"{sign}{hours:02d}:{minutes:02d}"

	return {
		"id": tz_name,
		"abbr": now.tzname(),
		"is_dst": is_dst,
		"offset": offset_seconds,
		"utc": utc_str,
		"current_time": now.isoformat(),
	}


def _country_code_to_emoji(cc: str | None) -> str | None:
	"""Convert ISO2 country code to flag emoji."""
	if not cc or len(cc) != 2:
		return None

	try:
		base = 0x1F1E6
		first = base + (ord(cc[0].upper()) - ord("A"))
		second = base + (ord(cc[1].upper()) - ord("A"))
		return chr(first) + chr(second)
	except Exception:
		return None


def _emoji_to_unicode_codes(emoji: str | None) -> str | None:
	"""Convert emoji string to unicode code representation (e.g. U+1F1E7 U+1F1EA)."""
	if not emoji:
		return None
	return " ".join(f"U+{ord(ch):04X}" for ch in emoji)


def lookup_ip(ip: str):
	"""Resolve an IP using GeoLite2 (City + ASN) + country metadata."""
	try:
		ip_obj = ipaddress.ip_address(ip)
	except ValueError:
		print(f"[!] Invalid IP address format: {ip}")
		return None, "invalid_ip"

	try:
		city = city_reader.city(ip)
	except geoip2.errors.AddressNotFoundError:
		print(f"[!] City not found for IP: {ip}")
		return None, "not_found"
	except Exception as e:
		print(f"[!] City lookup error for IP {ip}: {e}")
		return None, f"lookup_error:{e}"

	continent = city.continent.names.get("en") if city.continent else None
	country = city.country.names.get("en") if city.country else None
	region = city.subdivisions.most_specific
	region_name = region.names.get("en") if region and region.names else None
	city_name = city.city.names.get("en") if city.city else None

	timezone_id = city.location.time_zone
	timezone_info = _build_timezone_info(timezone_id)

	result: dict = {
		"ip": ip,
		"success": True,
		"type": "ipv4" if isinstance(ip_obj, ipaddress.IPv4Address) else "ipv6",
		"continent": continent,
		"continent_code": city.continent.code if city.continent else None,
		"country": country,
		"country_code": city.country.iso_code if city.country else None,
		"region": region_name,
		"region_code": region.iso_code if region else None,
		"city": city_name,
		"latitude": city.location.latitude,
		"longitude": city.location.longitude,
		"is_eu": getattr(city.country, "is_in_european_union", None),
		"postal": city.postal.code if city.postal else None,
		"calling_code": None,
		"capital": None,
		"borders": None,
		"flag": None,
		"connection": None,
		"timezone": timezone_info,
	}

	# Inject country metadata (if exists)
	cc = result["country_code"]
	if cc and cc in COUNTRY_META:
		meta = COUNTRY_META[cc]
		result["calling_code"] = meta.get("calling_code")
		result["capital"] = meta.get("capital")
		result["borders"] = meta.get("borders")

		# Flag: svg + emoji + emoji_unicode
		flag_meta = meta.get("flag") or {}
		svg = flag_meta.get("svg")
		emoji = flag_meta.get("emoji") or _country_code_to_emoji(cc)
		emoji_unicode = flag_meta.get("emoji_unicode") or _emoji_to_unicode_codes(emoji)

		result["flag"] = {
			"svg": svg,
			"emoji": emoji,
			"emoji_unicode": emoji_unicode,
		}

	# Resolve ASN connection details
	connection = _lookup_connection(ip)
	if connection:
		print(f"[+] ASN lookup succeeded for IP: {ip}")
		result["connection"] = connection
	else:
		print(f"[!] ASN lookup failed for IP: {ip}")

	return result, None
