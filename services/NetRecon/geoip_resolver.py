import json
import ipaddress
from pathlib import Path
import geoip2.database
import geoip2.errors

from config import *

# Base directory
BASE_DIR = Path(__file__).resolve().parent

# Database paths
GEOIP_CITY_DB = BASE_DIR / "data" / "GeoLite2-City.mmdb"
GEOIP_ASN_DB = BASE_DIR / "data" / "GeoLite2-ASN.mmdb"
COUNTRY_META_FILE = BASE_DIR / "data" / "country_meta.json"

# Load databases lazily once
city_reader = geoip2.database.Reader(str(GEOIP_CITY_DB))
asn_reader = geoip2.database.Reader(str(GEOIP_ASN_DB))

# Load optional country metadata
try:
	with COUNTRY_META_FILE.open("r", encoding="utf-8") as f:
		COUNTRY_META = json.load(f)
except FileNotFoundError:
	if DEBUG_MODE:
		print(f"[!] Country metadata file not found: {COUNTRY_META_FILE}")
	COUNTRY_META = {}  # Safe fallback

if DEBUG_MODE:
	print(f"[+] Loaded country metadata for {len(COUNTRY_META)} countries.")


def _lookup_connection(ip: str):
	"""Resolve ASN data (ISP, ASN, route) for an IP."""
	try:
		asn = asn_reader.asn(ip)
	except geoip2.errors.AddressNotFoundError:
		return None
	except Exception:
		return None

	return {
		"asn": asn.autonomous_system_number,
		"org": asn.autonomous_system_organization,
		"isp": asn.autonomous_system_organization,
		"route": str(asn.network),
	}

def lookup_ip(ip: str):
	"""Resolve an IP using GeoLite2 (City + ASN) + country metadata."""
	
	# Validate IP format
	try:
		ip_obj = ipaddress.ip_address(ip)
	except ValueError:
		if DEBUG_MODE:
			print(f"[!] Invalid IP address format: {ip}")
		return None, "invalid_ip"

	# Resolve CITY record
	try:
		city = city_reader.city(ip)
	except geoip2.errors.AddressNotFoundError:
		if DEBUG_MODE:
			print(f"[!] City not found for IP: {ip}")
		return None, "not_found"
	except Exception as e:
		if DEBUG_MODE:
			print(f"[!] City lookup error for IP {ip}: {e}")
		return None, f"lookup_error:{e}"

	continent = city.continent.names.get("en") if city.continent else None
	country = city.country.names.get("en") if city.country else None
	region = city.subdivisions.most_specific
	region_name = region.names.get("en") if region and region.names else None
	city_name = city.city.names.get("en") if city.city else None

	result = {
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
		"timezone": city.location.time_zone,
	}

	# Inject country metadata (if exists)
	cc = result["country_code"]
	if DEBUG_MODE:
		print(cc)
		print(COUNTRY_META.keys())
	if cc and cc in COUNTRY_META:
		meta = COUNTRY_META[cc]
		result["calling_code"] = meta.get("calling_code")
		result["capital"] = meta.get("capital")
		result["borders"] = meta.get("borders")

		flag_meta = meta.get("flag")
		if flag_meta:
			result["flag"] = {
				"emoji": flag_meta.get("emoji"),
				"svg": flag_meta.get("svg"),
				"png": flag_meta.get("png"),
			}

	# Resolve ASN connection details
	connection = _lookup_connection(ip)
	if connection:
		if DEBUG_MODE:
			print(f"[+] ASN lookup succeeded for IP: {ip}")
		result["connection"] = connection
	else:
		if DEBUG_MODE:
			print(f"[!] ASN lookup failed for IP: {ip}")

	return result, None

