import json
from pathlib import Path

import requests


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_PATH = BASE_DIR / "data" / "country_meta.json"
RESTCOUNTRIES_URL = "https://restcountries.com/v3.1/all"


def fetch_countries():
	"""Fetch raw country data from RestCountries API."""
	resp = requests.get(
		RESTCOUNTRIES_URL,
		params={
			"fields": "cca2,name,capital,idd,borders",  # limit payload
		},
		timeout=15,
	)
	resp.raise_for_status()
	return resp.json()


def build_country_meta(raw_countries):
	"""Build country metadata dict keyed by ISO 3166-1 alpha-2 (cca2)."""
	meta = {}

	for c in raw_countries:
		code = c.get("cca2")
		if not code:
			continue

		code = code.upper()

		# Name
		name_data = c.get("name") or {}
		name = name_data.get("common") or name_data.get("official") or code

		# Capital: usually a list; take the first if present
		capitals = c.get("capital") or []
		capital = capitals[0] if capitals else None

		# Calling code: RestCountries v3 idd: { root: "+1", suffixes: ["340", ...] }
		idd = c.get("idd") or {}
		root = idd.get("root")
		suffixes = idd.get("suffixes") or []
		if root and suffixes:
			# take the first suffix to create a default calling code like "+90"
			calling_code = f"{root}{suffixes[0]}"
		elif root:
			calling_code = root
		else:
			calling_code = None

		# Borders: already ISO 3166-1 alpha-3; you can keep as-is,
		# or later map to alpha-2 if you want.
		borders = c.get("borders") or []

		# Flag URLs: based on ISO alpha-2; always lowercased
		cc_lower = code.lower()
		flag = {
			"emoji": None,  # can be filled later if needed
			"svg": f"https://flagcdn.com/{cc_lower}.svg",
			"png": f"https://flagcdn.com/w320/{cc_lower}.png",
		}

		meta[code] = {
			"name": name,
			"calling_code": calling_code,
			"capital": capital,
			"borders": borders,
			"flag": flag,
		}

	return meta


def main():
	"""Fetch, build and write country metadata file."""
	print("[*] Fetching country data from RestCountries...")
	countries = fetch_countries()
	print(f"[+] Received {len(countries)} raw country entries.")

	meta = build_country_meta(countries)
	print(f"[+] Built metadata for {len(meta)} countries.")

	OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

	with OUTPUT_PATH.open("w", encoding="utf-8") as f:
		json.dump(meta, f, ensure_ascii=False, indent=2)

	print(f"[+] Written country_meta.json to: {OUTPUT_PATH}")


if __name__ == "__main__":
	main()
