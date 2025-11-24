# IP Scraper (GeoLite2-based)

This project is a simple IP information API built with Flask and MaxMind's GeoLite2 databases.  
It is designed as a lightweight alternative to external IP lookup services such as ipwho.is by using local MaxMind `.mmdb` files.

---

## Features

- Lookup IP address information using local GeoLite2 databases
- Normalized JSON response with:
  - Continent, country, region, city
  - Latitude / longitude
  - Timezone
  - Postal code
  - Custom country metadata (calling code, capital, borders, flags)
  - Connection info (ASN, ISP, route) via GeoLite2-ASN
- Simple HTTP API: `GET /ip/<ip>?raw=1`

---

## Requirements

- Python 3.9+ (recommended)
- MaxMind GeoLite2 databases:
  - `GeoLite2-City.mmdb`
  - `GeoLite2-ASN.mmdb`
- The following Python packages (installed via `requirements.txt`):
  - `Flask`
  - `geoip2`

---

## Project Structure

Example structure:

```text
project-root/
├─ app.py
├─ geoip_resolver.py
├─ requirements.txt
└─ data/
   ├─ GeoLite2-City.mmdb
   ├─ GeoLite2-ASN.mmdb
   └─ country_meta.json
