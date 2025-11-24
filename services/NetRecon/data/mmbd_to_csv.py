import sys
import csv
import ipaddress
import maxminddb
from pathlib import Path


def mmdb_to_csv(mmdb_path: str, csv_path: str) -> None:
	mmdb_path = Path(mmdb_path)

	if not mmdb_path.exists():
		print(f"[!] MMDB file not found: {mmdb_path}")
		return

	with maxminddb.open_database(str(mmdb_path)) as reader:
		row_count = 0

		with open(csv_path, "w", newline="", encoding="utf-8") as f:
			writer = csv.writer(f)

			writer.writerow([
				"start_ip",
				"end_ip",
				"country_iso",
				"country_name",
				"city_name",
				"latitude",
				"longitude",
			])
			for network, data in reader:
				start_ip = network.network_address
				end_ip = network.broadcast_address

				country_iso = ""
				country_name = ""
				city_name = ""
				latitude = ""
				longitude = ""

				if isinstance(data, dict):
					country = data.get("country") or data.get("registered_country") or {}
					country_iso = country.get("iso_code", "") or country.get("iso", "") or ""
					names = country.get("names", {})
					if isinstance(names, dict):
						country_name = names.get("en", "") or next(iter(names.values()), "")

					city = data.get("city", {})
					city_names = city.get("names", {})
					if isinstance(city_names, dict):
						city_name = city_names.get("en", "") or next(iter(city_names.values()), "")

					location = data.get("location", {})
					if isinstance(location, dict):
						latitude = location.get("latitude", "")
						longitude = location.get("longitude", "")

				writer.writerow([
					str(start_ip),
					str(end_ip),
					country_iso,
					country_name,
					city_name,
					latitude,
					longitude,
				])

				row_count += 1
				
				if row_count <= 3:
					print(f"[DEBUG] {start_ip} - {end_ip} → {country_iso} {country_name} {city_name}")

		print(f"✔ Done. Wrote {row_count} rows to {csv_path}")


if __name__ == "__main__":
	if len(sys.argv) >= 2:
		mmdb_file = sys.argv[1]
	else:
		mmdb_file = "GeoLite2-City.mmdb"

	if len(sys.argv) >= 3:
		csv_file = sys.argv[2]
	else:
		csv_file = "output.csv"

	print(f"[*] Reading MMDB: {mmdb_file}")
	print(f"[*] Writing CSV:  {csv_file}")
	mmdb_to_csv(mmdb_file, csv_file)
