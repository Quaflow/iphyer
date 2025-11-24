import os
from flask import Flask, jsonify, request
from geoip_resolver import lookup_ip

from datetime import datetime, timedelta
from config import settings

app = Flask(__name__)


@app.route("/ip/<ip>")
def ip_lookup(ip):
	"""Perform an IP lookup using local GeoLite2 databases.
	
	Query param:
		raw=1  -> returns raw/normalized result (currently identical)
	"""
	print(f"[+] Lookup request for IP: {ip}")
	raw = request.args.get("raw", "0").lower() in ("1", "true", "yes")

	start_time = datetime.now()
	data, err = lookup_ip(ip)
	end_time = datetime.now()
	elapsed_time = (end_time - start_time).total_seconds()
	print(f"[+] Lookup completed in {elapsed_time:.2f} seconds.")

	if err == "invalid_ip":
		return jsonify({"error": "invalid_ip", "ip": ip}), 400
	if err == "not_found":
		return jsonify({"error": "ip_not_found", "ip": ip}), 404
	if err and err.startswith("lookup_error"):
		return jsonify({"error": "lookup_failed", "details": err}), 502

	# Raw is currently equal to the normalized output
	return jsonify(data)


if __name__ == "__main__":
	port = settings.port
	# Debug should only be enabled in development
	if DEBUG_MODE:
		app.debug = True
	app.run(host="0.0.0.0", port=port, debug=settings.flask_debug)
