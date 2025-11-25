import os
from flask import Flask, jsonify, request, g, Response
from geoip_resolver import lookup_ip

from datetime import datetime, timedelta
from config import settings
from formatters import to_ipwhois_format

from logging_config import setup_logging
from metrics import metrics
import logging
import time
from prometheus_exporter import format_prometheus_metrics

from rate_limiter import check_rate_limit

# Configure logging before creating the app
setup_logging()
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.before_request
def before_request():
	"""Store request start time and enforce rate limiting."""
	g.request_start_time = time.perf_counter()

	# Extract client IP (respecting X-Forwarded-For if present)
	client_ip = request.headers.get("X-Forwarded-For", request.remote_addr) or "unknown"

	rl_result = check_rate_limit(client_ip)

	if not rl_result.allowed:
		# Build a 429 Too Many Requests response
		payload = {
			"error": "rate_limited",
			"message": "Too many requests. Please slow down.",
			"retry_after_seconds": rl_result.retry_after,
		}
		resp = jsonify(payload)
		resp.status_code = 429
		if rl_result.retry_after is not None:
			resp.headers["Retry-After"] = str(rl_result.retry_after)
		return resp


@app.after_request
def after_request(response):
	"""Log request details and record metrics after each response."""
	try:
		start = getattr(g, "request_start_time", None)
		duration_ms = None
		if start is not None:
			duration_ms = (time.perf_counter() - start) * 1000.0
		else:
			duration_ms = 0.0

		path = request.path
		status_code = response.status_code
		method = request.method
		client_ip = request.headers.get("X-Forwarded-For", request.remote_addr)

		# Record metrics
		metrics.record_request(path=path, status_code=status_code, duration_ms=duration_ms)

		# Structured-ish logging (simple key=value style)
		logger.info(
			"request_completed method=%s path=%s status=%s duration_ms=%.2f client_ip=%s",
			method,
			path,
			status_code,
			duration_ms,
			client_ip,
		)
	except Exception as e:
		logger.error("after_request logging failed: %s", e)

	return response

@app.route("/ip/<ip>")
def ip_lookup(ip):
	"""Perform an IP lookup using local GeoLite2 databases.

	Query params:
		raw=1        -> returns internal normalized payload
		compat=ipwhois -> returns ipwho.is compatible payload
	"""
	print(f"[+] Lookup request for IP: {ip}")
	raw = request.args.get("raw", "0").lower() in ("1", "true", "yes")
	compat = request.args.get("compat", "").lower()

	data, err = lookup_ip(ip)
	

	if err == "invalid_ip":
		return jsonify({"error": "invalid_ip", "ip": ip}), 400
	if err == "not_found":
		return jsonify({"error": "ip_not_found", "ip": ip}), 404
	if err and err.startswith("lookup_error"):
		return jsonify({"error": "lookup_failed", "details": err}), 502

	if compat == "ipwhois":
		return jsonify(to_ipwhois_format(data))

	# Raw is currently equal to the normalized output
	return jsonify(data)

@app.route("/health")
def health():
	"""Simple health check endpoint."""
	return jsonify({"status": "ok"}), 200


@app.route("/metrics")
def metrics_endpoint():
	"""Expose basic in-memory metrics for observability."""
	snap = metrics.snapshot()
	return jsonify(snap), 200

@app.route("/metrics/prom")
def metrics_prom_endpoint():
	"""Expose metrics in Prometheus text exposition format."""
	snap = metrics.snapshot()
	body = format_prometheus_metrics(snap)
	# Prometheus text format content type
	return Response(
		body,
		status=200,
		mimetype="text/plain; version=0.0.4; charset=utf-8",
	)

if __name__ == "__main__":
	port = settings.port
	# Debug should only be enabled in development
	if settings.flask_debug:
		app.debug = True
	app.run(
		host="0.0.0.0",
		port=port,
		debug=settings.flask_debug
	)
