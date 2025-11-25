from typing import Dict, Any


def _sanitize_label_value(value: str) -> str:
	"""Escape characters inside Prometheus label values."""
	return (
		value.replace("\\", "\\\\")
		.replace("\n", "\\n")
		.replace('"', '\\"')
	)


def format_prometheus_metrics(snapshot: Dict[str, Any]) -> str:
	"""Convert internal metrics snapshot into Prometheus exposition format."""
	lines: list[str] = []

	total_requests = snapshot.get("total_requests", 0) or 0
	total_success = snapshot.get("total_success", 0) or 0
	total_errors = snapshot.get("total_errors", 0) or 0
	avg_latency = snapshot.get("average_latency_ms", 0.0) or 0.0
	by_path = snapshot.get("by_path", {}) or {}
	by_status = snapshot.get("by_status_code", {}) or {}
	last_ts = snapshot.get("last_request_timestamp", 0) or 0

	# Total requests
	lines.append("# HELP netrecon_requests_total Total number of HTTP requests handled.")
	lines.append("# TYPE netrecon_requests_total counter")
	lines.append(f"netrecon_requests_total {total_requests}")

	# Successful requests
	lines.append("# HELP netrecon_requests_success_total Total number of successful HTTP requests.")
	lines.append("# TYPE netrecon_requests_success_total counter")
	lines.append(f"netrecon_requests_success_total {total_success}")

	# Error requests
	lines.append("# HELP netrecon_requests_error_total Total number of error HTTP responses.")
	lines.append("# TYPE netrecon_requests_error_total counter")
	lines.append(f"netrecon_requests_error_total {total_errors}")

	# Average latency
	lines.append("# HELP netrecon_request_latency_ms_average Average request latency in milliseconds.")
	lines.append("# TYPE netrecon_request_latency_ms_average gauge")
	lines.append(f"netrecon_request_latency_ms_average {avg_latency}")

	# Last request timestamp
	lines.append("# HELP netrecon_last_request_timestamp_seconds Unix timestamp of the last handled request.")
	lines.append("# TYPE netrecon_last_request_timestamp_seconds gauge")
	lines.append(f"netrecon_last_request_timestamp_seconds {int(last_ts)}")

	# Requests by path (with labels)
	lines.append("# HELP netrecon_requests_by_path_total Total requests grouped by HTTP path.")
	lines.append("# TYPE netrecon_requests_by_path_total counter")
	for path, count in by_path.items():
		if path is None:
			continue
		label = _sanitize_label_value(str(path))
		lines.append(f'netrecon_requests_by_path_total{{path="{label}"}} {count}')

	# Requests by status code (with labels)
	lines.append("# HELP netrecon_requests_by_status_total Total requests grouped by HTTP status code.")
	lines.append("# TYPE netrecon_requests_by_status_total counter")
	for status, count in by_status.items():
		label = _sanitize_label_value(str(status))
		lines.append(f'netrecon_requests_by_status_total{{status="{label}"}} {count}')

	# Newline at the end is recommended by Prometheus
	return "\n".join(lines) + "\n"
