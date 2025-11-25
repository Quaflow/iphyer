import threading
from collections import defaultdict
from time import time
from datetime import datetime, timezone


class Metrics:
	"""Simple in-memory metrics collector for NetRecon."""

	def __init__(self) -> None:
		self._lock = threading.Lock()
		self.total_requests = 0
		self.total_errors = 0
		self.total_success = 0
		self.path_counters = defaultdict(int)
		self.status_counters = defaultdict(int)
		self.last_request_timestamp = None
		self.last_request_datetime = None
		self.total_latency_ms = 0.0

	def record_request(self, path: str, status_code: int, duration_ms: float) -> None:
		"""Record a single HTTP request."""
		now_ts = time()
		now_dt = datetime.fromtimestamp(now_ts, timezone.utc)
		with self._lock:
			self.total_requests += 1
			self.path_counters[path] += 1
			self.status_counters[status_code] += 1
			self.total_latency_ms += duration_ms
			self.last_request_timestamp = now_ts
			self.last_request_datetime = now_dt

			if 200 <= status_code < 400:
				self.total_success += 1
			else:
				self.total_errors += 1

	def snapshot(self) -> dict:
		"""Return a snapshot of current metrics as a plain dict."""
		with self._lock:
			avg_latency = (
				self.total_latency_ms / self.total_requests
				if self.total_requests > 0
				else 0.0
			)

			return {
				"total_requests": self.total_requests,
				"total_success": self.total_success,
				"total_errors": self.total_errors,
				"average_latency_ms": avg_latency,
				"by_path": dict(self.path_counters),
				"by_status_code": dict(self.status_counters),
				"last_request_timestamp": self.last_request_timestamp,
				"last_request_datetime": self.last_request_datetime,
			}


metrics = Metrics()
