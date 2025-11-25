from __future__ import annotations

import logging
from typing import Tuple, Optional

import redis

from config import settings

logger = logging.getLogger(__name__)


class RateLimitResult:
	"""Container for rate limit decision."""

	__slots__ = ("allowed", "retry_after", "remaining")

	def __init__(self, allowed: bool, retry_after: Optional[int], remaining: Optional[int]) -> None:
		self.allowed = allowed
		self.retry_after = retry_after
		self.remaining = remaining


_redis_client: Optional[redis.Redis] = None


def get_redis_client() -> Optional[redis.Redis]:
	"""Initialize and cache Redis client. Fail open if connection fails."""
	global _redis_client
	if _redis_client is not None:
		return _redis_client

	try:
		_redis_client = redis.Redis.from_url(settings.redis_url)
		# Optional lightweight ping to verify connectivity
		_redis_client.ping()
		logger.info("Connected to Redis for rate limiting at %s", settings.redis_url)
		return _redis_client
	except Exception as e:
		logger.error("Failed to connect to Redis for rate limiting: %s", e)
		_redis_client = None
		return None


def check_rate_limit(
	identifier: str,
) -> RateLimitResult:
	"""Check rate limit for a given identifier (e.g. client IP).

	Uses a fixed window algorithm:
		- Redis key: netrecon:rl:<identifier>
		- INCR on each request
		- EXPIRE set to window size on first hit
		- If count > limit => request is rejected until key expires
	"""
	if not settings.rate_limit_enabled:
		# Rate limiting disabled => always allow
		return RateLimitResult(allowed=True, retry_after=None, remaining=None)

	client = get_redis_client()
	if client is None:
		# Redis not available => fail open (do not block traffic)
		return RateLimitResult(allowed=True, retry_after=None, remaining=None)

	limit = settings.rate_limit_requests_per_window
	window = settings.rate_limit_window_seconds

	key = f"netrecon:rl:{identifier}"

	try:
		# Increment the counter for this identifier
		current = client.incr(key)

		if current == 1:
			# First hit in this window, set the TTL
			client.expire(key, window)

		if current > limit:
			# Over the limit, compute how many seconds until reset
			ttl = client.ttl(key)
			retry_after = ttl if ttl is not None and ttl > 0 else window
			remaining = 0
			return RateLimitResult(allowed=False, retry_after=int(retry_after), remaining=remaining)

		remaining = max(limit - current, 0)
		return RateLimitResult(allowed=True, retry_after=None, remaining=remaining)

	except Exception as e:
		# On Redis error we fail open but log the problem
		logger.error("Rate limiting check failed for %s: %s", identifier, e)
		return RateLimitResult(allowed=True, retry_after=None, remaining=None)
