from __future__ import annotations

import json
import threading
import time
from typing import Any


class Cache:
    """Redis cache with an in-memory fallback for local development and tests."""

    def __init__(self, redis_url: str = ""):
        self._memory: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self._redis = None
        if redis_url:
            try:
                import redis

                client = redis.from_url(redis_url, decode_responses=True)
                client.ping()
                self._redis = client
            except Exception:
                self._redis = None

    def get(self, key: str) -> Any | None:
        if self._redis:
            raw = self._redis.get(key)
            return json.loads(raw) if raw else None
        with self._lock:
            value = self._memory.get(key)
            if not value or value[0] < time.monotonic():
                self._memory.pop(key, None)
                return None
            return value[1]

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        if self._redis:
            self._redis.set(key, json.dumps(value, default=str), ex=ttl_seconds)
            return
        with self._lock:
            self._memory[key] = (time.monotonic() + ttl_seconds, value)

    def increment(self, key: str, ttl_seconds: int) -> int:
        if self._redis:
            count = self._redis.incr(key)
            if count == 1:
                self._redis.expire(key, ttl_seconds)
            return int(count)
        with self._lock:
            value = self._memory.get(key)
            current = int(value[1]) if value and value[0] >= time.monotonic() else 0
            next_value = current + 1
            self._memory[key] = (time.monotonic() + ttl_seconds, next_value)
            return next_value


class FixedWindowLimiter:
    """A Redis-backed fixed-window limiter, with a thread-safe local fallback."""

    def __init__(self, cache: Cache, max_per_minute: int, max_per_day: int):
        self.cache = cache
        self.max_per_minute = max_per_minute
        self.max_per_day = max_per_day
        self._window = 0
        self._count = 0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        now_window = int(time.time() // 60)
        day = time.strftime("%Y%m%d", time.gmtime())
        daily = self.cache.increment(f"fmp:daily:{day}", 86460)
        if daily > self.max_per_day:
            raise RateLimitExceeded("FMP daily request budget exhausted")
        if self.cache._redis:
            key = f"fmp:rate:{now_window}"
            count = self.cache._redis.incr(key)
            if count == 1:
                self.cache._redis.expire(key, 61)
            if count > self.max_per_minute:
                raise RateLimitExceeded("FMP request-per-minute budget exhausted")
            return
        with self._lock:
            if self._window != now_window:
                self._window, self._count = now_window, 0
            self._count += 1
            if self._count > self.max_per_minute:
                raise RateLimitExceeded("FMP request-per-minute budget exhausted")


class RateLimitExceeded(RuntimeError):
    pass
