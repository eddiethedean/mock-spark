"""Lightweight profiling utilities for Mock Spark hot paths."""

from __future__ import annotations

import logging
import threading
import time
import tracemalloc
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from collections.abc import Generator

from mock_spark import config

logger = logging.getLogger("mock_spark.profiling")


@dataclass
class ProfileEvent:
    """Structured representation of a profiling sample."""

    name: str
    category: str
    duration_ms: float
    peak_kb: float
    metadata: dict[str, Any] = field(default_factory=dict)


class _ThreadLocalEvents(threading.local):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[ProfileEvent] = []


class ProfilingRecorder:
    """Collect profiling events in thread local storage."""

    def __init__(self) -> None:
        self._events = _ThreadLocalEvents()
        self._lock = threading.Lock()

    def record_event(self, event: ProfileEvent) -> None:
        self._events.events.append(event)
        logger.debug(
            "Profiling event %s (%s): %.2fms (peak %.1fKB) metadata=%s",
            event.name,
            event.category,
            event.duration_ms,
            event.peak_kb,
            event.metadata,
        )

    def reset(self) -> None:
        self._events.events = []

    def list_events(self) -> list[ProfileEvent]:
        return list(self._events.events)


_recorder = ProfilingRecorder()
_tracemalloc_lock = threading.Lock()
_tracemalloc_started = False


def _ensure_tracemalloc() -> None:
    global _tracemalloc_started
    if _tracemalloc_started:
        return
    with _tracemalloc_lock:
        if not _tracemalloc_started:
            tracemalloc.start()
            _tracemalloc_started = True


class ProfileScope:
    """Container used inside the profiling context to collect metadata."""

    __slots__ = ("_metadata",)

    def __init__(self) -> None:
        self._metadata: dict[str, Any] = {}

    def add_metadata(self, extra: dict[str, Any] | None) -> None:
        if not extra:
            return
        self._metadata.update(extra)

    @property
    def metadata(self) -> dict[str, Any]:
        return self._metadata


class _NoopScope(ProfileScope):
    def __init__(self) -> None:
        super().__init__()

    def add_metadata(self, extra: dict[str, Any] | None) -> None:  # noqa: D401
        return


def profiling_enabled() -> bool:
    """Fast check for whether profiling is enabled."""

    return config.is_feature_enabled("enable_performance_profiling")


@contextmanager
def profile_block(
    name: str,
    category: str = "runtime",
) -> Generator[ProfileScope, None, None]:
    """Context manager that records timing + memory metrics."""

    if not profiling_enabled():
        yield _NoopScope()
        return

    _ensure_tracemalloc()
    scope = ProfileScope()
    start_time = time.perf_counter()
    before_current, before_peak = tracemalloc.get_traced_memory()

    try:
        yield scope
    finally:
        duration_ms = (time.perf_counter() - start_time) * 1000.0
        after_current, after_peak = tracemalloc.get_traced_memory()
        peak_kb = max(before_peak, after_peak) / 1024.0
        alloc_kb = max(after_current - before_current, 0) / 1024.0
        scope.add_metadata({"alloc_kb": round(alloc_kb, 3)})
        _recorder.record_event(
            ProfileEvent(
                name=name,
                category=category,
                duration_ms=round(duration_ms, 3),
                peak_kb=round(peak_kb, 3),
                metadata=scope.metadata,
            )
        )


def profiled(
    name: str,
    category: str = "runtime",
    extra_resolver: (
        Callable[[tuple[Any, ...], dict[str, Any], Any], dict[str, Any] | None] | None
    ) = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator for profiling functions/methods."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not profiling_enabled():
                return func(*args, **kwargs)

            with profile_block(name, category) as scope:
                result = func(*args, **kwargs)
                if extra_resolver is not None:
                    try:
                        scope.add_metadata(extra_resolver(args, kwargs, result))
                    except Exception as exc:  # pragma: no cover - defensive
                        logger.debug(
                            "Failed to resolve profiling metadata for %s: %s",
                            name,
                            exc,
                        )
                return result

        return wrapper

    return decorator


def collect_events() -> list[ProfileEvent]:
    """Return events recorded on this thread."""

    return _recorder.list_events()


def clear_events() -> None:
    """Clear events recorded on this thread."""

    _recorder.reset()
