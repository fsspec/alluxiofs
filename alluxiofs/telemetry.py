"""
OpenTelemetry observability for AlluxioFileSystem I/O operations.

Wraps AlluxioClient read/write methods with OTel spans and records:
  - Per-operation spans (path, bytes, latency_ms, success attributes)
  - Counters : bytes_read, bytes_written, cache_hits, cache_misses
  - Histograms: read_latency_ms, write_latency_ms (used for p50/p99)

OTel SDK is an optional dependency. When not installed, all instrumentation
is a no-op and only in-process Python counters are maintained.

Opt-in usage
-------------
    from alluxiofs.telemetry import AlluxioTelemetry, instrument_alluxio_client

    telem = instrument_alluxio_client(alluxio_client, enable_otel=True)
    # ... run training workload ...
    telem.print_summary()
    metrics = telem.get_summary()   # dict with p50/p99 latencies, cache hit rate
"""

import contextlib
import functools
import logging
import time
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_OTEL_AVAILABLE = False
try:
    from opentelemetry import metrics, trace  # type: ignore
    from opentelemetry.sdk.metrics import MeterProvider  # type: ignore
    from opentelemetry.sdk.trace import TracerProvider  # type: ignore

    _OTEL_AVAILABLE = True
except ImportError:
    pass


@contextlib.contextmanager
def _noop_span():
    """Context manager that does nothing when OTel is unavailable."""
    yield


class AlluxioTelemetry:
    """
    Lightweight telemetry collector for Alluxio I/O operations.

    In-process counters are always available (no external dependency).
    OpenTelemetry SDK spans and metric exports are used when available.

    Attributes (updated in real-time):
        reads_total, writes_total: operation counts
        bytes_read, bytes_written: cumulative byte counts
        cache_hits, cache_misses: Alluxio cache outcome counts
        read_latency_ms, write_latency_ms: per-op latency samples (floats)

    Args:
        service_name: OTel service name attribute.
        enable_otel: Whether to emit OTel spans/metrics (requires SDK).
        tracer_provider: Custom OTel TracerProvider; uses global if None.
        meter_provider: Custom OTel MeterProvider; uses global if None.
    """

    def __init__(
        self,
        service_name: str = "alluxiofs",
        enable_otel: bool = True,
        tracer_provider=None,
        meter_provider=None,
    ):
        self.service_name = service_name
        self._use_otel = enable_otel and _OTEL_AVAILABLE

        # --- In-process counters (always available) ---
        self.reads_total: int = 0
        self.writes_total: int = 0
        self.bytes_read: int = 0
        self.bytes_written: int = 0
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        self.read_latency_ms: List[float] = []
        self.write_latency_ms: List[float] = []

        # --- OTel SDK instruments ---
        self._tracer = None
        self._bytes_read_ctr = None
        self._bytes_written_ctr = None
        self._read_lat_hist = None
        self._write_lat_hist = None
        self._cache_hit_ctr = None
        self._cache_miss_ctr = None

        if self._use_otel:
            tp = tracer_provider or trace.get_tracer_provider()
            mp = meter_provider or metrics.get_meter_provider()
            self._tracer = tp.get_tracer(service_name)
            meter = mp.get_meter(service_name)
            self._bytes_read_ctr = meter.create_counter(
                "alluxio.bytes_read",
                description="Total bytes read from Alluxio",
            )
            self._bytes_written_ctr = meter.create_counter(
                "alluxio.bytes_written",
                description="Total bytes written to Alluxio",
            )
            self._read_lat_hist = meter.create_histogram(
                "alluxio.read_latency_ms",
                description="Read operation latency in milliseconds",
            )
            self._write_lat_hist = meter.create_histogram(
                "alluxio.write_latency_ms",
                description="Write operation latency in milliseconds",
            )
            self._cache_hit_ctr = meter.create_counter(
                "alluxio.cache_hits",
                description="Number of Alluxio cache hits",
            )
            self._cache_miss_ctr = meter.create_counter(
                "alluxio.cache_misses",
                description="Number of Alluxio cache misses",
            )
        elif enable_otel and not _OTEL_AVAILABLE:
            logger.warning(
                "opentelemetry-sdk not installed; OTel disabled. "
                "Install with: pip install opentelemetry-sdk "
                "opentelemetry-api"
            )

    # ------------------------------------------------------------------
    # Recording helpers
    # ------------------------------------------------------------------

    def record_read(
        self,
        path: str,
        offset: int,
        length: int,
        latency_s: float,
        success: bool = True,
    ) -> None:
        """Record a completed read_file_range call."""
        latency_ms = latency_s * 1000.0
        self.reads_total += 1
        self.bytes_read += max(0, length)
        self.read_latency_ms.append(latency_ms)
        if self._use_otel:
            attrs = {"path": path, "success": str(success)}
            self._bytes_read_ctr.add(max(0, length), attrs)
            self._read_lat_hist.record(latency_ms, attrs)

    def record_write(
        self,
        path: str,
        num_bytes: int,
        latency_s: float,
        success: bool = True,
    ) -> None:
        """Record a completed write_chunked call."""
        latency_ms = latency_s * 1000.0
        self.writes_total += 1
        self.bytes_written += num_bytes
        self.write_latency_ms.append(latency_ms)
        if self._use_otel:
            attrs = {"path": path, "success": str(success)}
            self._bytes_written_ctr.add(num_bytes, attrs)
            self._write_lat_hist.record(latency_ms, attrs)

    def record_cache_hit(self, path: str) -> None:
        self.cache_hits += 1
        if self._use_otel:
            self._cache_hit_ctr.add(1, {"path": path})

    def record_cache_miss(self, path: str) -> None:
        self.cache_misses += 1
        if self._use_otel:
            self._cache_miss_ctr.add(1, {"path": path})

    def start_span(self, name: str, attributes: Optional[Dict] = None):
        """
        Return an OTel span context manager.

        Falls back to a no-op context manager when OTel is unavailable.
        Usage::

            with telem.start_span("my_op", {"key": "val"}):
                do_work()
        """
        if self._use_otel and self._tracer is not None:
            return self._tracer.start_as_current_span(
                name, attributes=attributes or {}
            )
        return _noop_span()

    # ------------------------------------------------------------------
    # Summary / reporting
    # ------------------------------------------------------------------

    def get_summary(self) -> Dict:
        """
        Return a dict of aggregated metrics.

        Keys include:
            reads_total, writes_total,
            bytes_read_gb, bytes_written_gb,
            cache_hits, cache_misses, cache_hit_rate,
            read_latency_p50_ms, read_latency_p99_ms,
            write_latency_p50_ms, write_latency_p99_ms,
            read_throughput_mb_s, write_throughput_mb_s
        """

        def percentile(samples: List[float], p: int) -> float:
            if not samples:
                return 0.0
            ordered = sorted(samples)
            idx = min(int(len(ordered) * p / 100), len(ordered) - 1)
            return ordered[idx]

        total_ops = self.cache_hits + self.cache_misses
        cache_hit_rate = self.cache_hits / total_ops if total_ops > 0 else 0.0

        total_read_ms = sum(self.read_latency_ms) or 1e-9
        total_write_ms = sum(self.write_latency_ms) or 1e-9

        return {
            "reads_total": self.reads_total,
            "writes_total": self.writes_total,
            "bytes_read_gb": self.bytes_read / 1e9,
            "bytes_written_gb": self.bytes_written / 1e9,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_rate": cache_hit_rate,
            "read_latency_p50_ms": percentile(self.read_latency_ms, 50),
            "read_latency_p99_ms": percentile(self.read_latency_ms, 99),
            "write_latency_p50_ms": percentile(self.write_latency_ms, 50),
            "write_latency_p99_ms": percentile(self.write_latency_ms, 99),
            "read_throughput_mb_s": (
                self.bytes_read / 1e6 / (total_read_ms / 1000)
                if self.read_latency_ms
                else 0.0
            ),
            "write_throughput_mb_s": (
                self.bytes_written / 1e6 / (total_write_ms / 1000)
                if self.write_latency_ms
                else 0.0
            ),
        }

    def print_summary(self) -> None:
        """Print a human-readable telemetry summary to stdout."""
        summary = self.get_summary()
        print("\n=== Alluxio I/O Telemetry Summary ===")
        col_w = max(len(k) for k in summary) + 2
        for k, v in summary.items():
            if isinstance(v, float):
                print(f"  {k:<{col_w}}: {v:.4f}")
            else:
                print(f"  {k:<{col_w}}: {v}")
        print("=" * 40)


def instrument_alluxio_client(
    alluxio_client,
    telemetry: Optional[AlluxioTelemetry] = None,
    enable_otel: bool = False,
) -> AlluxioTelemetry:
    """
    Monkey-patch an AlluxioClient to record telemetry on read/write calls.

    Wraps ``read_file_range`` and ``write_chunked`` with timing and byte
    counting. The original methods are called unchanged; only bookkeeping
    is added around them.

    Args:
        alluxio_client: The AlluxioClient instance to instrument.
        telemetry: Existing AlluxioTelemetry to reuse; creates one if None.
        enable_otel: Forward to AlluxioTelemetry constructor if telemetry
            is None (enables OTel SDK spans/metrics).

    Returns:
        The AlluxioTelemetry instance.

    Example::

        telem = instrument_alluxio_client(client)
        # run workload
        telem.print_summary()
    """
    if telemetry is None:
        telemetry = AlluxioTelemetry(enable_otel=enable_otel)

    # --- Wrap read_file_range ---
    original_read = alluxio_client.read_file_range

    @functools.wraps(original_read)
    def _instrumented_read(
        file_path, alluxio_path, offset=0, length=-1, prefetch_policy=None
    ):
        t0 = time.perf_counter()
        success = True
        try:
            result = original_read(
                file_path, alluxio_path, offset, length, prefetch_policy
            )
            return result
        except Exception:
            success = False
            raise
        finally:
            elapsed = time.perf_counter() - t0
            effective_len = length if length != -1 else 0
            telemetry.record_read(file_path, offset, effective_len, elapsed, success)

    alluxio_client.read_file_range = _instrumented_read

    # --- Wrap write_chunked ---
    original_write = alluxio_client.write_chunked

    @functools.wraps(original_write)
    def _instrumented_write(file_path, file_bytes, chunk_size=1024 * 1024):
        t0 = time.perf_counter()
        success = True
        try:
            result = original_write(file_path, file_bytes, chunk_size)
            return result
        except Exception:
            success = False
            raise
        finally:
            elapsed = time.perf_counter() - t0
            telemetry.record_write(file_path, len(file_bytes), elapsed, success)

    alluxio_client.write_chunked = _instrumented_write

    logger.info(
        "AlluxioClient instrumented with telemetry (otel=%s)", enable_otel
    )
    return telemetry
