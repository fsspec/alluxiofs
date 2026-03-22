"""
Unit tests for alluxiofs.telemetry (AlluxioTelemetry, instrument_alluxio_client).
"""

import io
import unittest
from unittest.mock import MagicMock


class TestAlluxioTelemetry(unittest.TestCase):
    def _make_telem(self):
        from alluxiofs.telemetry import AlluxioTelemetry

        return AlluxioTelemetry(enable_otel=False)

    def test_initial_state(self):
        telem = self._make_telem()
        self.assertEqual(telem.reads_total, 0)
        self.assertEqual(telem.writes_total, 0)
        self.assertEqual(telem.bytes_read, 0)
        self.assertEqual(telem.bytes_written, 0)
        self.assertEqual(telem.cache_hits, 0)
        self.assertEqual(telem.cache_misses, 0)
        self.assertEqual(telem.read_latency_ms, [])
        self.assertEqual(telem.write_latency_ms, [])

    def test_record_read(self):
        telem = self._make_telem()
        telem.record_read("s3://bucket/file.bin", offset=0, length=1024, latency_s=0.01)
        self.assertEqual(telem.reads_total, 1)
        self.assertEqual(telem.bytes_read, 1024)
        self.assertAlmostEqual(telem.read_latency_ms[0], 10.0, places=3)

    def test_record_write(self):
        telem = self._make_telem()
        telem.record_write("s3://bucket/out.bin", num_bytes=2048, latency_s=0.02)
        self.assertEqual(telem.writes_total, 1)
        self.assertEqual(telem.bytes_written, 2048)
        self.assertAlmostEqual(telem.write_latency_ms[0], 20.0, places=3)

    def test_record_cache_hit_miss(self):
        telem = self._make_telem()
        telem.record_cache_hit("s3://bucket/a")
        telem.record_cache_hit("s3://bucket/b")
        telem.record_cache_miss("s3://bucket/c")
        self.assertEqual(telem.cache_hits, 2)
        self.assertEqual(telem.cache_misses, 1)

    def test_get_summary_structure(self):
        telem = self._make_telem()
        telem.record_read("p", 0, 500, 0.005)
        telem.record_read("p", 0, 500, 0.015)
        telem.record_write("p", 1000, 0.010)
        telem.record_cache_hit("p")
        telem.record_cache_miss("p")
        summary = telem.get_summary()
        expected_keys = {
            "reads_total",
            "writes_total",
            "bytes_read_gb",
            "bytes_written_gb",
            "cache_hits",
            "cache_misses",
            "cache_hit_rate",
            "read_latency_p50_ms",
            "read_latency_p99_ms",
            "write_latency_p50_ms",
            "write_latency_p99_ms",
            "read_throughput_mb_s",
            "write_throughput_mb_s",
        }
        self.assertEqual(set(summary.keys()), expected_keys)

    def test_cache_hit_rate(self):
        telem = self._make_telem()
        for _ in range(3):
            telem.record_cache_hit("p")
        telem.record_cache_miss("p")
        summary = telem.get_summary()
        self.assertAlmostEqual(summary["cache_hit_rate"], 0.75)

    def test_get_summary_empty(self):
        telem = self._make_telem()
        summary = telem.get_summary()
        self.assertEqual(summary["reads_total"], 0)
        self.assertEqual(summary["cache_hit_rate"], 0.0)

    def test_noop_span(self):
        telem = self._make_telem()
        with telem.start_span("test_op", {"key": "val"}):
            pass  # should not raise

    def test_print_summary_no_crash(self):
        telem = self._make_telem()
        telem.record_read("p", 0, 1024, 0.001)
        telem.print_summary()  # should not raise


class TestInstrumentAlluxioClient(unittest.TestCase):
    def _make_mock_client(self):
        client = MagicMock()
        client.read_file_range = MagicMock(return_value=b"data")
        client.write_chunked = MagicMock(return_value=True)
        return client

    def test_instrument_wraps_read(self):
        from alluxiofs.telemetry import instrument_alluxio_client

        client = self._make_mock_client()
        telem = instrument_alluxio_client(client)

        result = client.read_file_range("s3://p", "/alluxio/p", 0, 100)
        self.assertEqual(result, b"data")
        self.assertEqual(telem.reads_total, 1)

    def test_instrument_wraps_write(self):
        from alluxiofs.telemetry import instrument_alluxio_client

        client = self._make_mock_client()
        telem = instrument_alluxio_client(client)

        client.write_chunked("s3://p", b"hello world")
        self.assertEqual(telem.writes_total, 1)
        self.assertEqual(telem.bytes_written, len(b"hello world"))

    def test_instrument_records_failure(self):
        from alluxiofs.telemetry import instrument_alluxio_client

        client = self._make_mock_client()
        client.read_file_range.side_effect = IOError("connection refused")
        telem = instrument_alluxio_client(client)

        with self.assertRaises(IOError):
            client.read_file_range("s3://p", "/alluxio/p", 0, 100)
        self.assertEqual(telem.reads_total, 1)

    def test_instrument_reuses_existing_telem(self):
        from alluxiofs.telemetry import AlluxioTelemetry, instrument_alluxio_client

        client = self._make_mock_client()
        existing = AlluxioTelemetry(enable_otel=False)
        returned = instrument_alluxio_client(client, telemetry=existing)
        self.assertIs(returned, existing)

    def test_multiple_ops_accumulate(self):
        from alluxiofs.telemetry import instrument_alluxio_client

        client = self._make_mock_client()
        telem = instrument_alluxio_client(client)

        for _ in range(5):
            client.read_file_range("s3://p", "/p", 0, 200)
        for _ in range(3):
            client.write_chunked("s3://p", b"x" * 100)

        self.assertEqual(telem.reads_total, 5)
        self.assertEqual(telem.writes_total, 3)
        self.assertEqual(telem.bytes_written, 300)
        self.assertEqual(len(telem.read_latency_ms), 5)
        self.assertEqual(len(telem.write_latency_ms), 3)


if __name__ == "__main__":
    unittest.main()
