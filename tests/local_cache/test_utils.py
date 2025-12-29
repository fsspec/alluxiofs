import sys
from unittest.mock import MagicMock
from unittest.mock import patch

import pycurl
import pytest

from alluxiofs.client.log import setup_logger
from alluxiofs.client.log import TagAdapter
from alluxiofs.client.log import TagFilter
from alluxiofs.client.prefetch_policy import AdaptiveWindowPrefetchPolicy
from alluxiofs.client.prefetch_policy import FixedWindowPrefetchPolicy
from alluxiofs.client.prefetch_policy import NoPrefetchPolicy
from alluxiofs.client.utils import _c_send_get_request_stream
from alluxiofs.client.utils import _c_send_get_request_write_bytes
from alluxiofs.client.utils import _c_send_get_request_write_file
from alluxiofs.client.utils import convert_ufs_info_to
from alluxiofs.client.utils import get_prefetch_policy
from alluxiofs.client.utils import get_protocol_from_path
from alluxiofs.client.utils import register_unregistered_ufs_to_fsspec
from alluxiofs.client.utils import retry_on_network


class TestUtils:
    def test_convert_ufs_info_to(self):
        # Test oss conversion
        info_oss = {"access_key": "ak", "secret_key": "sk", "endpoint": "ep"}
        res_oss = convert_ufs_info_to("oss", info_oss)
        assert res_oss == {"key": "ak", "secret": "sk", "endpoint": "ep"}

        # Test other ufs
        info_other = {
            "access_key": "ak",
            "secret_key": "sk",
            "endpoint": "ep",
            "other": "val",
        }
        res_other = convert_ufs_info_to("s3", info_other)
        assert res_other == info_other

    def test_get_protocol_from_path(self):
        assert get_protocol_from_path("s3://bucket/key") == "s3"
        assert get_protocol_from_path("hdfs://namenode/path") == "hdfs"
        assert get_protocol_from_path("/local/path") is None
        assert get_protocol_from_path(None) is None

    @patch("alluxiofs.client.utils.fsspec")
    def test_register_unregistered_ufs_to_fsspec(self, mock_fsspec):
        # Test bos registration success
        with patch.dict("sys.modules", {"bosfs": MagicMock()}):
            register_unregistered_ufs_to_fsspec("bos")
            mock_fsspec.register_implementation.assert_called()

        # Test other protocol (does nothing)
        mock_fsspec.reset_mock()
        register_unregistered_ufs_to_fsspec("s3")
        mock_fsspec.register_implementation.assert_not_called()

    def test_register_unregistered_ufs_to_fsspec_import_error(self):
        # Test bos import error
        # We need to simulate that bosfs cannot be imported
        with patch.dict("sys.modules"):
            if "bosfs" in sys.modules:
                del sys.modules["bosfs"]

            # We use a side effect on __import__ to raise ImportError only for bosfs
            original_import = __import__

            def mock_import(name, *args, **kwargs):
                if name == "bosfs":
                    raise ImportError("No module named bosfs")
                return original_import(name, *args, **kwargs)

            with patch("builtins.__import__", side_effect=mock_import):
                with pytest.raises(ImportError, match="Please install bosfs"):
                    register_unregistered_ufs_to_fsspec("bos")


class TestLogging:
    def test_tag_adapter(self):
        logger = MagicMock()
        adapter = TagAdapter(logger, {"tag": "[TAG]"})
        msg, kwargs = adapter.process("message", {})
        assert msg == "[TAG] message"

    def test_tag_filter(self):
        # Test string tags
        f = TagFilter("TAG1, TAG2")
        assert "TAG1" in f.tags
        assert "TAG2" in f.tags

        # Test list tags
        f = TagFilter(["TAG1", "TAG2"])
        assert "TAG1" in f.tags

        # Test None tags
        f = TagFilter(None)
        assert f.tags == []

        # Test filter logic
        f = TagFilter(["ERROR", "WARN"])
        record = MagicMock()

        record.getMessage.return_value = "This is an ERROR message"
        assert f.filter(record) is True

        record.getMessage.return_value = "This is a normal message"
        assert f.filter(record) is False

        # Empty tags allow all
        f = TagFilter([])
        assert f.filter(record) is True

    @patch("alluxiofs.client.log.logging")
    @patch("alluxiofs.client.log.os")
    def test_setup_logger(self, mock_os, mock_logging):
        mock_os.getenv.return_value = None
        mock_os.path.exists.return_value = False

        # Mock getLevelName to return an int
        mock_logging.INFO = 20
        mock_logging.getLevelName.return_value = 20

        logger = setup_logger(
            file_path="/tmp/logs", level_str="DEBUG", log_tags="TAG1"
        )
        assert logger is not None

        mock_os.makedirs.assert_called_with("/tmp/logs", exist_ok=True)
        mock_logging.FileHandler.assert_called()
        mock_logging.StreamHandler.assert_called()

        mock_logger = mock_logging.getLogger.return_value
        assert mock_logger.addHandler.call_count >= 2  # Stream and File


class TestPrefetchPolicy:
    @patch("alluxiofs.client.prefetch_policy.setup_logger")
    def test_get_prefetch_policy(self, mock_setup_logger):
        config = MagicMock()
        config.log_dir = "/tmp"
        config.log_level = "INFO"
        config.log_tag_allowlist = []

        config.local_cache_prefetch_policy = "none"
        assert isinstance(get_prefetch_policy(config, 1024), NoPrefetchPolicy)

        config.local_cache_prefetch_policy = "fixed_window"
        assert isinstance(
            get_prefetch_policy(config, 1024), FixedWindowPrefetchPolicy
        )

        config.local_cache_prefetch_policy = "adaptive_window"
        assert isinstance(
            get_prefetch_policy(config, 1024), AdaptiveWindowPrefetchPolicy
        )

        config.local_cache_prefetch_policy = "unknown"
        with pytest.raises(ValueError):
            get_prefetch_policy(config, 1024)


class TestRetry:
    def test_retry_on_network_success(self):
        mock_func = MagicMock(return_value="success")
        decorated = retry_on_network(tries=3, delay=0.1)(mock_func)

        assert decorated() == "success"
        assert mock_func.call_count == 1

    def test_retry_on_network_fail_then_success(self):
        mock_func = MagicMock(side_effect=[pycurl.error("error"), "success"])
        decorated = retry_on_network(tries=3, delay=0.01)(mock_func)

        assert decorated() == "success"
        assert mock_func.call_count == 2

    def test_retry_on_network_fail_max_retries(self):
        mock_func = MagicMock(side_effect=pycurl.error("error"))
        decorated = retry_on_network(tries=3, delay=0.01)(mock_func)

        with pytest.raises(pycurl.error):
            decorated()
        assert mock_func.call_count == 3

    def test_retry_on_network_runtime_error_timeout(self):
        # Simulate curl timeout error (28) wrapped in RuntimeError
        error = RuntimeError("cURL error: (28) Timeout")
        mock_func = MagicMock(side_effect=[error, "success"])
        decorated = retry_on_network(tries=3, delay=0.01)(mock_func)

        assert decorated() == "success"
        assert mock_func.call_count == 2

    def test_retry_on_network_runtime_error_other(self):
        error = RuntimeError("Other error")
        mock_func = MagicMock(side_effect=error)
        decorated = retry_on_network(tries=3, delay=0.01)(mock_func)

        with pytest.raises(RuntimeError, match="Other error"):
            decorated()
        assert mock_func.call_count == 1


class TestCurlRequests:
    @patch("alluxiofs.client.utils.pycurl.Curl")
    def test_c_send_get_request_write_bytes(self, mock_curl_cls):
        mock_curl = mock_curl_cls.return_value
        mock_curl.getinfo.return_value = 200

        with patch("alluxiofs.client.utils.BytesIO") as mock_bytes_io:
            mock_buffer = mock_bytes_io.return_value
            mock_buffer.getvalue.return_value = b"data"

            res = _c_send_get_request_write_bytes(
                "http://url", {"header": "val"}
            )

            assert res == b"data"
            mock_curl.perform.assert_called_once()
            mock_curl.setopt.assert_any_call(mock_curl.URL, b"http://url")

    @patch("alluxiofs.client.utils.pycurl.Curl")
    def test_c_send_get_request_write_bytes_error(self, mock_curl_cls):
        mock_curl = mock_curl_cls.return_value
        mock_curl.getinfo.return_value = 404

        with pytest.raises(FileNotFoundError):
            _c_send_get_request_write_bytes("http://url", {})

    @patch("alluxiofs.client.utils.pycurl.Curl")
    def test_c_send_get_request_write_file(self, mock_curl_cls):
        mock_curl = mock_curl_cls.return_value
        mock_curl.getinfo.return_value = 200

        mock_file = MagicMock()
        _c_send_get_request_write_file("http://url", {}, mock_file)

        mock_curl.perform.assert_called_once()
        mock_curl.setopt.assert_any_call(mock_curl.WRITEDATA, mock_file)

    @patch("alluxiofs.client.utils.pycurl.Curl")
    def test_c_send_get_request_stream(self, mock_curl_cls):
        mock_curl = mock_curl_cls.return_value
        mock_curl.getinfo.return_value = 200

        with patch("alluxiofs.client.utils.BytesIO") as mock_bytes_io:
            mock_buffer = mock_bytes_io.return_value

            res = _c_send_get_request_stream("http://url", 10)

            assert res == mock_buffer
            mock_curl.perform.assert_called_once()
