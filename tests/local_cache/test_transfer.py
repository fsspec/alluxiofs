import pytest
import json
from unittest.mock import MagicMock, patch

from alluxiofs.client.transfer import UfsInfo, UFSUpdater
from alluxiofs.client.const import ALLUXIO_UFS_INFO_REFRESH_INTERVAL_MINUTES

class TestUfsInfo:
    def test_init(self):
        info = UfsInfo("alluxio_path", "ufs_path", {"opt": "val"})
        assert info.alluxio_path == "alluxio_path"
        assert info.ufs_full_path == "ufs_path"
        assert info.options == {"opt": "val"}

class TestUFSUpdater:
    @pytest.fixture
    def mock_alluxio(self):
        alluxio = MagicMock()
        alluxio.config.ufs_info_refresh_interval_minutes = 10
        alluxio.config.log_dir = "/tmp"
        alluxio.config.log_level = "INFO"
        alluxio.config.log_tag_allowlist = []
        return alluxio

    def test_init_with_alluxio(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        assert updater.interval_seconds == 600
        assert updater.alluxio == mock_alluxio
        assert updater._cached_ufs == {}
        assert updater._path_map == {}

    def test_init_without_alluxio(self):
        updater = UFSUpdater(None)
        assert updater.interval_seconds == ALLUXIO_UFS_INFO_REFRESH_INTERVAL_MINUTES * 60
        assert updater.alluxio is None

    def test_parse_ufs_info_valid(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        json_str = json.dumps({
            "s3://bucket/path": {
                "alluxio_path": "/mnt/s3",
                "key": "value"
            }
        })
        result = updater.parse_ufs_info(json_str)
        assert len(result) == 1
        assert result[0].ufs_full_path == "s3://bucket/path"
        assert result[0].alluxio_path == "/mnt/s3"
        assert result[0].options == {"key": "value"}

    def test_parse_ufs_info_invalid_json(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        result = updater.parse_ufs_info("invalid json")
        assert result == []

    def test_parse_ufs_info_empty(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        assert updater.parse_ufs_info("") == []
        assert updater.parse_ufs_info(None) == []

    def test_parse_ufs_info_not_dict(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        result = updater.parse_ufs_info("[]")
        assert result == []

    def test_parse_ufs_info_value_not_dict(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        json_str = json.dumps({"path": "not a dict"})
        result = updater.parse_ufs_info(json_str)
        assert result == []

    @patch("alluxiofs.client.transfer.UFSUpdater.get_protocol_from_path")
    @patch("alluxiofs.client.transfer.register_unregistered_ufs_to_fsspec")
    @patch("alluxiofs.client.transfer.filesystem")
    @patch("alluxiofs.client.transfer.fsspec.get_filesystem_class")
    def test_register_ufs_fallback(self, mock_get_fs_class, mock_filesystem, mock_register, mock_get_protocol, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        ufs_info = UfsInfo("/mnt/s3", "s3://bucket", {"key": "val"})

        mock_get_protocol.return_value = "s3"
        mock_get_fs_class.return_value = True # Simulate supported protocol
        mock_fs_instance = MagicMock()
        mock_filesystem.return_value = mock_fs_instance

        updater.register_ufs_fallback([ufs_info])

        mock_get_protocol.assert_called_with("s3://bucket")
        mock_register.assert_called_with("s3")
        mock_filesystem.assert_called_with("s3", key="val")
        assert updater._cached_ufs["s3://bucket"] == mock_fs_instance
        assert updater._path_map["s3://bucket"] == "/mnt/s3"

    @patch("alluxiofs.client.transfer.register_unregistered_ufs_to_fsspec")
    @patch("alluxiofs.client.transfer.fsspec.get_filesystem_class")
    def test_register_ufs_fallback_unsupported(self, mock_get_fs_class, mock_register, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        ufs_info = UfsInfo("/mnt/unknown", "unknown://bucket", {})

        mock_get_fs_class.return_value = None # Simulate unsupported protocol

        with pytest.raises(ValueError, match="Unsupported protocol"):
            updater.register_ufs_fallback([ufs_info])

    def test_get_ufs_from_cache(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        updater._cached_ufs = {"s3://bucket": "fs_instance"}

        assert updater.get_ufs_from_cache("s3://bucket/file") == "fs_instance"
        assert updater.get_ufs_from_cache("hdfs://namenode/file") is None

    def test_get_alluxio_path_from_ufs_full_path(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        updater._cached_ufs = {"s3://bucket": "fs_instance"}
        updater._path_map = {"s3://bucket": "/mnt/s3"}

        assert updater.get_alluxio_path_from_ufs_full_path("s3://bucket/file") == "/mnt/s3/file"
        assert updater.get_alluxio_path_from_ufs_full_path("hdfs://namenode/file") is None

    @patch("alluxiofs.client.transfer.UFSUpdater.parse_ufs_info")
    @patch("alluxiofs.client.transfer.UFSUpdater.register_ufs_fallback")
    def test_execute_update_success(self, mock_register, mock_parse, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        mock_alluxio.get_ufs_info_from_worker.return_value = '{"json": "data"}'
        mock_parse.return_value = ["parsed_info"]

        updater._execute_update()

        mock_parse.assert_called_with('{"json": "data"}')
        mock_register.assert_called_with(["parsed_info"])
        assert updater._init_event.is_set()

    def test_execute_update_fail_none(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        mock_alluxio.get_ufs_info_from_worker.return_value = None

        updater._execute_update()
        # Should not raise exception, just log warning
        assert updater._init_event.is_set()

    def test_execute_update_exception(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        mock_alluxio.get_ufs_info_from_worker.side_effect = Exception("Network error")

        updater._execute_update()
        # Should catch exception and log error
        assert updater._init_event.is_set()

    def test_start_stop_updater(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        with patch("threading.Thread") as mock_thread_cls:
            mock_thread = MagicMock()
            mock_thread_cls.return_value = mock_thread

            updater.start_updater()
            mock_thread.start.assert_called_once()

            # Start again should do nothing
            updater.start_updater()
            mock_thread.start.assert_called_once()

            mock_thread.is_alive.return_value = True

            # Mock _stop_event to verify calls
            updater._stop_event = MagicMock()

            updater.stop_updater()

            updater._stop_event.set.assert_called_once()
            mock_thread.join.assert_called_once()
            updater._stop_event.clear.assert_called_once()

    def test_must_get_methods_wait(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)

        # Mock _init_event.wait to return immediately
        updater._init_event.wait = MagicMock()

        updater._cached_ufs = {"s3://bucket": "fs"}
        assert updater.must_get_ufs_count() == 1
        updater._init_event.wait.assert_called()

        updater.get_ufs_from_cache = MagicMock(return_value="fs")
        assert updater.must_get_ufs_from_path("path") == "fs"

        updater.get_ufs_from_cache.return_value = None
        with pytest.raises(ValueError):
            updater.must_get_ufs_from_path("path")

        updater.get_alluxio_path_from_ufs_full_path = MagicMock(return_value="/path")
        assert updater.must_get_alluxio_path_from_ufs_full_path("path") == "/path"

