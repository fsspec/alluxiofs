import json
import threading
import time
from typing import Any
from typing import Dict
from typing import Optional

import fsspec
from fsspec import filesystem

from alluxiofs.client.const import ALLUXIO_UFS_INFO_REFRESH_INTERVAL_MINUTES
from alluxiofs.client.utils import get_protocol_from_path
from alluxiofs.client.utils import register_unregistered_ufs_to_fsspec
from alluxiofs.client.utils import setup_logger
from alluxiofs.client.utils import TagAdapter


class UfsInfo:
    """
    Data class representing UFS information.
    """

    def __init__(
        self, alluxio_path: str, ufs_full_path: str, options: Dict[str, Any]
    ):
        self.alluxio_path = alluxio_path
        self.ufs_full_path = ufs_full_path
        self.options = options


class UFSUpdater:
    """
    Class responsible for periodically updating Ufs Info in the background.
    """

    def __init__(self, alluxio):
        self.alluxio = alluxio
        self.config = alluxio.config if alluxio else None
        if self.alluxio:
            self.interval_seconds = (
                self.alluxio.config.ufs_info_refresh_interval_minutes * 60
            )
            base_logger = setup_logger(
                self.config.log_dir,
                self.config.log_level,
                self.__class__.__name__,
                self.config.log_tag_allowlist,
            )
            self.logger = TagAdapter(base_logger, {"tag": "[TRANSFER]"})
        else:
            self.interval_seconds = (
                ALLUXIO_UFS_INFO_REFRESH_INTERVAL_MINUTES * 60
            )
            base_logger = setup_logger(
                class_name=self.__class__.__name__,
                log_tags=self.config.log_tag_allowlist
                if self.config
                else None,
            )
            self.logger = TagAdapter(base_logger, {"tag": "[TRANSFER]"})

        # Stores the latest fetched result
        self._cached_ufs: Optional[Dict[str, Any]] = {}
        self._path_map: Optional[Dict[str, str]] = {}

        # Lock to protect the shared variables _cached_ufs and _path_map
        self._lock = threading.RLock()
        self._init_event = threading.Event()
        # Thread control flag
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _get_ufs_info_from_worker(self):
        """
        Original method: Fetch Ufs Info from the worker.
        """
        if self.alluxio:
            return self.alluxio.get_ufs_info_from_worker()
        else:
            return ""

    def _update_thread_target(self):
        """
        Target function for the background thread: periodically update data.
        """
        self.logger.debug(
            f"Background update thread started, updating every {self.interval_seconds} seconds..."
        )

        # Execute once immediately upon start
        self._execute_update()

        # Loop until the stop event is set
        while not self._stop_event.wait(self.interval_seconds):
            self._execute_update()

        self.logger.debug("Background update thread stopped.")

    def _execute_update(self):
        """
        Helper method to fetch data and update the cache safely.
        """
        try:
            ufs_info_json = self._get_ufs_info_from_worker()

            if ufs_info_json is not None:
                # Use lock to safely update the shared variable
                with self._lock:
                    ufs_info_list = self.parse_ufs_info(ufs_info_json)
                    self.register_ufs_fallback(ufs_info_list)
                self.logger.debug(
                    f"ufs's Info updated. Time: {time.strftime('%H:%M:%S')}"
                )
            else:
                self.logger.warning(
                    f"ufs's Info update failed, keeping previous result. Time: {time.strftime('%H:%M:%S')}"
                )
        except Exception as e:
            self.logger.error(
                f"Exception occurred during ufs's Info update: {e}"
            )
        finally:
            self._init_event.set()

    def start_updater(self):
        """
        Start the background thread.
        """
        if self._thread is None or not self._thread.is_alive():
            # daemon=True ensures the thread exits when the main program exits
            self._thread = threading.Thread(
                target=self._update_thread_target, daemon=True
            )
            self._thread.start()
            self.logger.debug("Ufs Info updater started.")
        else:
            self.logger.debug("Updater is already running.")

    def stop_updater(self):
        """
        Stop the background thread gracefully.
        """
        if self._thread and self._thread.is_alive():
            self._stop_event.set()  # Signal the thread to stop
            self._thread.join()  # Wait for the thread to finish
            self._stop_event.clear()  # Reset the event for the next start
            self.logger.debug("Ufs Info updater stopped.")

    def parse_ufs_info(self, ufs_info_str: str) -> list:
        """
        Parse UFS info from a JSON string.

        Args:
            ufs_info_str: JSON string containing UFS information

        Returns:
            List of UfsInfo objects, empty list if parsing fails or no data
        """
        if not ufs_info_str or not ufs_info_str.strip():
            self.logger.warning("Empty or None UFS info string provided")
            return []

        ufs_info_list = []

        try:
            ufs_info_json = json.loads(ufs_info_str)

            if not isinstance(ufs_info_json, dict):
                self.logger.error(
                    f"UFS info should be a JSON object, got {type(ufs_info_json)}"
                )
                return []

            for ufs_full_path, value in ufs_info_json.items():
                if not isinstance(value, dict):
                    self.logger.warning(
                        f"UFS config for {ufs_full_path} is not a dictionary, skipping"
                    )
                    continue

                alluxio_path = value.get("alluxio_path", "")
                options = {
                    k: v for k, v in value.items() if k != "alluxio_path"
                }

                ufs_info = UfsInfo(
                    alluxio_path=alluxio_path,
                    ufs_full_path=ufs_full_path,
                    options=options,
                )
                ufs_info_list.append(ufs_info)

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse UFS info JSON: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error parsing UFS info: {e}")
            return []

        return ufs_info_list

    def must_get_ufs_count(self):
        self._init_event.wait()
        return self.get_ufs_count()

    def get_ufs_count(self):
        with self._lock:
            if self._cached_ufs is None:
                return 0
            return len(self._cached_ufs)

    def get_protocol_from_path(self, path):
        return get_protocol_from_path(path)

    def must_get_ufs_from_path(self, path: str):
        self._init_event.wait()
        ufs = self.get_ufs_from_cache(path)
        if ufs is None:
            self.logger.error(
                f"No registered UFS found in alluxio for path: {path}"
            )
            raise ValueError(
                f"No registered UFS found in alluxio for path: {path}"
            )
        return ufs

    def get_ufs_from_cache(self, path: str):
        with self._lock:
            for ufs_path in self._cached_ufs:
                if path.startswith(ufs_path):
                    return self._cached_ufs[ufs_path]

    def must_get_alluxio_path_from_ufs_full_path(self, path: str):
        self._init_event.wait()
        return self.get_alluxio_path_from_ufs_full_path(path)

    def get_alluxio_path_from_ufs_full_path(self, path: str):
        with self._lock:
            for ufs_path in self._cached_ufs:
                if path.startswith(ufs_path):
                    return path.replace(ufs_path, self._path_map[ufs_path], 1)

    def register_ufs_fallback(self, ufs_info_list: UfsInfo):
        """
        Register under file systems (UFS) for fallback when accessed files fail in Alluxiofs.

        Args:
            ufs_info_list: List of UfsInfo objects containing UFS details
        """
        for ufs_info in ufs_info_list:
            protocol = self.get_protocol_from_path(
                ufs_info.ufs_full_path.lower()
            )
            register_unregistered_ufs_to_fsspec(protocol)
            if fsspec.get_filesystem_class(protocol) is None:
                raise ValueError(f"Unsupported protocol: {protocol}")
            else:
                target_options = ufs_info.options
                self._cached_ufs[ufs_info.ufs_full_path] = filesystem(
                    protocol, **target_options
                )
                self._path_map[ufs_info.ufs_full_path] = ufs_info.alluxio_path
                self.logger.debug(
                    f"Registered UFS client for {ufs_info.ufs_full_path}"
                )
