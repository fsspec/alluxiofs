# main.py
import logging
import os


def setup_logging(
    log_file: str = "/Users/alluxio/downloads/res/logfile.log",
    level: str = "DEBUG",
):
    # Ensure the directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    handlers = [logging.StreamHandler(), logging.FileHandler(log_file)]

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s -- %(message)s",
        handlers=handlers,
    )


# Now import other modules

import pytest

# Your main script logic here
if __name__ == "__main__":
    # Run your main logic or tests
    pytest.main(["-v", "local_fallback_alluxio_prefix_test.py"])
