import logging


def set_log_level(logger, test_options):
    if "log_level" in test_options:
        log_level = test_options["log_level"].upper()
        if log_level == "DEBUG":
            logger.setLevel(logging.DEBUG)
        elif log_level == "INFO":
            logger.setLevel(logging.INFO)
        elif log_level == "WARN" or log_level == "WARNING":
            logger.setLevel(logging.WARN)
        else:
            logger.warning(f"Unsupported log level: {log_level}")
