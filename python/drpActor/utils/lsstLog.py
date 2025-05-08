import logging


def setLsstLongLog(level=logging.INFO):
    """
    Configure LSST loggers to use a detailed, consistent log format.

    This function:
    - Enables Python-based logging for LSST log messages using `lsst.log.usePythonLogging()`.
    - Sets a verbose log format that includes timestamp, logger name, filename, and line number.
    - Applies the specified logging level (default: INFO) to all LSST-related loggers.
    - Attaches a stream handler if no handlers are already set.

    Parameters
    ----------
    level : int, optional
        Logging level to apply to LSST loggers. Defaults to `logging.INFO`.
    """
    import lsst.log
    lsst.log.usePythonLogging()
    # "Borrowed" long-log style
    formatter = logging.Formatter(
        fmt="%(levelname)-5s %(asctime)s %(name)s (%(filename)s:%(lineno)d) - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )

    # logging.getLogger().setLevel(logging.WARNING)  # Default level for all loggers

    for name in logging.root.manager.loggerDict:
        if name.startswith("lsst"):
            logger = logging.getLogger(name)
            logger.setLevel(level)
            if not logger.handlers:
                handler = logging.StreamHandler()
                handler.setFormatter(formatter)
                logger.addHandler(handler)
            else:
                for handler in logger.handlers:
                    handler.setFormatter(formatter)
