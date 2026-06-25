import os
import time
import logging

logger = logging.getLogger(__name__)

DEFAULT_HEARTBEAT_FILE = "/tmp/divifilter_healthcheck"
# Margin (seconds) added on top of the configured max inter-run delay before a
# heartbeat is considered stale - covers the time a normal scrape/update takes.
HEARTBEAT_MARGIN_SECONDS = 600


def heartbeat_file():
    return os.getenv("HEALTHCHECK_FILE", DEFAULT_HEARTBEAT_FILE)


def write_heartbeat(max_delay_seconds=0, path=None):
    """
    Record a liveness heartbeat (current time + the configured max inter-run
    delay) for the Docker healthcheck. Called once per loop iteration. Never
    raises - a heartbeat write must not be able to break the main loop.
    """
    path = path or heartbeat_file()
    try:
        with open(path, "w") as f:
            f.write(f"{time.time()} {int(max_delay_seconds)}")
    except OSError as e:
        logger.warning("Could not write heartbeat to %s: %s", path, e)


def is_healthy(path=None):
    """
    True if the heartbeat file was updated recently enough that the loop is
    clearly still alive and progressing.

    The staleness threshold defaults to the max inter-run delay recorded in the
    heartbeat plus a margin, so an idle-but-healthy loop is never flagged. It
    can be overridden with the HEALTHCHECK_MAX_STALENESS_SECONDS env var.
    """
    path = path or heartbeat_file()
    try:
        with open(path) as f:
            parts = f.read().split()
        last = float(parts[0])
        max_delay = int(parts[1]) if len(parts) > 1 else 0
    except (OSError, ValueError, IndexError):
        return False

    override = os.getenv("HEALTHCHECK_MAX_STALENESS_SECONDS")
    threshold = float(override) if override else max_delay + HEARTBEAT_MARGIN_SECONDS
    return (time.time() - last) <= threshold
