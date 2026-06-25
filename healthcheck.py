import sys

from divifilter_data_updater.health import is_healthy

if __name__ == "__main__":
    # Exit 0 (healthy) / 1 (unhealthy) for the Docker HEALTHCHECK.
    sys.exit(0 if is_healthy() else 1)
