import os
import time
import tempfile
import unittest
from unittest.mock import patch

from divifilter_data_updater.health import write_heartbeat, is_healthy


class TestHealth(unittest.TestCase):

    def setUp(self):
        fd, self.path = tempfile.mkstemp()
        os.close(fd)
        os.remove(self.path)  # start with no heartbeat file

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_fresh_heartbeat_is_healthy(self):
        write_heartbeat(max_delay_seconds=3600, path=self.path)
        self.assertTrue(is_healthy(path=self.path))

    def test_missing_file_is_unhealthy(self):
        self.assertFalse(is_healthy(path=self.path))

    def test_malformed_file_is_unhealthy(self):
        with open(self.path, "w") as f:
            f.write("not-a-timestamp")
        self.assertFalse(is_healthy(path=self.path))

    def test_stale_heartbeat_is_unhealthy(self):
        # heartbeat far older than max_delay (60s) + margin (600s)
        stale = time.time() - 5000
        with open(self.path, "w") as f:
            f.write(f"{stale} 60")
        self.assertFalse(is_healthy(path=self.path))

    def test_threshold_adapts_to_recorded_max_delay(self):
        # 1000s old is stale for a 60s max delay but fine for a 3600s one
        old = time.time() - 1000
        with open(self.path, "w") as f:
            f.write(f"{old} 60")
        self.assertFalse(is_healthy(path=self.path))
        with open(self.path, "w") as f:
            f.write(f"{old} 3600")
        self.assertTrue(is_healthy(path=self.path))

    @patch.dict(os.environ, {"HEALTHCHECK_MAX_STALENESS_SECONDS": "100"})
    def test_env_override_tightens_threshold(self):
        old = time.time() - 500
        with open(self.path, "w") as f:
            f.write(f"{old} 3600")  # would be healthy by recorded delay...
        self.assertFalse(is_healthy(path=self.path))  # ...but override caps at 100s

    def test_write_heartbeat_never_raises_on_bad_path(self):
        # writing to a non-existent directory must not raise
        write_heartbeat(max_delay_seconds=10, path="/nonexistent_dir/hb")


if __name__ == '__main__':
    unittest.main()
