import pytest


@pytest.fixture(autouse=True)
def isolated_livesync_hash_cache(tmp_path, monkeypatch):
    """No test may consume or overwrite a real connector hash cache."""
    monkeypatch.setenv("LIVESYNC_HASH_CACHE_DIR", str(tmp_path / "livesync-hashes"))
