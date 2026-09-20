"""LiveSync content hashes and stable, temporary source snapshots."""

import hashlib
from pathlib import Path
from unittest.mock import Mock

import pytest
import respx

from oikb.connectors import SourceFileUnavailable
from oikb.connectors.livesync import LiveSyncConnector
from oikb.sync import run_sync


def gateway():
    listing = respx.post("http://gateway/hooks/livesync-list").respond(200, json={
        "status": "success", "count": 1,
        "files": [{"path": "Atlas/note.md", "size": 999, "revision": "1-first"}],
    })
    read = respx.post("http://gateway/hooks/livesync-read").respond(200, content=b"# Note\n")
    return listing, read


@respx.mock
def test_hash_snapshot_refresh_and_revision_independence():
    listing, read = gateway()
    with LiveSyncConnector(root="Atlas", gateway_url="http://gateway") as connector:
        first = connector.build_manifest()[0]
        temp_path = Path(connector._snapshot_dir.name)
        assert first.checksum == hashlib.sha256(b"# Note\n").hexdigest()
        assert first.size == len(b"# Note\n")
        read.respond(200, content=b"changed during scan")
        assert connector.read_file("", "note.md") == b"# Note\n"
        assert read.call_count == 1

        # A new revision invalidates the cached hash.
        listing.respond(200, json={"status": "success", "count": 1, "files": [
            {"path": "Atlas/note.md", "revision": "2-new", "size": 999},
        ]})
        second = connector.build_manifest()[0]
        assert second.checksum == hashlib.sha256(b"changed during scan").hexdigest()
        assert not temp_path.exists()
        # Different revision, same bytes: checksum stays stable.
        listing.respond(200, json={"status": "success", "count": 1, "files": [
            {"path": "Atlas/note.md", "revision": "3-new", "size": 999},
        ]})
        assert connector.build_manifest()[0].checksum == second.checksum
        read.respond(200, content=b"")
        listing.respond(200, json={"status": "success", "count": 1, "files": [
            {"path": "Atlas/note.md", "revision": "4-empty", "size": 0},
        ]})
        empty = connector.build_manifest()[0]
        assert empty.checksum == hashlib.sha256(b"").hexdigest() and empty.size == 0
        temp_path = Path(connector._snapshot_dir.name)
    assert not temp_path.exists()


@respx.mock
def test_failed_scan_cleans_snapshot_and_never_diffs_destination():
    _, read = gateway()
    read.respond(404)
    connector = LiveSyncConnector(root="Atlas", gateway_url="http://gateway")
    client = Mock()
    with pytest.raises(SourceFileUnavailable):
        run_sync(client, connector, "kb", quiet=True)
    assert connector._snapshot_dir is None
    assert not client.mock_calls
