"""Connector-local caching, bounded reads and unchanged shared scan behavior."""

import hashlib
import json
from unittest.mock import Mock

import httpx
import pytest
import respx

from oikb.connectors import SourceFileUnavailable
from oikb.connectors.livesync import LiveSyncConnector
from oikb.kb_sync import run_entries_sync
from oikb.sync import SyncCancelled, build_manifest_filter, run_sync


def serve(files):
    def listing(request):
        return httpx.Response(200, json={"status": "success", "count": len(files), "files": [
            {"path": path, "revision": revision, "size": len(content)}
            for path, (revision, content) in files.items()
        ]})

    def read(request):
        _, content = files[json.loads(request.content)["path"]]
        return httpx.Response(200, content=content)

    lists = respx.post("http://gateway/hooks/livesync-list").mock(side_effect=listing)
    reads = respx.post("http://gateway/hooks/livesync-read").mock(side_effect=read)
    return lists, reads


def connector(**kwargs):
    return LiveSyncConnector(root="Atlas", gateway_url="http://gateway", **kwargs)


@respx.mock
def test_warm_scan_reuses_hash_across_connector_instances():
    files = {"Atlas/a.md": ("1-a", b"alpha"), "Atlas/b.md": ("1-b", b"beta")}
    lists, reads = serve(files)
    with connector(token="private-token") as source:
        initial = source.build_manifest()
        cache_path = source._cache_path
        assert "private-token" not in cache_path.read_text()
        assert "alpha" not in cache_path.read_text()
    assert reads.call_count == 2 and lists.call_count == 2
    with connector(token="private-token") as source:
        assert source.build_manifest() == initial
        assert not source._snapshots
    assert reads.call_count == 2 and lists.call_count == 3


@respx.mock
def test_only_changed_revisions_download_and_hash_stays_content_based():
    files = {"Atlas/a.md": ("1-a", b"alpha"), "Atlas/b.md": ("1-b", b"beta")}
    _, reads = serve(files)
    with connector() as source:
        initial = source.build_manifest()
    files["Atlas/a.md"] = ("2-a", b"alpha")
    with connector() as source:
        assert source.build_manifest() == initial
    assert reads.call_count == 3
    assert json.loads(reads.calls.last.request.content)["path"] == "Atlas/a.md"
    files["Atlas/a.md"] = ("3-a", b"changed")
    with connector() as source:
        assert source.build_manifest()[0].checksum == hashlib.sha256(b"changed").hexdigest()
    assert reads.call_count == 4


@respx.mock
def test_kb_rebuild_downloads_cached_files_only_when_upload_is_needed():
    files = {"Atlas/a.md": ("1-a", b"alpha"), "Atlas/b.md": ("1-b", b"beta")}
    _, reads = serve(files)
    with connector() as source:
        source.build_manifest()
    client = Mock()
    client.sync_diff.side_effect = lambda kb, manifest: {"added": manifest}
    result = run_sync(client, connector(), "empty-kb", quiet=True, concurrency=2)
    assert result.added == 2 and not result.errors
    assert reads.call_count == 4
    for call in client.upload_file.call_args_list:
        assert call.kwargs["file_hash"] == hashlib.sha256(call.kwargs["file_content"]).hexdigest()


@pytest.mark.parametrize("combined", [False, True])
@respx.mock
def test_existing_filters_still_apply_after_scan_in_both_entry_points(combined):
    files = {"Atlas/note.md": ("1", b"ok"), "Atlas/attachment.bin": ("2", b"x" * 20),
             "Atlas/large.md": ("3", b"x" * 20)}
    _, reads = serve(files)
    client = Mock()
    client.sync_diff.return_value = {}
    if combined:
        run_entries_sync(client, [{"source": "livesync:Atlas", "kb-id": "kb", "target-path": "mirror",
                                  "filter": {"include": ["*.md"], "max-size": 10}}],
                         resolve_connector=lambda *a, **kw: connector(), quiet=True)
        assert client.sync_diff.call_args.args[1][0]["path"] == "mirror"
    else:
        run_sync(client, connector(), "kb", quiet=True,
                 manifest_filter=build_manifest_filter(include=["*.md"], max_size=10))
    assert reads.call_count == 3  # Shared OIKB filtering is intentionally unchanged.
    assert [e["filename"] for e in client.sync_diff.call_args.args[1]] == ["note.md"]


@respx.mock
def test_all_filtered_out_keeps_existing_empty_manifest_semantics():
    _, reads = serve({"Atlas/b.bin": ("1", b"binary")})
    client = Mock()
    client.sync_diff.return_value = {}
    run_sync(client, connector(), "kb", quiet=True,
             manifest_filter=build_manifest_filter(include=["*.md"]))
    assert reads.call_count == 1
    assert client.sync_diff.call_args.args[1] == []


@respx.mock
def test_revision_race_never_publishes_hash_or_changes_destination():
    files = {"Atlas/note.md": ("1", b"before")}
    _, reads = serve(files)

    def change_during_read(request):
        files["Atlas/note.md"] = ("2", b"after")
        return httpx.Response(200, content=b"before")

    reads.mock(side_effect=change_during_read)
    source, client = connector(), Mock()
    with pytest.raises(SourceFileUnavailable, match="changed during scan"):
        run_sync(client, source, "kb", quiet=True)
    assert not source._cache_path.exists()
    assert source._snapshot_dir is None and not client.mock_calls


@respx.mock
def test_cache_hit_is_verified_again_when_needed_for_upload():
    files = {"Atlas/note.md": ("1", b"before")}
    _, reads = serve(files)
    with connector() as source:
        source.build_manifest()
    with connector() as source:
        source.build_manifest()
        files["Atlas/note.md"] = ("2", b"after")
        with pytest.raises(SourceFileUnavailable, match="changed during scan"):
            source.read_file("", "note.md")
    assert reads.call_count == 1  # Revision guard prevented even the download.


@respx.mock
def test_bad_cached_digest_is_invalidated_not_uploaded():
    files = {"Atlas/note.md": ("1", b"before")}
    _, reads = serve(files)
    with connector() as source:
        source.build_manifest()
        cache = json.loads(source._cache_path.read_text())
        cache["files"]["note.md"]["sha256"] = "0" * 64
        source._cache_path.write_text(json.dumps(cache))
    with connector() as source:
        source.build_manifest()
        with pytest.raises(SourceFileUnavailable, match="content changed"):
            source.read_file("", "note.md")
        assert not source._snapshots
    with connector() as source:
        assert source.build_manifest()[0].checksum == hashlib.sha256(b"before").hexdigest()
    assert reads.call_count == 3


@pytest.mark.parametrize("bad_cache", ["not json", "[]", '{"version":2,"files":{}}', '{"version":1,"files":{"note.md":{}}}'])
@respx.mock
def test_invalid_cache_is_a_miss_not_an_empty_source(bad_cache):
    _, reads = serve({"Atlas/note.md": ("1", b"note")})
    with connector() as source:
        source._cache_path.parent.mkdir(parents=True)
        source._cache_path.write_text(bad_cache)
        assert len(source.build_manifest()) == 1
    assert reads.call_count == 1


@respx.mock
def test_deleted_paths_are_pruned_from_hash_cache():
    files = {"Atlas/note.md": ("1", b"note")}
    _, reads = serve(files)
    with connector() as source:
        source.build_manifest()
    files.clear()
    with connector() as source:
        assert source.build_manifest() == []
        assert json.loads(source._cache_path.read_text())["files"] == {}
    assert reads.call_count == 1


def test_cache_identity_separates_gateway_root_and_credentials():
    identities = set()
    for options in ({}, {"root": "Other"}, {"token": "secret"}, {"gateway_url": "http://other"}):
        with LiveSyncConnector(**{"root": "Atlas", "gateway_url": "http://gateway", **options}) as source:
            identities.add(source._cache_path)
    assert len(identities) == 4


@pytest.mark.parametrize("combined", [False, True])
@respx.mock
def test_shared_cancellation_remains_after_scan_in_both_entry_points(combined):
    files = {"Atlas/a.md": ("1", b"a"), "Atlas/b.md": ("1", b"b")}
    _, reads = serve(files)
    cancelled = [False]

    def cancel_on_read(request):
        cancelled[0] = True
        return httpx.Response(200, content=b"a")

    reads.mock(side_effect=cancel_on_read)
    source, client = connector(), Mock()
    with pytest.raises(SyncCancelled):
        if combined:
            run_entries_sync(client, [{"source": "livesync:Atlas", "kb-id": "kb"}],
                             resolve_connector=lambda *a, **kw: source, quiet=True,
                             cancel_requested=lambda: cancelled[0])
        else:
            run_sync(client, source, "kb", quiet=True, cancel_requested=lambda: cancelled[0])
    assert reads.call_count == 2 and not client.mock_calls
    assert source._snapshot_dir is None and source._cache_path.exists()


class Chunks(httpx.SyncByteStream):
    def __init__(self):
        self.count = 0
        self.closed = False

    def __iter__(self):
        for _ in range(10):
            self.count += 1
            yield b"x" * 65536

    def close(self):
        self.closed = True


@pytest.mark.parametrize("reason", ["file_limit", "disk_limit"])
@respx.mock
def test_stream_stops_early_and_cleans_partial_download(reason):
    chunks = Chunks()
    respx.post("http://gateway/hooks/livesync-list").respond(200, json={
        "status": "success", "count": 1, "files": [{"path": "Atlas/note.md", "revision": "1", "size": 1}],
    })
    respx.post("http://gateway/hooks/livesync-read").respond(200, stream=chunks)
    limits = {"max_file_bytes": 70000} if reason == "file_limit" else {"max_snapshot_bytes": 70000}
    source, client = connector(**limits), Mock()
    with pytest.raises(SourceFileUnavailable):
        run_sync(client, source, "kb", quiet=True)
    assert chunks.count == 2 and chunks.closed
    assert not source._cache_path.exists() and source._snapshot_dir is None
    assert not client.mock_calls


@respx.mock
def test_unwritable_cache_does_not_fail_sync(tmp_path):
    _, reads = serve({"Atlas/note.md": ("1", b"note")})
    blocked = tmp_path / "not-a-directory"
    blocked.write_text("occupied")
    with connector(cache_dir=str(blocked)) as source:
        assert len(source.build_manifest()) == 1
    assert reads.call_count == 1


@respx.mock
def test_cached_upload_http_failure_keeps_readable_error_and_blocks_cleanup(monkeypatch):
    monkeypatch.setattr("oikb.sync.time.sleep", lambda _: None)
    _, reads = serve({"Atlas/note.md": ("1", b"note")})
    with connector() as source:
        source.build_manifest()
    reads.mock(side_effect=lambda request: httpx.Response(503, text="gateway temporarily unavailable"))
    client = Mock()
    client.sync_diff.side_effect = lambda kb, manifest: {"added": manifest, "deleted": [{"file_id": "old"}]}
    result = run_sync(client, connector(), "kb", quiet=True)
    assert result.errors and "gateway temporarily unavailable" in result.errors[0]
    client.upload_file.assert_not_called()
    client.sync_cleanup.assert_not_called()


@respx.mock
def test_filtered_scan_cache_does_not_hide_newly_included_files():
    _, reads = serve({"Atlas/a.md": ("1", b"a"), "Atlas/b.txt": ("1", b"b")})
    client = Mock()
    client.sync_diff.return_value = {}
    run_sync(client, connector(), "kb", quiet=True,
             manifest_filter=build_manifest_filter(include=["*.md"]))
    assert len(client.sync_diff.call_args.args[1]) == 1
    assert reads.call_count == 2
    with connector() as source:
        assert len(source.build_manifest()) == 2
    assert reads.call_count == 2


@respx.mock
def test_warm_sync_with_matching_kb_hashes_does_not_read_or_upload():
    files = {"Atlas/note.md": ("1", b"note")}
    lists, reads = serve(files)
    with connector() as source:
        stored = source.build_manifest()[0].checksum
    client = Mock()

    def diff(kb, manifest):
        assert manifest[0]["checksum"] == stored
        return {"unmodified_count": 1}

    client.sync_diff.side_effect = diff
    result = run_sync(client, connector(), "kb", quiet=True)
    assert result.unmodified == 1 and result.total_changes == 0
    assert reads.call_count == 1 and lists.call_count == 3
    client.upload_file.assert_not_called()
    client.sync_cleanup.assert_not_called()
