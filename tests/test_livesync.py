from __future__ import annotations

import json
from unittest.mock import Mock

import httpx
import pytest
import respx
from click.testing import CliRunner

from oikb.cli import _resolve_connector, cli
from oikb.client import OikbClient
from oikb.connectors import SourceFileUnavailable
from oikb.connectors.livesync import LiveSyncConnector, parse_livesync_source
from oikb.sync import build_manifest_filter, run_sync


# ── 1. Source Syntax & Parsing ──────────────────────────────────────


def test_parse_livesync_source_standard():
    assert parse_livesync_source("livesync:Knowledge/software-engineering") == {
        "root": "Knowledge/software-engineering"
    }


def test_parse_livesync_source_with_slashes():
    assert parse_livesync_source("livesync:/Knowledge/software-engineering/") == {
        "root": "Knowledge/software-engineering"
    }


def test_parse_livesync_source_empty_root():
    assert parse_livesync_source("livesync:") == {"root": ""}
    assert parse_livesync_source("livesync:/") == {"root": ""}


def test_parse_livesync_source_with_query_params():
    parsed = parse_livesync_source("livesync:Knowledge/docs?timeout=45&extra=val")
    assert parsed == {
        "root": "Knowledge/docs",
        "timeout": "45",
        "extra": "val",
    }


def test_parse_livesync_source_invalid_scheme():
    with pytest.raises(ValueError, match="Invalid LiveSync source"):
        parse_livesync_source("confluence:ENG")


# ── 2. Connector Init & Auth Validation ─────────────────────────────


def test_init_missing_gateway_url(monkeypatch):
    monkeypatch.delenv("LIVESYNC_GATEWAY_URL", raising=False)
    monkeypatch.delenv("LIVESYNC_URL", raising=False)
    with pytest.raises(ValueError, match="LiveSync gateway URL required"):
        LiveSyncConnector(root="docs")


def test_init_env_url_and_token(monkeypatch):
    monkeypatch.setenv("LIVESYNC_GATEWAY_URL", "http://gateway.local")
    monkeypatch.setenv("LIVESYNC_GATEWAY_TOKEN", "env-secret-token")

    connector = LiveSyncConnector(root="Knowledge")
    try:
        assert connector.gateway_url == "http://gateway.local"
        assert connector.token == "env-secret-token"
        assert (
            connector._http.headers.get("Authorization")
            == "Bearer env-secret-token"
        )
    finally:
        connector.close()


def test_init_explicit_args():
    connector = LiveSyncConnector(
        root="Knowledge/software-engineering",
        gateway_url="http://custom-gw:9000/",
        token="my-token",
    )
    try:
        assert connector.gateway_url == "http://custom-gw:9000"
        assert connector.root == "Knowledge/software-engineering"
        assert connector.token == "my-token"
        assert connector._http.headers.get("Authorization") == "Bearer my-token"
    finally:
        connector.close()


# ── 3. List Hierarchy & Read File ───────────────────────────────────


@respx.mock
def test_list_hierarchy_and_read_file():
    gw_url = "http://livesync-gateway"
    files_payload = [
        {
            "path": "domain-reference/open-webui.md",
            "size": 1234,
            "revision": "rev-101",
        },
        {
            "path": "overview.md",
            "size": 500,
            "revision": "rev-102",
        },
    ]
    respx.get(f"{gw_url}/files").mock(
        return_value=httpx.Response(200, json=files_payload)
    )
    respx.get(f"{gw_url}/files/domain-reference/open-webui.md").mock(
        return_value=httpx.Response(200, content=b"# Open WebUI Architecture")
    )
    respx.get(f"{gw_url}/files/overview.md").mock(
        return_value=httpx.Response(200, content=b"# Overview Notes")
    )

    connector = LiveSyncConnector(
        root="Knowledge/software-engineering",
        gateway_url=gw_url,
        token="test-token",
    )
    try:
        manifest = connector.build_manifest()
        assert len(manifest) == 2

        # Manifest is sorted by display_path
        first = manifest[0]
        assert first.filename == "open-webui.md"
        assert first.path == "domain-reference"
        assert first.checksum == "rev-101"
        assert first.size == 1234
        assert first.display_path == "domain-reference/open-webui.md"

        second = manifest[1]
        assert second.filename == "overview.md"
        assert second.path == ""
        assert second.checksum == "rev-102"
        assert second.size == 500
        assert second.display_path == "overview.md"

        # Read file contents
        content1 = connector.read_file("domain-reference", "open-webui.md")
        assert content1 == b"# Open WebUI Architecture"

        content2 = connector.read_file("", "overview.md")
        assert content2 == b"# Overview Notes"
    finally:
        connector.close()


# ── 4. Nested Folders ───────────────────────────────────────────────


@respx.mock
def test_nested_folders():
    gw_url = "http://livesync-gateway"
    files_payload = [
        {
            "path": "Knowledge/software-engineering/backend/services/auth/service.go",
            "size": 4096,
            "revision": "hash-go-42",
        }
    ]
    respx.get(f"{gw_url}/files").mock(
        return_value=httpx.Response(200, json=files_payload)
    )
    respx.get(
        f"{gw_url}/files/Knowledge/software-engineering/backend/services/auth/service.go"
    ).mock(return_value=httpx.Response(200, content=b"package auth"))

    connector = LiveSyncConnector(
        root="Knowledge/software-engineering",
        gateway_url=gw_url,
    )
    try:
        manifest = connector.build_manifest()
        assert len(manifest) == 1
        entry = manifest[0]
        assert entry.filename == "service.go"
        assert entry.path == "backend/services/auth"
        assert (
            entry.display_path == "backend/services/auth/service.go"
        )
        assert entry.checksum == "hash-go-42"

        data = connector.read_file("backend/services/auth", "service.go")
        assert data == b"package auth"
    finally:
        connector.close()


# ── 5. Empty Source ─────────────────────────────────────────────────


@respx.mock
def test_empty_source():
    gw_url = "http://livesync-gateway"
    respx.get(f"{gw_url}/files").mock(
        return_value=httpx.Response(200, json=[])
    )

    connector = LiveSyncConnector(root="EmptyFolder", gateway_url=gw_url)
    try:
        manifest = connector.build_manifest()
        assert manifest == []
    finally:
        connector.close()


# ── 6. Auth & Auth Errors ───────────────────────────────────────────


@respx.mock
def test_auth_headers_and_unauthorized():
    gw_url = "http://livesync-gateway"
    list_route = respx.get(f"{gw_url}/files").mock(
        return_value=httpx.Response(200, json=[])
    )

    connector = LiveSyncConnector(
        root="docs", gateway_url=gw_url, token="secret-bearer-token"
    )
    try:
        connector.build_manifest()
        assert list_route.called
        assert (
            list_route.calls[0].request.headers["Authorization"]
            == "Bearer secret-bearer-token"
        )
    finally:
        connector.close()

    # Unauthorized test
    respx.get(f"{gw_url}/files").mock(
        return_value=httpx.Response(401, text="Unauthorized")
    )
    with LiveSyncConnector(root="docs", gateway_url=gw_url, token="bad-token") as conn:
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            conn.build_manifest()
        assert exc_info.value.response.status_code == 401


# ── 7. Gateway Errors & File Not Found ──────────────────────────────


@respx.mock
def test_gateway_errors_and_file_unavailable():
    gw_url = "http://livesync-gateway"
    respx.get(f"{gw_url}/files").mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )

    with LiveSyncConnector(root="docs", gateway_url=gw_url) as conn:
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            conn.build_manifest()
        assert exc_info.value.response.status_code == 500

    # 404 on read raises SourceFileUnavailable
    respx.get(f"{gw_url}/files").mock(
        return_value=httpx.Response(
            200,
            json=[{"path": "missing.txt", "size": 10, "revision": "1"}],
        )
    )
    respx.get(f"{gw_url}/files/missing.txt").mock(
        return_value=httpx.Response(404, text="Not Found")
    )

    with LiveSyncConnector(root="", gateway_url=gw_url) as conn:
        conn.build_manifest()
        with pytest.raises(SourceFileUnavailable, match="not found on LiveSync gateway"):
            conn.read_file("", "missing.txt")


# ── 8. Duplicate Paths Handling ─────────────────────────────────────


@respx.mock
def test_duplicate_paths_deduplicated():
    gw_url = "http://livesync-gateway"
    # Gateway returns duplicate path entries (e.g. earlier revision then updated revision)
    files_payload = [
        {"path": "notes/todo.md", "size": 100, "revision": "rev-old"},
        {"path": "notes/todo.md", "size": 250, "revision": "rev-new"},
    ]
    respx.get(f"{gw_url}/files").mock(
        return_value=httpx.Response(200, json=files_payload)
    )

    with LiveSyncConnector(root="", gateway_url=gw_url) as conn:
        manifest = conn.build_manifest()
        assert len(manifest) == 1
        entry = manifest[0]
        assert entry.display_path == "notes/todo.md"
        assert entry.checksum == "rev-new"
        assert entry.size == 250


# ── 9. Revision Changes & Deleted Files ─────────────────────────────


@respx.mock
def test_revision_changes_and_deleted_files():
    gw_url = "http://livesync-gateway"
    client = Mock()

    # Scenario 1: Initial state
    respx.get(f"{gw_url}/files").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"path": "doc1.md", "size": 10, "revision": "v1"},
                {"path": "doc2.md", "size": 20, "revision": "v1"},
            ],
        )
    )
    with LiveSyncConnector(root="", gateway_url=gw_url) as conn:
        manifest1 = conn.build_manifest()
        assert {m.filename: m.checksum for m in manifest1} == {
            "doc1.md": "v1",
            "doc2.md": "v1",
        }

    # Scenario 2: Revision changed for doc1.md, doc2.md was deleted
    respx.get(f"{gw_url}/files").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"path": "doc1.md", "size": 15, "revision": "v2"},
            ],
        )
    )
    with LiveSyncConnector(root="", gateway_url=gw_url) as conn:
        manifest2 = conn.build_manifest()
        assert len(manifest2) == 1
        assert manifest2[0].filename == "doc1.md"
        assert manifest2[0].checksum == "v2"

    # Verify Open WebUI diff API would receive these checksums
    client.sync_diff.return_value = {
        "added": [],
        "modified": [manifest2[0].to_dict()],
        "deleted": [{"filename": "doc2.md", "path": "", "id": "file-doc2"}],
        "unmodified_count": 0,
    }
    with LiveSyncConnector(root="", gateway_url=gw_url) as conn:
        result = run_sync(client=client, connector=conn, kb_id="kb-123", dry_run=True)
        assert result.modified == 1
        assert result.deleted == 1


# ── 10. CLI & Config Resolver Support ───────────────────────────────


def test_resolve_connector_livesync(monkeypatch):
    monkeypatch.setenv("LIVESYNC_GATEWAY_URL", "http://gw-test")
    connector = _resolve_connector(
        "livesync:Knowledge/software-engineering",
        auth={"token": "bearer-123"},
    )
    try:
        assert isinstance(connector, LiveSyncConnector)
        assert connector.root == "Knowledge/software-engineering"
        assert connector.token == "bearer-123"
        assert connector.gateway_url == "http://gw-test"
    finally:
        connector.close()


def test_resolve_connector_livesync_path_override(monkeypatch):
    monkeypatch.setenv("LIVESYNC_GATEWAY_URL", "http://gw-test")
    connector = _resolve_connector(
        "livesync:Knowledge/software-engineering",
        path="Overridden/Path",
    )
    try:
        assert connector.root == "Overridden/Path"
    finally:
        connector.close()


# ── 11. Milestone Test: CLI diff against mocked gateway ─────────────


@respx.mock
def test_cli_diff_milestone(monkeypatch):
    gw_url = "http://livesync-gateway"
    webui_url = "http://openwebui-api"
    kb_id = "test-kb-id"

    monkeypatch.setenv("LIVESYNC_GATEWAY_URL", gw_url)
    monkeypatch.setenv("LIVESYNC_GATEWAY_TOKEN", "gw-token")
    monkeypatch.setenv("OPEN_WEBUI_URL", webui_url)
    monkeypatch.setenv("OPEN_WEBUI_API_KEY", "sk-webui-token")

    # Mock LiveSync gateway
    respx.get(f"{gw_url}/files").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "path": "domain-reference/open-webui.md",
                    "size": 1234,
                    "revision": "rev-1",
                },
                {
                    "path": "guide.md",
                    "size": 800,
                    "revision": "rev-2",
                },
            ],
        )
    )

    # Mock Open WebUI diff endpoint
    respx.post(f"{webui_url}/api/v1/knowledge/{kb_id}/sync/diff").mock(
        return_value=httpx.Response(
            200,
            json={
                "added": [
                    {
                        "filename": "open-webui.md",
                        "path": "domain-reference",
                        "checksum": "rev-1",
                        "size": 1234,
                    }
                ],
                "modified": [
                    {
                        "filename": "guide.md",
                        "path": "",
                        "checksum": "rev-2",
                        "size": 800,
                    }
                ],
                "deleted": [],
                "unmodified_count": 0,
                "mkdir": ["domain-reference"],
                "rmdir": [],
            },
        )
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "diff",
            "livesync:Knowledge/software-engineering",
            "--kb-id",
            kb_id,
        ],
    )

    assert result.exit_code == 0, result.output
    assert "1 added" in result.output
    assert "1 modified" in result.output


# ── 12. Full Sync & Incremental Behavior ────────────────────────────


@respx.mock
def test_full_sync_and_incremental_diff():
    gw_url = "http://livesync-gateway"
    webui_url = "http://openwebui-api"
    kb_id = "test-kb-id"

    # Gateway serves 2 files
    respx.get(f"{gw_url}/files").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "path": "domain-reference/open-webui.md",
                    "size": 12,
                    "revision": "rev-1",
                },
                {
                    "path": "readme.md",
                    "size": 6,
                    "revision": "rev-2",
                },
            ],
        )
    )
    respx.get(f"{gw_url}/files/domain-reference/open-webui.md").mock(
        return_value=httpx.Response(200, content=b"docs content")
    )
    respx.get(f"{gw_url}/files/readme.md").mock(
        return_value=httpx.Response(200, content=b"readme")
    )

    # First sync: both files are added
    respx.post(f"{webui_url}/api/v1/knowledge/{kb_id}/sync/diff").mock(
        return_value=httpx.Response(
            200,
            json={
                "added": [
                    {
                        "filename": "open-webui.md",
                        "path": "domain-reference",
                        "checksum": "rev-1",
                        "size": 12,
                    },
                    {
                        "filename": "readme.md",
                        "path": "",
                        "checksum": "rev-2",
                        "size": 6,
                    },
                ],
                "modified": [],
                "deleted": [],
                "unmodified_count": 0,
                "mkdir": ["domain-reference"],
                "rmdir": [],
                "directory_map": {"domain-reference": "dir-1"},
            },
        )
    )
    respx.post(f"{webui_url}/api/v1/knowledge/{kb_id}/dirs/create").mock(
        return_value=httpx.Response(200, json={"id": "dir-1", "name": "domain-reference"})
    )
    upload_route = respx.post(f"{webui_url}/api/v1/files/").mock(
        return_value=httpx.Response(200, json={"id": "uploaded-file-id"})
    )

    client = OikbClient(base_url=webui_url, token="test-token")
    conn1 = LiveSyncConnector(
        root="Knowledge/software-engineering", gateway_url=gw_url
    )
    res1 = run_sync(client=client, connector=conn1, kb_id=kb_id)

    assert res1.added == 2
    assert upload_route.call_count == 2

    # Second sync: no changes in gateway, Open WebUI reports all unmodified
    respx.post(f"{webui_url}/api/v1/knowledge/{kb_id}/sync/diff").mock(
        return_value=httpx.Response(
            200,
            json={
                "added": [],
                "modified": [],
                "deleted": [],
                "unmodified_count": 2,
                "mkdir": [],
                "rmdir": [],
            },
        )
    )

    conn2 = LiveSyncConnector(
        root="Knowledge/software-engineering", gateway_url=gw_url
    )
    res2 = run_sync(client=client, connector=conn2, kb_id=kb_id)

    assert res2.added == 0
    assert res2.modified == 0
    assert res2.deleted == 0
    assert res2.unmodified == 2
    # No new file uploads occurred on second sync!
    assert upload_route.call_count == 2
    client.close()


# ── 13. Folder Filtering ────────────────────────────────────────────


@respx.mock
def test_folder_filtering():
    gw_url = "http://livesync-gateway"
    respx.get(f"{gw_url}/files").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"path": "docs/architecture.md", "size": 100, "revision": "1"},
                {"path": "docs/image.png", "size": 2000, "revision": "2"},
                {"path": "notes.txt", "size": 50, "revision": "3"},
            ],
        )
    )

    with LiveSyncConnector(root="", gateway_url=gw_url) as conn:
        manifest = conn.build_manifest()
        filter_func = build_manifest_filter(include=["**/*.md"])
        filtered = filter_func(manifest)
        assert len(filtered) == 1
        assert filtered[0].display_path == "docs/architecture.md"
