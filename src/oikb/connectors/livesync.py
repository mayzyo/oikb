"""LiveSync connector — sync files from a LiveSync API Gateway to an Open WebUI Knowledge Base.

Auth via LIVESYNC_GATEWAY_TOKEN env var (Bearer token).
Gateway URL via LIVESYNC_GATEWAY_URL env var or connector config.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import replace
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory, gettempdir
from threading import Lock
from typing import Any
from urllib.parse import parse_qsl

import httpx

from oikb.connectors import BaseConnector, ManifestEntry, SourceFileUnavailable

_log = logging.getLogger(__name__)
_MAX_CACHE_BYTES = 8 * 1024 * 1024


class LiveSyncConnector(BaseConnector):
    """Sync files from a LiveSync API gateway.

    Args:
        root:        Root folder or prefix in LiveSync (e.g. "Knowledge/software-engineering").
        gateway_url: Base URL of the LiveSync API gateway (or LIVESYNC_GATEWAY_URL env var).
        token:       Bearer token for gateway auth (or LIVESYNC_GATEWAY_TOKEN env var).
        timeout:     HTTP request timeout in seconds (default: 120.0).
        client:      Optional existing httpx.Client instance (for testing).
        cache_dir:   Hash metadata cache directory (or LIVESYNC_HASH_CACHE_DIR).
        max_file_bytes: Maximum downloaded/uploaded file size (default: 32 MiB).
        max_snapshot_bytes: Temporary file budget per scan (default: 256 MiB).
    """

    def __init__(
        self,
        root: str = "",
        gateway_url: str | None = None,
        token: str | None = None,
        *,
        path: str | None = None,
        url: str | None = None,
        base_url: str | None = None,
        timeout: float = 120.0,
        client: httpx.Client | None = None,
        scope: str | None = None,
        scopes_file: str | None = None,
        cache_dir: str | None = None,
        max_file_bytes: int = 32 * 1024 * 1024,
        max_snapshot_bytes: int = 256 * 1024 * 1024,
        **kwargs: Any,
    ):
        raw_root = path if path is not None else root
        self.root = (raw_root or "").strip().strip("/")
        if scope is not None:
            if raw_root:
                raise ValueError("Use either a LiveSync root path or a named scope, not both")
            filename = scopes_file or os.environ.get("LIVESYNC_SCOPES_FILE")
            if not filename:
                raise ValueError("Named LiveSync scopes require LIVESYNC_SCOPES_FILE")
            policy = json.loads(Path(filename).read_text(encoding="utf-8"))
            entries = policy.get("scopes") if isinstance(policy, dict) else None
            if not isinstance(entries, list) or not entries:
                raise ValueError("LiveSync scope file must define a nonempty scopes list")
            names = [entry.get("name") for entry in entries if isinstance(entry, dict)]
            if len(names) != len(entries) or any(not isinstance(name, str) or not name for name in names) or len(set(names)) != len(names):
                raise ValueError("LiveSync scopes must have unique nonempty names")
            matches = [entry for entry in entries if entry["name"] == scope]
            if len(matches) != 1:
                raise ValueError(f"Unknown LiveSync scope: {scope}")
            selected = matches[0]
            root = selected.get("path")
            if not isinstance(root, str) or not root or "\\" in root or any(
                not part or part.startswith(".") for part in root.split("/")
            ) or any(ord(char) < 32 or ord(char) == 127 for char in root):
                raise ValueError("LiveSync scope path must be a canonical vault-relative path")
            operations = selected.get("operations", [])
            if not isinstance(operations, list) or "list" not in operations or "read" not in operations:
                raise ValueError(f"LiveSync scope {scope} must permit list and read for mirroring")
            self.root = root

        gw_url = (
            gateway_url
            or url
            or base_url
            or kwargs.get("gateway_url")
            or kwargs.get("url")
            or os.environ.get("LIVESYNC_GATEWAY_URL")
            or os.environ.get("LIVESYNC_URL")
        )
        if not gw_url:
            raise ValueError(
                "LiveSync gateway URL required. Set via:\n"
                "  export LIVESYNC_GATEWAY_URL=http://livesync-api-gateway\n"
                "or pass gateway_url in connector auth config."
            )
        self.gateway_url = str(gw_url).rstrip("/")

        self.token = (
            token
            or kwargs.get("gateway_token")
            or kwargs.get("token")
            or os.environ.get("LIVESYNC_GATEWAY_TOKEN")
            or os.environ.get("LIVESYNC_TOKEN")
        )

        headers: dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": "oikb/0.5 (+https://github.com/open-webui/oikb)",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        self._own_client = client is None
        self._http = client or httpx.Client(
            base_url=self.gateway_url,
            headers=headers,
            timeout=timeout,
        )
        self._snapshot_dir: TemporaryDirectory | None = None
        self._snapshots: dict[tuple[str, str], Path] = {}
        self._manifest: dict[tuple[str, str], ManifestEntry] = {}
        self._listed: dict[tuple[str, str], ManifestEntry] = {}
        self._snapshot_bytes = 0
        self._budget_lock = Lock()
        self._max_file_bytes = int(max_file_bytes)
        self._max_snapshot_bytes = int(max_snapshot_bytes)
        if min(self._max_file_bytes, self._max_snapshot_bytes) <= 0:
            raise ValueError("LiveSync download limits must be positive byte counts")
        cache_root = cache_dir or os.environ.get("LIVESYNC_HASH_CACHE_DIR") or str(
            Path(gettempdir()) / f"oikb-livesync-hashes-{getattr(os, 'getuid', lambda: 'user')()}"
        )
        # Separate endpoints, scopes/roots and credentials; never persist tokens.
        identity = json.dumps([self.gateway_url, self.root, self.token], ensure_ascii=True)
        self._cache_path = Path(cache_root) / (hashlib.sha256(identity.encode()).hexdigest() + ".json")

    def build_manifest(self) -> list[ManifestEntry]:
        """Download only revisions without a cached hash; OIKB filters later."""
        self._clear_snapshots()
        self._manifest.clear()
        self._listed.clear()
        listed = self._list_entries()
        cached = self._load_cache()
        updated = {}
        entries = []
        downloaded = False
        self._snapshot_dir = TemporaryDirectory(prefix="oikb-livesync-")
        try:
            for entry in listed:
                key = (entry.path, entry.filename)
                self._listed[key] = entry
                hit = cached.get(entry.display_path)
                if hit and hit["revision"] == entry.checksum and hit["listed_size"] == entry.size:
                    actual = replace(entry, checksum=hit["sha256"], size=hit["size"])
                    if actual.size > self._max_file_bytes:
                        raise SourceFileUnavailable(f"LiveSync file exceeds download limit: {entry.display_path}")
                else:
                    actual = self._download(entry)
                    downloaded = True
                entries.append(actual)
                updated[entry.display_path] = {
                    "revision": entry.checksum, "listed_size": entry.size,
                    "sha256": actual.checksum, "size": actual.size,
                }
            if downloaded:
                # List again using existing list/read permissions, not info.
                # Never cache bytes under a revision that changed mid-download.
                self._verify_revisions(self._listed)
            self._manifest = {(e.path, e.filename): e for e in entries}
            self._save_cache(updated)
            return entries
        except BaseException:
            self._manifest.clear()
            self._clear_snapshots()
            raise

    def _list_entries(self) -> list[ManifestEntry]:
        payload: dict[str, str] = {}
        if self.root:
            payload["path"] = self.root

        resp = self._http.post("/hooks/livesync-list", json=payload)
        resp.raise_for_status()

        data = resp.json()
        if not isinstance(data, dict) or data.get("status") != "success" or not isinstance(data.get("files"), list):
            raise ValueError(
                "Invalid LiveSync manifest response; refusing destructive sync"
            )
        raw_entries = data["files"]
        if data.get("count") != len(raw_entries):
            raise ValueError("Incomplete LiveSync manifest; refusing destructive sync")

        # Reject ambiguous manifests rather than deleting from an incomplete view.
        entries_by_path: dict[str, ManifestEntry] = {}

        for item in raw_entries:
            if not isinstance(item, dict):
                raise ValueError("Invalid LiveSync manifest entry")

            raw_path = str(item.get("path") or item.get("name") or "").strip()
            if not raw_path:
                raise ValueError("LiveSync entry has no path")
            if item.get("has_conflict"):
                raise ValueError(f"Resolve LiveSync conflict before mirroring: {raw_path}")
            if raw_path.startswith("/") or "\\" in raw_path or any(p in ("", ".", "..") for p in raw_path.split("/")):
                raise ValueError(f"Unsafe LiveSync path: {raw_path}")

            norm_path = raw_path.strip("/")
            norm_root = self.root

            if norm_root:
                if norm_path == norm_root:
                    raise ValueError(f"LiveSync root is a file: {norm_root}")
                if norm_path.startswith(f"{norm_root}/"):
                    rel_path = norm_path[len(norm_root) + 1 :]
                else:
                    raise ValueError(f"LiveSync entry outside configured root: {raw_path}")
            else:
                rel_path = norm_path

            if not rel_path:
                continue

            parts = rel_path.rsplit("/", 1)
            if len(parts) == 2:
                dir_path, filename = parts[0], parts[1]
            else:
                dir_path, filename = "", parts[0]

            size = int(item.get("size", 0))
            revision = str(
                item.get("revision")
                or item.get("checksum")
                or item.get("etag")
                or item.get("hash")
                or ""
            )
            if not revision or size < 0:
                raise ValueError(f"Invalid LiveSync metadata: {raw_path}")

            entry = ManifestEntry(
                filename=filename,
                path=dir_path,
                checksum=revision,
                size=size,
            )
            display = entry.display_path
            if display in entries_by_path:
                raise ValueError(f"Duplicate LiveSync path: {raw_path}")
            entries_by_path[display] = entry

        return sorted(entries_by_path.values(), key=lambda e: e.display_path)

    def _verify_revisions(self, expected: dict[tuple[str, str], ManifestEntry]) -> None:
        current = {(e.path, e.filename): e for e in self._list_entries()}
        for key, before in expected.items():
            if current.get(key) != before:
                raise SourceFileUnavailable(f"LiveSync source changed during scan: {before.display_path}; retry next sync")

    def _download(self, entry: ManifestEntry) -> ManifestEntry:
        """Stream one bounded file to a private snapshot and hash the same bytes."""
        if entry.size > self._max_file_bytes:
            raise SourceFileUnavailable(f"LiveSync file exceeds download limit: {entry.display_path}")
        if self._snapshot_dir is None:
            self._snapshot_dir = TemporaryDirectory(prefix="oikb-livesync-")
        remote_path = "/".join(p for p in (self.root, entry.display_path) if p)
        digest, size, reserved = hashlib.sha256(), 0, 0
        # Unique temp names also support concurrent upload reads after a warm scan.
        with NamedTemporaryFile(dir=self._snapshot_dir.name, delete=False) as output:
            snapshot = Path(output.name)
            try:
                with self._http.stream("POST", "/hooks/livesync-read", json={"path": remote_path}) as resp:
                    if resp.status_code == 404:
                        raise SourceFileUnavailable(f"File '{entry.display_path}' not found on LiveSync gateway")
                    if not resp.is_success:
                        # The existing upload error handler reads response.text.
                        # Preserve a bounded error body without buffering an
                        # unlimited streaming response or using private fields.
                        error_body = bytearray()
                        for chunk in resp.iter_bytes(chunk_size=4096):
                            error_body.extend(chunk)
                            if len(error_body) >= 16384:
                                break
                        httpx.Response(resp.status_code, content=bytes(error_body), request=resp.request).raise_for_status()
                    resp.raise_for_status()
                    for chunk in resp.iter_bytes(chunk_size=64 * 1024):
                        size += len(chunk)
                        if size > self._max_file_bytes:
                            raise SourceFileUnavailable(f"LiveSync file exceeds download limit: {entry.display_path}")
                        with self._budget_lock:
                            if self._snapshot_bytes + len(chunk) > self._max_snapshot_bytes:
                                raise SourceFileUnavailable("LiveSync snapshot disk budget exceeded; existing KB preserved")
                            self._snapshot_bytes += len(chunk)
                            reserved += len(chunk)
                        output.write(chunk)
                        digest.update(chunk)
            except BaseException:
                output.close()
                snapshot.unlink(missing_ok=True)
                with self._budget_lock:
                    self._snapshot_bytes -= reserved
                raise
        self._snapshots[(entry.path, entry.filename)] = snapshot
        return replace(entry, checksum=digest.hexdigest(), size=size)

    def _load_cache(self) -> dict[str, dict]:
        """Invalid/unavailable metadata is a cache miss, never an empty source."""
        try:
            with self._cache_path.open("rb") as source:
                raw = source.read(_MAX_CACHE_BYTES + 1)
            if len(raw) > _MAX_CACHE_BYTES:
                return {}
            data = json.loads(raw)
            if data.get("version") != 1 or not isinstance(data.get("files"), dict):
                return {}
            valid = {}
            for path, item in data["files"].items():
                if (isinstance(item, dict) and isinstance(item.get("revision"), str)
                    and type(item.get("size")) is int and item["size"] >= 0
                    and type(item.get("listed_size")) is int and item["listed_size"] >= 0
                    and isinstance(item.get("sha256"), str) and len(item["sha256"]) == 64
                    and all(c in "0123456789abcdef" for c in item["sha256"])):
                    valid[path] = item
            return valid
        except (OSError, ValueError, AttributeError, RecursionError):
            return {}

    def _save_cache(self, entries: dict[str, dict]) -> None:
        temporary = None
        try:
            payload = json.dumps({"version": 1, "files": entries}).encode()
            if len(payload) > _MAX_CACHE_BYTES:
                _log.warning("LiveSync hash cache exceeds metadata limit; skipping cache write")
                return
            self._cache_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with NamedTemporaryFile(dir=self._cache_path.parent, delete=False) as output:
                temporary = Path(output.name)
                output.write(payload)
            os.replace(temporary, self._cache_path)
        except OSError:
            _log.warning("LiveSync hash cache unavailable; future scans may re-download files")
        finally:
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    _log.warning("Could not remove temporary LiveSync hash cache file")

    def read_file(self, path: str, filename: str) -> bytes:
        """Read raw file content from the LiveSync gateway.

        Args:
            path:     Directory path relative to source root.
            filename: Basename of the file.

        Returns:
            Raw bytes of the file content.
        """
        key = (path, filename)
        if key in self._snapshots:
            return self._snapshots[key].read_bytes()
        expected = self._manifest.get(key)
        listed = self._listed.get(key)
        try:
            if expected and listed:
                self._verify_revisions({key: listed})
            actual = self._download(listed or ManifestEntry(filename, path, "", 0))
            if expected and listed:
                self._verify_revisions({key: listed})
                if actual.checksum != expected.checksum or actual.size != expected.size:
                    cached = self._load_cache()
                    cached.pop(expected.display_path, None)
                    self._save_cache(cached)
                    raise SourceFileUnavailable(f"LiveSync content changed since manifest: {expected.display_path}; retry next sync")
            return self._snapshots[key].read_bytes()
        except BaseException:
            snapshot = self._snapshots.pop(key, None)
            if snapshot is not None:
                snapshot.unlink(missing_ok=True)
            raise

    def _clear_snapshots(self) -> None:
        self._snapshots.clear()
        self._snapshot_bytes = 0
        if self._snapshot_dir is not None:
            self._snapshot_dir.cleanup()
            self._snapshot_dir = None

    def close(self) -> None:
        """Release this scan's temporary files and HTTP client if owned."""
        self._clear_snapshots()
        if self._own_client:
            self._http.close()


def parse_livesync_source(source: str) -> dict[str, Any]:
    """Parse a livesync:PATH source string.

    Examples:
        livesync:Knowledge/software-engineering
        livesync:docs/api
        livesync:
    """
    if not source.startswith("livesync:"):
        raise ValueError(
            f"Invalid LiveSync source: '{source}'. Expected format: livesync:<path>"
        )

    raw = source.removeprefix("livesync:")
    if "?" in raw:
        path_part, query_part = raw.split("?", 1)
        params = dict(parse_qsl(query_part, keep_blank_values=True))
    else:
        path_part, params = raw, {}

    root = path_part.strip().strip("/")
    return {
        "root": root,
        **params,
    }
