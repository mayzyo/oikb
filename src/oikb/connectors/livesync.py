"""LiveSync connector — sync files from a LiveSync API Gateway to an Open WebUI Knowledge Base.

Auth via LIVESYNC_GATEWAY_TOKEN env var (Bearer token).
Gateway URL via LIVESYNC_GATEWAY_URL env var or connector config.
"""

from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

import httpx

from oikb.connectors import BaseConnector, ManifestEntry, SourceFileUnavailable


class LiveSyncConnector(BaseConnector):
    """Sync files from a LiveSync API gateway.

    Args:
        root:        Root folder or prefix in LiveSync (e.g. "Knowledge/software-engineering").
        gateway_url: Base URL of the LiveSync API gateway (or LIVESYNC_GATEWAY_URL env var).
        token:       Bearer token for gateway auth (or LIVESYNC_GATEWAY_TOKEN env var).
        timeout:     HTTP request timeout in seconds (default: 120.0).
        client:      Optional existing httpx.Client instance (for testing).
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
        # Mapping from (dir_path, filename) and display_path to remote gateway path
        self._file_paths: dict[tuple[str, str], str] = {}

    def build_manifest(self) -> list[ManifestEntry]:
        """Scan the LiveSync gateway and return a manifest of all files under root."""
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
        self._file_paths.clear()

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
            self._file_paths[(dir_path, filename)] = raw_path
            self._file_paths[(display, "")] = raw_path

        entries = sorted(entries_by_path.values(), key=lambda e: e.display_path)
        return entries

    def read_file(self, path: str, filename: str) -> bytes:
        """Read raw file content from the LiveSync gateway.

        Args:
            path:     Directory path relative to source root.
            filename: Basename of the file.

        Returns:
            Raw bytes of the file content.
        """
        key = (path, filename)
        display = f"{path}/{filename}" if path else filename

        if key in self._file_paths:
            remote_path = self._file_paths[key]
        elif (display, "") in self._file_paths:
            remote_path = self._file_paths[(display, "")]
        else:
            rel = display
            if self.root:
                remote_path = f"{self.root}/{rel}"
            else:
                remote_path = rel

        def read(remote_file_path: str) -> httpx.Response:
            return self._http.post(
                "/hooks/livesync-read",
                json={"path": remote_file_path.lstrip("/")},
            )

        try:
            resp = read(remote_path)
            if resp.status_code == 404:
                raise SourceFileUnavailable(
                    f"File '{display}' not found on LiveSync gateway (path: {remote_path})"
                )
            resp.raise_for_status()
            return resp.content
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise SourceFileUnavailable(
                    f"File '{display}' not found on LiveSync gateway: {exc}"
                ) from exc
            raise

    def close(self) -> None:
        """Release HTTP client if owned."""
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
