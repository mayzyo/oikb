"""LiveSync connector — sync files from a LiveSync API Gateway to an Open WebUI Knowledge Base.

Auth via LIVESYNC_GATEWAY_TOKEN env var (Bearer token).
Gateway URL via LIVESYNC_GATEWAY_URL env var or connector config.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import parse_qsl, quote

import httpx

from oikb.connectors import BaseConnector, ManifestEntry, SourceFileUnavailable


class LiveSyncConnector(BaseConnector):
    """Sync files from a LiveSync API gateway.

    Args:
        root:        Root folder or prefix in LiveSync (e.g. "Knowledge/software-engineering").
        gateway_url: Base URL of the LiveSync API gateway (or LIVESYNC_GATEWAY_URL env var).
        token:       Bearer token for gateway auth (or LIVESYNC_GATEWAY_TOKEN env var).
        timeout:     HTTP request timeout in seconds (default: 30.0).
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
        timeout: float = 30.0,
        client: httpx.Client | None = None,
        **kwargs: Any,
    ):
        raw_root = path if path is not None else root
        self.root = (raw_root or "").strip().strip("/")

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
        params: dict[str, str] = {}
        if self.root:
            params["prefix"] = self.root

        resp = self._http.get("/files", params=params)
        resp.raise_for_status()

        data = resp.json()
        if isinstance(data, list):
            raw_entries = data
        elif isinstance(data, dict):
            raw_entries = data.get("files", data.get("results", []))
        else:
            raise ValueError(
                f"Unexpected LiveSync gateway response format: {type(data).__name__}"
            )

        # Deduplicate paths (last seen entry wins)
        entries_by_path: dict[str, ManifestEntry] = {}

        for item in raw_entries:
            if not isinstance(item, dict):
                continue

            raw_path = str(item.get("path") or item.get("name") or "").strip()
            if not raw_path:
                continue

            norm_path = raw_path.strip("/")
            norm_root = self.root

            if norm_root:
                if norm_path == norm_root:
                    continue
                if norm_path.startswith(f"{norm_root}/"):
                    rel_path = norm_path[len(norm_root) + 1 :]
                else:
                    rel_path = norm_path
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

            entry = ManifestEntry(
                filename=filename,
                path=dir_path,
                checksum=revision,
                size=size,
            )
            display = entry.display_path
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

        url_path = f"/files/{quote(remote_path.lstrip('/'), safe='/')}"
        try:
            resp = self._http.get(url_path)
            if resp.status_code == 404 and self.root:
                # Fallback: check alternate path if root was omitted or duplicated
                if not remote_path.startswith(f"{self.root}/"):
                    alt_path = f"{self.root}/{remote_path}"
                    alt_resp = self._http.get(
                        f"/files/{quote(alt_path.lstrip('/'), safe='/')}"
                    )
                    if alt_resp.status_code != 404:
                        resp = alt_resp
                elif remote_path.startswith(f"{self.root}/"):
                    alt_path = remote_path[len(self.root) + 1 :]
                    alt_resp = self._http.get(
                        f"/files/{quote(alt_path.lstrip('/'), safe='/')}"
                    )
                    if alt_resp.status_code != 404:
                        resp = alt_resp

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
