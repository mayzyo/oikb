"""HTTP client wrapping the Open WebUI Knowledge Base sync API."""

from __future__ import annotations

import json
import time
from typing import Any

import httpx


class OikbClient:
    """Stateless HTTP client for the Open WebUI KB API.

    All methods are synchronous — httpx handles connection pooling internally.
    """

    def __init__(self, base_url: str, token: str, timeout: float = 120.0,
                 processing_timeout: float = 300.0, poll_interval: float = 1.0):
        self._processing_timeout = processing_timeout
        self._poll_interval = poll_interval
        self._base_url = base_url.rstrip("/")
        self._http = httpx.Client(
            base_url=f"{self._base_url}/api/v1",
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
        )

    def __enter__(self) -> OikbClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    # ── Sync API ────────────────────────────────────────────────

    def sync_diff(
        self,
        kb_id: str,
        manifest: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """POST /knowledge/{id}/sync/diff — compute diff from manifest."""
        resp = self._http.post(
            f"/knowledge/{kb_id}/sync/diff",
            json={"manifest": manifest},
        )
        resp.raise_for_status()
        return resp.json()

    def sync_cleanup(
        self,
        kb_id: str,
        file_ids: list[str],
        dir_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """POST /knowledge/{id}/sync/cleanup — remove stale files and dirs."""
        payload: dict[str, Any] = {"file_ids": file_ids}
        if dir_ids:
            payload["dir_ids"] = dir_ids
        resp = self._http.post(
            f"/knowledge/{kb_id}/sync/cleanup",
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()

    # ── File upload ─────────────────────────────────────────────

    def upload_file(
        self,
        file_content: bytes,
        filename: str,
        kb_id: str,
        file_hash: str,
        directory_id: str | None = None,
    ) -> dict[str, Any]:
        """POST /files/ — upload a single file to the KB."""

        metadata: dict[str, Any] = {
            "knowledge_id": kb_id,
            "file_hash": file_hash,
        }
        if directory_id:
            metadata["directory_id"] = directory_id

        resp = self._http.post(
            "/files/",
            files={"file": (filename, file_content)},
            data={"metadata": json.dumps(metadata)},
        )
        resp.raise_for_status()
        uploaded = resp.json()
        file_id = uploaded.get("id")
        if not file_id:
            raise RuntimeError("Upload response did not contain a file ID")
        try:
            self.wait_for_processing(file_id, kb_id, filename)
        except Exception as exc:
            # Once accepted, retrying POST can create duplicate files. Leave
            # recovery to the next run instead of retrying an accepted upload.
            raise RuntimeError(f"Upload {file_id} accepted but not confirmed: {exc}") from exc
        return uploaded

    def wait_for_processing(self, file_id: str, kb_id: str, filename: str = "") -> None:
        """Wait for indexing AND the durable KB link before replacing old data."""
        deadline = time.monotonic() + self._processing_timeout
        while time.monotonic() < deadline:
            resp = self._http.get(f"/files/{file_id}/process/status")
            resp.raise_for_status()
            state = resp.json()
            if state.get("status") == "failed":
                raise RuntimeError(f"File processing failed: {state.get('error', file_id)}")
            if state.get("status") == "completed":
                # The diff collapses duplicate filenames; use the paginated
                # file list to verify the exact new ID, including replacements.
                page, seen = 1, 0
                while time.monotonic() < deadline:
                    linked = self._http.get(f"/knowledge/{kb_id}/files", params={"query": filename, "page": page})
                    linked.raise_for_status()
                    data = linked.json()
                    items = data["items"]
                    if any(item["id"] == file_id for item in items):
                        return
                    seen += len(items)
                    if not items or seen >= data["total"]:
                        break
                    page += 1
            time.sleep(self._poll_interval)
        raise TimeoutError(f"Timed out waiting for indexing/KB linking of {file_id}; old file retained")

    def cleanup_replacement(self, kb_id: str, old_id: str, new_id: str) -> None:
        """Protect against Open WebUI's hash-wide vector cleanup."""
        hashes = []
        for file_id in (old_id, new_id):
            resp = self._http.get(f"/files/{file_id}")
            resp.raise_for_status()
            hashes.append(resp.json().get("hash"))
        if not all(hashes) or hashes[0] == hashes[1]:
            raise RuntimeError("Replacement has missing or identical indexed content hash; preserving old file to avoid deleting shared vectors")
        try:
            self.sync_cleanup(kb_id, [old_id])
        except Exception as exc:
            raise RuntimeError(f"Replacement indexed but stale-file cleanup failed: {exc}") from exc

    # ── Directory management ────────────────────────────────────

    def create_directory(
        self,
        kb_id: str,
        name: str,
        parent_id: str | None = None,
    ) -> dict[str, Any]:
        """POST /knowledge/{id}/dirs/create — create a directory."""
        payload: dict[str, Any] = {"name": name}
        if parent_id:
            payload["parent_id"] = parent_id
        resp = self._http.post(
            f"/knowledge/{kb_id}/dirs/create",
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()

    # ── KB management ───────────────────────────────────────────

    def reset_kb(
        self,
        kb_id: str,
        include_directories: bool = True,
    ) -> dict[str, Any]:
        """POST /knowledge/{id}/reset — reset the KB."""
        resp = self._http.post(
            f"/knowledge/{kb_id}/reset",
            params={"include_directories": include_directories},
        )
        resp.raise_for_status()
        return resp.json()

    def get_kb(self, kb_id: str) -> dict[str, Any]:
        """GET /knowledge/{id} — get KB metadata.

        Note: this endpoint returns metadata only. Its ``files`` field is a
        server-hydrated convenience that some Open WebUI versions return as
        null; use ``list_kb_files``/``count_kb_files`` for the file list.
        """
        resp = self._http.get(f"/knowledge/{kb_id}")
        resp.raise_for_status()
        return resp.json()

    def list_kb_files(self, kb_id: str) -> list[dict[str, Any]]:
        """GET /knowledge/{id}/files — list files in a KB."""
        resp = self._http.get(f"/knowledge/{kb_id}/files")
        resp.raise_for_status()
        data = resp.json()
        return data.get("items", [])

    def count_kb_files(self, kb_id: str) -> int:
        """GET /knowledge/{id}/files — total file count for a KB."""
        resp = self._http.get(f"/knowledge/{kb_id}/files")
        resp.raise_for_status()
        return resp.json().get("total", 0)
