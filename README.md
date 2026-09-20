# oikb

Sync local files, repositories, cloud storage and LiveSync notes into Open WebUI
Knowledge Bases. Requires Open WebUI 0.9.6+.

## Quick start

Install this checkout and preview a sync:

```sh
pip install .
export OPEN_WEBUI_URL=http://localhost:3000
export OPEN_WEBUI_API_KEY=your-api-key
oikb sync ./docs --kb-id your-kb-id --dry-run
```

Remove `--dry-run` to sync. Use `oikb --help` or the [full guide](docs/guide.md)
for other connectors, authentication, filters, webhooks and deployment.

## LiveSync

```sh
export LIVESYNC_GATEWAY_URL=http://livesync-api-gateway:9000
export LIVESYNC_GATEWAY_TOKEN=your-gateway-key
```

Create `.oikb.yaml`, then run `oikb daemon`:

```yaml
defaults:
  interval: 5m
sources:
  - name: productivity
    source: livesync:Atlas/Productivity
    kb-id: your-kb-id
```

Add entries for other folders/KBs. Named scopes use `livesync:?scope=productivity`
and `LIVESYNC_SCOPES_FILE`, pointing to the shared gateway scope catalogue.
Scopes must allow `list` and `read`; KB names need not match scope names.
Set `OIKB_API_KEY` to protect daemon control endpoints.

### Behavior and limits

- SHA-256 hashes are cached by revision; unchanged notes avoid repeat downloads.
  Cache loss causes a fresh scan. Override its temporary-directory location with
  `LIVESYNC_HASH_CACHE_DIR`.
- Downloads stream to temporary disk snapshots. Defaults: 32 MiB/file and
  256 MiB/scan; override connector options `max_file_bytes` and
  `max_snapshot_bytes`. Uploads still hold a selected file in RAM.
- OIKB filters and cancellation apply **after scanning**. Excluded files may
  still download or block a scan. Network waits use the request timeout.
- Revision/hash checks reject source changes during reads. Existing KB notes
  with missing/legacy hashes, or identical content at different paths, can
  still trigger duplicate-content errors; there is no automatic reconciliation.

## Safety and health

Use dedicated mirror KBs: source deletions remove destination files, and an
empty source clears its mirror. Replacements require confirmed uploads;
partial runs block unrelated cleanup. Back up KB-only content before clearing
a KB; it cannot be restored from LiveSync.

Use `/livez` for startup/liveness and `/health/ready` for readiness.
Readiness can return 503 while the process runs if syncs are partial, failing,
overdue, or have never succeeded. `/health` shows per-source details.

## Tests

```sh
uv run --extra dev pytest -q
```

The optional gateway contract test requires a **disposable** gateway and
`OIKB_TEST_GATEWAY_URL`, `OIKB_TEST_GATEWAY_TOKEN`, and `LIVESYNC_SCOPES_FILE`.

[MIT license](LICENSE).
