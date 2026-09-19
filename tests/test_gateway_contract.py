"""Opt-in cross-repo contract against a disposable real gateway/CLI container."""
import os
import time
import uuid

import httpx
import pytest

from oikb.connectors.livesync import LiveSyncConnector


@pytest.mark.skipif(not os.getenv('OIKB_TEST_GATEWAY_URL'), reason='requires disposable gateway container')
def test_real_gateway_multiple_mirrors():
    url = os.environ['OIKB_TEST_GATEWAY_URL']
    token = os.environ['OIKB_TEST_GATEWAY_TOKEN']
    with httpx.Client(base_url=url, headers={'Authorization': f'Bearer {token}'}, timeout=120) as api:
        for attempt in range(60):
            try:
                if api.get('/livez').is_success:
                    break
            except httpx.TransportError:
                pass
            time.sleep(1)
        else:
            pytest.fail('gateway did not become live')
        assert api.post('/hooks/sync', json={}).status_code == 403
        for scope in ('productivity', 'agreements', 'driving'):
            with LiveSyncConnector(scope=scope, gateway_url=url, token=token) as connector:
                root = connector.root
            relative_prefix = 'oikb-contract-' + uuid.uuid4().hex
            prefix = root + '/' + relative_prefix
            files = [prefix+'/note.md', prefix+'/nested/note.md']
            try:
                for path in files:
                    api.post('/hooks/livesync-write', json={'path': path, 'content': '# Contract\n'}).raise_for_status()
                with LiveSyncConnector(scope=scope, gateway_url=url, token=token) as connector:
                    manifest = connector.build_manifest()
                    assert {entry.display_path for entry in manifest} == {relative_prefix+'/note.md', relative_prefix+'/nested/note.md'}
                    for entry in manifest:
                        assert entry.checksum and entry.size == 11
                        assert connector.read_file(entry.path, entry.filename) == b'# Contract\n'
                    api.post('/hooks/livesync-write', json={'path': files[0], 'content': ''}).raise_for_status()
                    assert connector.read_file(relative_prefix, 'note.md') == b''
            finally:
                for path in files:
                    api.post('/hooks/livesync-delete', json={'path': path}).raise_for_status()
            with LiveSyncConnector(scope=scope, gateway_url=url, token=token) as connector:
                assert connector.build_manifest() == []
