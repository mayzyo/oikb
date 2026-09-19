from unittest.mock import Mock

import httpx
import pytest
import respx

from oikb.client import OikbClient
from oikb.connectors import ManifestEntry, SourceFileUnavailable
from oikb.connectors.livesync import LiveSyncConnector
from oikb.sync import run_sync


def source(entries=None, content=b'new'):
    connector = Mock()
    connector.build_manifest.return_value = entries if entries is not None else [
        ManifestEntry(filename='note.md', path='', checksum='rev2', size=len(content))
    ]
    connector.read_file.return_value = content
    return connector


def client_for_replacement():
    client = Mock()
    client.upload_file.return_value = {'id': 'new'}
    client.sync_diff.return_value = {
        'modified': [{'filename': 'note.md', 'path': '', 'stale_file_id': 'old'}],
        'deleted': [{'filename': 'gone.md', 'file_id': 'gone'}],
        'rmdir': ['gone-dir'],
    }
    return client


@pytest.mark.parametrize('failure', ['read', 'upload', 'missing'])
def test_failed_replacement_never_deletes_old_files(failure):
    connector, client = source(), client_for_replacement()
    if failure == 'read':
        connector.read_file.side_effect = RuntimeError('read failed')
    elif failure == 'missing':
        connector.read_file.side_effect = SourceFileUnavailable('vanished')
    else:
        client.upload_file.side_effect = RuntimeError('indexing failed')
    result = run_sync(client, connector, 'kb', quiet=True)
    assert result.errors or result.warnings
    client.sync_cleanup.assert_not_called()
    client.cleanup_replacement.assert_not_called()
    assert result.modified == result.deleted == 0


def test_successful_replacement_is_uploaded_before_cleanup():
    client = client_for_replacement()
    result = run_sync(client, source(), 'kb', quiet=True)
    calls = [c[0] for c in client.mock_calls]
    assert calls.index('upload_file') < calls.index('cleanup_replacement') < calls.index('sync_cleanup')
    client.cleanup_replacement.assert_called_once_with('kb', 'old', 'new')
    client.sync_cleanup.assert_called_once_with('kb', ['gone'], ['gone-dir'])
    assert result.modified == result.deleted == 1


def test_empty_source_removes_last_mirrored_file():
    client = Mock()
    client.sync_diff.return_value = {'deleted': [{'file_id': 'last', 'filename': 'last.md'}]}
    result = run_sync(client, source([]), 'kb', quiet=True)
    client.sync_diff.assert_called_once_with('kb', [])
    client.sync_cleanup.assert_called_once_with('kb', ['last'], None)
    assert result.deleted == 1


def test_empty_file_is_submitted_not_silently_skipped():
    client = client_for_replacement()
    result = run_sync(client, source(content=b''), 'kb', quiet=True)
    assert client.upload_file.call_args.kwargs['file_content'] == b''
    assert result.modified == 1


def test_cleanup_failure_does_not_reupload_confirmed_replacement():
    client = client_for_replacement()
    response = httpx.Response(503, request=httpx.Request('GET', 'http://webui/files/old'))
    client.cleanup_replacement.side_effect = httpx.HTTPStatusError('unavailable', request=response.request, response=response)
    result = run_sync(client, source(), 'kb', quiet=True)
    assert result.errors
    assert client.upload_file.call_count == 1
    client.sync_cleanup.assert_not_called()


@respx.mock
def test_upload_waits_for_processing_and_kb_link():
    respx.post('http://webui/api/v1/files/').respond(200, json={'id': 'new'})
    status = respx.get('http://webui/api/v1/files/new/process/status').mock(side_effect=[
        httpx.Response(200, json={'status': 'pending'}),
        httpx.Response(200, json={'status': 'completed'}),
        httpx.Response(200, json={'status': 'completed'}),
    ])
    links = respx.get('http://webui/api/v1/knowledge/kb/files').mock(side_effect=[
        httpx.Response(200, json={'items': [], 'total': 0}),
        httpx.Response(200, json={'items': [{'id': 'new'}], 'total': 1}),
    ])
    with OikbClient('http://webui', 'token', poll_interval=0) as client:
        assert client.upload_file(b'data', 'note.md', 'kb', 'hash')['id'] == 'new'
    assert status.call_count == 3 and links.call_count == 2


@respx.mock
def test_processing_failure_is_not_success():
    respx.post('http://webui/api/v1/files/').respond(200, json={'id': 'new'})
    respx.get('http://webui/api/v1/files/new/process/status').respond(200, json={'status': 'failed', 'error': 'embedding unavailable'})
    with OikbClient('http://webui', 'token', poll_interval=0) as client:
        with pytest.raises(RuntimeError, match='processing failed'):
            client.upload_file(b'data', 'note.md', 'kb', 'hash')


@respx.mock
def test_processing_timeout_preserves_failure():
    respx.post('http://webui/api/v1/files/').respond(200, json={'id': 'new'})
    with OikbClient('http://webui', 'token', processing_timeout=0) as client:
        with pytest.raises(RuntimeError, match='Timed out'):
            client.upload_file(b'data', 'note.md', 'kb', 'hash')


@pytest.mark.parametrize('payload', [
    {}, [], {'files': []}, {'status': 'success', 'count': 1, 'files': []},
    {'status': 'success', 'count': 1, 'files': [{'path': 'Atlas/Productivity-private/a.md', 'revision': '1', 'size': 1}]},
    {'status': 'success', 'count': 1, 'files': [{'path': 'Atlas/Productivity/a.md', 'revision': '1', 'size': 1, 'has_conflict': True}]},
])
@respx.mock
def test_bad_or_conflicted_manifest_fails_closed(payload):
    respx.post('http://gateway/hooks/livesync-list').respond(200, json=payload)
    with LiveSyncConnector(root='Atlas/Productivity', gateway_url='http://gateway') as connector:
        with pytest.raises(ValueError):
            connector.build_manifest()


@respx.mock
def test_missing_note_never_falls_back_to_another_root():
    route = respx.post('http://gateway/hooks/livesync-read').respond(404)
    with LiveSyncConnector(root='Atlas/Productivity', gateway_url='http://gateway') as connector:
        with pytest.raises(SourceFileUnavailable):
            connector.read_file('', 'note.md')
    assert route.call_count == 1


@respx.mock
def test_replacement_cleanup_guards_shared_hash_vectors():
    respx.get('http://webui/api/v1/files/old').respond(200, json={'hash': 'same'})
    new = respx.get('http://webui/api/v1/files/new').respond(200, json={'hash': 'same'})
    cleanup = respx.post('http://webui/api/v1/knowledge/kb/sync/cleanup').respond(200, json={'status': True})
    with OikbClient('http://webui', 'token') as client:
        with pytest.raises(RuntimeError, match='identical'):
            client.cleanup_replacement('kb', 'old', 'new')
        assert not cleanup.called
        new.respond(200, json={'hash': 'changed'})
        client.cleanup_replacement('kb', 'old', 'new')
        assert cleanup.call_count == 1
