import json

import pytest

from oikb.cli import _resolve_connector
from oikb.connectors.livesync import LiveSyncConnector


def policy_file(tmp_path, monkeypatch, scopes):
    path = tmp_path / 'scopes.json'
    path.write_text(json.dumps({'scopes': scopes}), encoding='utf-8')
    monkeypatch.setenv('LIVESYNC_SCOPES_FILE', str(path))
    monkeypatch.setenv('LIVESYNC_GATEWAY_URL', 'http://gateway')
    return path


def test_native_connector_resolves_shared_scope(tmp_path, monkeypatch):
    policy_file(tmp_path, monkeypatch, [
        {'name':'work', 'path':'Atlas/Productivity', 'operations':['list','read']},
        {'name':'driving', 'path':'Efforts/Driving', 'operations':['list','read']},
        # Gateway-only scope for another service: no OIKB mapping required.
        {'name':'automation', 'path':'Integrations/Automation', 'operations':['write']},
    ])
    with _resolve_connector('livesync:?scope=work') as connector:
        assert connector.root == 'Atlas/Productivity'
    with _resolve_connector('livesync:?scope=driving') as connector:
        assert connector.root == 'Efforts/Driving'


@pytest.mark.parametrize('scopes', [
    [],
    [{'name':'other', 'path':'Atlas/Productivity', 'operations':['list','read']}],
    [{'name':'work', 'path':'Atlas/Productivity', 'operations':['read']}],
    [{'name':'work', 'path':'../Other', 'operations':['list','read']}],
    [{'name':'work', 'path':'/Atlas', 'operations':['list','read']}],
    [{'name':'work', 'path':'Atlas', 'operations':['list','read']}, {'name':'work', 'path':'Efforts', 'operations':['list','read']}],
])
def test_invalid_scope_never_falls_back_to_empty_root(tmp_path, monkeypatch, scopes):
    policy_file(tmp_path, monkeypatch, scopes)
    with pytest.raises(ValueError):
        _resolve_connector('livesync:?scope=work')


def test_missing_scope_file_and_path_override_fail(tmp_path, monkeypatch):
    path = policy_file(tmp_path, monkeypatch, [{'name':'work','path':'Atlas/Productivity','operations':['list','read']}])
    with pytest.raises(ValueError, match='either'):
        LiveSyncConnector(root='Other', scope='work')
    path.unlink()
    with pytest.raises(FileNotFoundError):
        _resolve_connector('livesync:?scope=work')
