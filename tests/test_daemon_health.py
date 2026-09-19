import asyncio
import time
from unittest.mock import Mock

from fastapi.testclient import TestClient

from oikb import daemon


def test_readiness_tracks_scheduler_and_source_health(monkeypatch):
    monkeypatch.setattr(daemon, '_entries', [{'source': 'livesync:Atlas/Productivity'}])
    monkeypatch.setattr(daemon, '_scheduler_state', {})
    monkeypatch.setattr(daemon, '_shutdown_event', asyncio.Event())
    task = Mock()
    task.done.return_value = False
    monkeypatch.setattr(daemon.app.state, 'scheduler_task', task, raising=False)
    with TestClient(daemon.app) as client:
        assert client.get('/livez').status_code == 200
        assert client.get('/health/ready').status_code == 503
        state = {'status': 'success', 'last_success': time.time(), 'next_sync_at': time.time()+300}
        daemon._scheduler_state['livesync:Atlas/Productivity'] = state
        assert client.get('/health/ready').status_code == 200
        state['status'] = 'partial'
        assert client.get('/health').status_code == 503
        assert client.get('/livez').status_code == 200
        state.update(status='success', next_sync_at=time.time()-2000)
        assert client.get('/health/ready').status_code == 503
        state.update(status='running', started_at=time.time()-2000)
        assert client.get('/health/ready').status_code == 503
        state.update(status='success', next_sync_at=time.time()+300)
        task.done.return_value = True
        assert client.get('/health/ready').status_code == 503


def test_live_stays_healthy_without_sources(monkeypatch):
    monkeypatch.setattr(daemon, '_entries', [])
    with TestClient(daemon.app) as client:
        assert client.get('/health/ready').status_code == 503
        assert client.get('/livez').status_code == 200
