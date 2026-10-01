"""The Render Blueprint is a deployment decision, so its safety properties are pinned."""
import pathlib

import pytest
import yaml

import pump_worker

ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope='module')
def worker():
    blueprint = yaml.safe_load((ROOT / 'render.yaml').read_text())
    assert [service['name'] for service in blueprint['services']] == ['backend-track-pump-worker']
    return blueprint['services'][0]


def test_the_worker_deploys_main_and_only_when_a_person_says_so(worker):
    assert worker['type'] == 'worker' and worker['branch'] == 'main'
    assert worker['autoDeployTrigger'] == 'off'
    assert worker['numInstances'] == 1                      # the worker has no leader election
    assert worker['startCommand'] == 'python pump_worker.py'
    assert worker['rootDir'] == 'backend-track'


def test_no_secret_is_committed(worker):
    secrets = {env['key']: env for env in worker['envVars'] if env['key'] in ('SOLANA_RPC_URL', 'DATABASE_URL')}
    assert set(secrets) == {'SOLANA_RPC_URL', 'DATABASE_URL'}
    assert all(env.get('sync') is False and 'value' not in env for env in secrets.values())


def test_the_blueprint_runs_the_validated_launches_only_stream(worker):
    # Measured live: the whole-program stream is ~105 notifications/s (~10M credits a month, half of it
    # failed transactions); the creations stream delivered every launch at ~1/60 of that.
    stream = [env for env in worker['envVars'] if env['key'] == 'PUMP_WORKER_STREAM'][0]
    assert stream['value'] == 'creations'
    assert stream['value'] in pump_worker.STREAMS            # and it is a value the worker accepts


def test_the_code_default_stays_the_old_stream_so_an_existing_deployment_keeps_its_cursor():
    assert pump_worker.Settings.from_env({}).stream == 'program'
