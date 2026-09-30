"""Tests that the bounded research workflow stays bounded, read-only, and honest.

These assert properties of the workflow file itself: an incomplete sample must
surface as unknown/incomplete rather than as a green run that quietly reported
zero buyers, and nothing here may place a trade or provision paid hosting.
"""
import re
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / '.github/workflows/solana-research.yml'
TEXT = WORKFLOW.read_text()
SPEC = yaml.safe_load(TEXT)
JOB = SPEC['jobs']['sample']
STEPS = JOB['steps']


def step_by_name(fragment):
    matches = [s for s in STEPS if fragment in (s.get('name') or '')]
    assert len(matches) == 1, f'expected exactly one step matching {fragment!r}'
    return matches[0]


def test_workflow_is_declared_bounded():
    assert JOB['timeout-minutes'] <= 6
    assert JOB['runs-on'] == 'ubuntu-latest'
    # Sampling is a short snapshot, never continuous monitoring.
    assert '--seconds 60' in step_by_name('Listen for 60 seconds')['run']


def test_permissions_are_read_only():
    """Asserted on the parsed spec: a token could never publish or deploy."""
    assert SPEC['permissions'] == {'contents': 'read'}
    for step in STEPS:
        assert step.get('permissions') in (None, {'contents': 'read'})
    for job in SPEC['jobs'].values():
        assert job.get('permissions') in (None, {'contents': 'read'})
    assert 'pull_request_target' not in TEXT


def test_workflow_never_trades_or_provisions_paid_hosting():
    forbidden = ['sendTransaction', 'signTransaction', 'solana-keygen',
                 'spl-token transfer', 'solana transfer', 'pump_worker.py',
                 'render', 'stripe', 'paystack', 'neon']
    lowered = TEXT.lower()
    hits = [token for token in forbidden if token.lower() in lowered]
    assert hits == [], f'bounded research workflow must not reference: {hits}'


def test_provider_url_is_never_echoed():
    # The RPC URL embeds an API key; it may be read into the environment only.
    assert 'echo "$SOLANA_RPC_URL"' not in TEXT
    assert 'echo $SOLANA_RPC_URL' not in TEXT
    assert 'HELIUS_RPC_URL }}' not in TEXT.replace('${{ secrets.HELIUS_RPC_URL }}', '')


def test_validated_launches_are_mirrored_into_annotations():
    """Artifact blobs are unreachable from restricted networks; annotations are not."""
    run = step_by_name('Listen for 60 seconds')['run']
    assert '::notice::Validated launch mint=' in run


def test_evidence_step_tolerates_an_unknown_result():
    """Non-zero exit means unknown/incomplete, which must not read as a broken run."""
    run = step_by_name('Validate early buyers')['run']
    assert 'pump_evidence.py' in run
    assert 'set +e' in run and 'set -e' in run
    assert 'exit 0' in run
    assert '::notice::' in run or '::warning::' in run


def emitted_text(fragment):
    """Text the workflow actually emits, excluding its own explanatory comments."""
    run = step_by_name(fragment)['run']
    out = []
    for line in run.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        if re.match(r'^(echo\b|print\(|f\.write\()', stripped):
            out.append(stripped)
    return '\n'.join(out).lower()


def test_both_no_evidence_paths_report_unknown_not_zero():
    """Checks emitted output only: a comment denying a claim is not a claim."""
    emitted = emitted_text('Validate early buyers')
    assert emitted, 'expected the evidence step to emit something'
    for phrase in ('zero buyers', 'no buyers', '0 buyers', 'first 50'):
        assert phrase not in emitted, f'workflow must not emit {phrase!r}'
    assert 'unknown/incomplete' in emitted
    assert 'no evidence' in emitted


def test_quiet_sample_is_reported_as_unknown_not_as_an_absence_of_activity():
    emitted = emitted_text('Validate early buyers')
    # Both branches that skip validation must name the unknown status.
    assert emitted.count('unknown/incomplete') >= 2
    for phrase in ('no launches occurred', 'no activity', 'skipped'):
        assert phrase not in emitted


def test_evidence_status_reaches_the_human_readable_summary():
    run = step_by_name('Validate early buyers')['run']
    for field in ('evidence_status', 'coverage_proven', 'buyer_count_known',
                  'unavailable_transactions', 'reached_launch_slot', 'claim'):
        assert field in run, f'summary must report {field}'


def test_artifact_names_are_attempt_scoped():
    upload = STEPS[-1]
    assert 'upload-artifact' in upload['uses']
    name = upload['with']['name']
    assert 'github.run_attempt' in name, 'artifact names must be unique per attempt'
    assert upload['with'].get('if-no-files-found') == 'warn'
    assert upload['with']['retention-days'] == 7


def test_missing_secret_fails_loudly_instead_of_sampling_nothing():
    for fragment in ('Listen for 60 seconds', 'Validate early buyers'):
        assert 'Missing HELIUS_RPC_URL' in step_by_name(fragment)['run']


@pytest.mark.parametrize('field', ['evidence_status', 'claim'])
def test_report_fields_are_written_to_the_artifact(field):
    assert 'pump-evidence.json' in STEPS[-1]['with']['path']
    assert field in step_by_name('Validate early buyers')['run']


def test_push_paths_cover_every_script_the_workflow_executes():
    """A change to a script the workflow runs must re-trigger it."""
    # PyYAML reads the bare `on:` key as boolean True.
    triggers = SPEC.get('on', SPEC.get(True))
    paths = triggers['push']['paths']
    for script in ('backend-track/pump_actions_sample.py', 'backend-track/pump_evidence.py'):
        assert script in paths, f'{script} is executed by this workflow but not watched'
    assert '.github/workflows/solana-research.yml' in paths


def test_workflow_is_on_demand_with_no_schedule():
    """A timed sample spends provider credits whether or not anyone reads it."""
    triggers = SPEC.get('on', SPEC.get(True))
    assert 'schedule' not in triggers
    assert 'cron' not in TEXT
    assert 'workflow_dispatch' in triggers
