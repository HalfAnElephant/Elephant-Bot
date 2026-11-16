"""Release-facing checks for the offline control-plane benchmark."""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts' / 'benchmark_control_plane.py'
HISTORICAL = ROOT / 'docs' / 'control-plane-benchmark.json'
OFFLINE_SCRIPT = '''
import runpy
import socket
import sys


def observe(event, args):
    if event == "socket.connect" and args[0].family in {socket.AF_INET, socket.AF_INET6}:
        raise AssertionError("offline benchmark attempted a network connection")


sys.addaudithook(observe)
sys.argv = sys.argv[1:]
runpy.run_path(sys.argv[0], run_name="__main__")
'''


def run_benchmark(cwd, *options):
    # Keep real model, QQ and proxy credentials out of the child entirely.
    # Supply the repository resources needed by Alembic in a disposable CWD.
    # A regression to a relative default output path cannot modify tracked files.
    for name in ('alembic.ini', 'migrations'):
        link = cwd / name
        if not link.exists():
            link.symlink_to(ROOT / name)
    return subprocess.run(
        [sys.executable, '-c', OFFLINE_SCRIPT, str(SCRIPT), '--seconds', '1', '--rate', '12', '--seed', '123',
         *map(str, options)],
        cwd=cwd,
        env={'PATH': '/usr/bin:/bin', 'HOME': str(cwd), 'PYTHONPATH': str(ROOT / 'src')},
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )


def test_offline_control_plane_benchmark_drains_and_preserves_historical_report(tmp_path):
    historical_hash = hashlib.sha256(HISTORICAL.read_bytes()).digest()
    docs = tmp_path / 'docs'
    docs.mkdir()
    sentinel = docs / HISTORICAL.name
    sentinel.write_text('historical data', encoding='utf-8')

    run = run_benchmark(tmp_path)
    assert run.returncode == 0, run.stderr + run.stdout
    report = json.loads(run.stdout)
    assert report['report_version'] == 2
    assert report['seed'] == 123
    assert report['events'] == 12
    assert report['inbox_states'] == {'DONE': 12}
    assert report['passed'] is True
    assert report['counters']['accepted'] == 12
    assert report['counters']['duplicates_attempted'] == report['counters']['duplicates_rejected'] == 2
    for counter in ('tasks_admitted', 'tasks_queried', 'tasks_cancelled'):
        assert report['counters'][counter] == 1
    assert report['counters']['network_attempts'] == 0
    assert report['task_states'] == {'CANCELLED': 1}
    assert report['drain']['completed'] is True
    assert report['drain']['timed_out'] is False
    assert report['latency_ms']['ingress']['count'] == report['events']
    for phase in ('task_accept', 'task_query', 'task_cancel'):
        assert report['latency_ms'][phase]['count'] > 0
    for phase in report['latency_ms'].values():
        if phase['count']:
            assert 0 <= phase['p50'] <= phase['p95'] <= phase['p99']
    assert sentinel.read_text(encoding='utf-8') == 'historical data'
    assert hashlib.sha256(HISTORICAL.read_bytes()).digest() == historical_hash


def test_offline_benchmark_writes_only_new_output(tmp_path):
    output = tmp_path / 'run.json'
    first = run_benchmark(tmp_path, '--output', output)
    assert first.returncode == 0, first.stderr + first.stdout
    saved = output.read_bytes()
    assert json.loads(saved) == json.loads(first.stdout)
    second = run_benchmark(tmp_path, '--output', output)
    assert second.returncode != 0
    assert output.read_bytes() == saved
