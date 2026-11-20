"""The task final deadline is independent of the stress run timeout."""
from pathlib import Path


def test_task_final_latency_gate_reports_each_slow_request(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "scripts"))
    from stress_qq_delivery import task_latency_report

    timelines = {
        "on-limit": {"received_after_ms": 120000},
        "late": {"received_after_ms": 239474},
        "missing": {"received_after_ms": None},
    }
    samples, slow = task_latency_report(timelines, 120)
    assert samples["count"] == 2
    assert slow == ["late"]
    assert task_latency_report(timelines, None) == (samples, [])
