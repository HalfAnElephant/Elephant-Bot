"""The optional decoder exemption must not mask unrelated regression skips."""
import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("check_test_skips", Path("scripts/check_test_skips.py"))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


@pytest.mark.parametrize(("classname", "name", "reason", "allowed"), [
    (*gate.OPTIONAL, gate.REASON, True),
    (*gate.OPTIONAL, "fixture missing", False),
    ("tests.test_worker_failures", "test_lease", gate.REASON, False),
    (gate.OPTIONAL[0], "test_real_silk_decode_in_child_process", gate.REASON, False),
])
def test_only_missing_optional_ffmpeg_can_skip(tmp_path, classname, name, reason, allowed):
    from xml.etree.ElementTree import Element, ElementTree, SubElement

    suite = Element("testsuite")
    case = SubElement(suite, "testcase", classname=classname, name=name)
    SubElement(case, "skipped", message=reason)
    path = tmp_path / "pytest.xml"
    ElementTree(suite).write(path)
    if allowed:
        gate.check(path)
    else:
        with pytest.raises(AssertionError, match="non-optional"):
            gate.check(path)
