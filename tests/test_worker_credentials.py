import subprocess
from types import SimpleNamespace

import pytest

from qqbot.jobs.worker import configured_github


@pytest.mark.parametrize("host,key", [("github.com", "GH_TOKEN"), ("git.example.com", "GH_ENTERPRISE_TOKEN")])
def test_local_gh_login_precedes_environment(monkeypatch, host, key):
    monkeypatch.setenv("QQBOT_GITHUB_TOKEN", "fallback")
    for name in ("GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN", "GITHUB_ENTERPRISE_TOKEN"):
        monkeypatch.setenv(name, "override")
    monkeypatch.setenv("GH_CONFIG_DIR", "/credential-directory")
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        assert kwargs["env"]["GH_CONFIG_DIR"] == "/credential-directory"
        assert all(name not in kwargs["env"] for name in
                   ("GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN", "GITHUB_ENTERPRISE_TOKEN"))
        assert kwargs["stderr"] == subprocess.DEVNULL and kwargs["timeout"] == 5
        return SimpleNamespace(returncode=0, stdout="local-login\n")

    monkeypatch.setattr(subprocess, "run", run)
    github = configured_github(SimpleNamespace(github_host=host, gh="/usr/bin/gh"))
    assert github.credentials(SimpleNamespace(host=host)) == {key: "local-login"}
    assert calls == [["/usr/bin/gh", "auth", "token", "--hostname", host]]
    with pytest.raises(PermissionError):
        github.credentials(SimpleNamespace(host="other.example.com"))
    assert len(calls) == 1


@pytest.mark.parametrize("failure", ["missing", "timeout", "failed", "empty"])
@pytest.mark.parametrize("fallback", ["configured-token", ""])
def test_missing_local_login_falls_back(monkeypatch, failure, fallback):
    monkeypatch.setenv("CUSTOM_TOKEN", fallback)

    def run(*args, **kwargs):
        if failure == "missing":
            raise FileNotFoundError()
        if failure == "timeout":
            raise subprocess.TimeoutExpired("gh", 5)
        return SimpleNamespace(returncode=1 if failure == "failed" else 0, stdout="")

    monkeypatch.setattr(subprocess, "run", run)
    github = configured_github(SimpleNamespace(github_token_env="CUSTOM_TOKEN"))
    assert github.credentials(SimpleNamespace(host="github.com")) == ({"GH_TOKEN": fallback} if fallback else {})
