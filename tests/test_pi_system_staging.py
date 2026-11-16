"""Exercise the privileged Pi staging flow without installing or becoming root."""

import os
import stat
import subprocess
from pathlib import Path

import pytest

SOURCE = Path(__file__).resolve().parents[1] / "deploy/pi-install.sh"


@pytest.mark.parametrize("failure", [None, "npm", "ownership", "probe", "marker"])
def test_system_pi_hardens_staged_dependencies_before_probe_or_publish(tmp_path, failure):
    base = tmp_path / "installed"
    base.mkdir()
    (base / "package.json").write_text("{}\n")
    (base / "package-lock.json").write_text("{}\n")
    target = base / "node_modules"
    target.mkdir()
    (target / "keep").write_text("previous Pi")
    marker = base / ".elephant-pi"
    marker.write_text("/operator/pi\n")
    probe = tmp_path / "probe-ran"

    result = subprocess.run(
        [
            "/bin/sh", "-ec",
            r'''
. "$1"
base=$2
failure=$3
probe=$4
env() {
    test "$*" = '-i PATH='"$PATH"' HOME=/root npm ci --ignore-scripts --no-audit --no-fund' || exit 30
    test "$PWD" != "$base" && test "$(stat -c %a .)" = 700 || exit 31
    test -f package.json && test -f package-lock.json && test ! -e .elephant-pi || exit 32
    mkdir -m 0777 node_modules
    mkdir -m 0777 node_modules/.bin node_modules/pkg
    touch node_modules/pkg/cli
    chmod 0777 node_modules/pkg/cli
    ln -s ../pkg/cli node_modules/.bin/pi
    test "$failure" != npm
}
find() {
    # Model the real root-owner preflight without requiring root on pytest.
    test "$2" != "$base/node_modules" || exit 34
    test "$failure" != ownership || printf '%s\n' "$2"
}
elephant_pi_version() {
    test "$2:$3" = "1.5.0:system" || exit 35
    test "$(stat -c %a "$(dirname "$(dirname "$(dirname "$1")")")")" = 711 || exit 36
    test -z "$(command find "$(dirname "$(dirname "$1")")" ! -type l -perm /022 -print)" || exit 37
    test "$(stat -Lc %a "$1")" = 755 || exit 38
    test "$(cat "$base/node_modules/keep")" = "previous Pi" || exit 39
    test "$(cat "$base/.elephant-pi")" = /operator/pi || exit 40
    : > "$probe"
    test "$failure" != probe
}
if [ "$failure" = marker ]; then
    elephant_pi_write_marker() { return 55; }
fi
elephant_install_private_system_pi "$base" 1.5.0
''',
            "_", str(SOURCE), str(base), failure or "", str(probe),
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert not list(tmp_path.glob(".elephant-pi-stage.*"))
    assert probe.exists() is (failure in {None, "probe", "marker"})
    if failure:
        assert result.returncode != 0
        assert marker.read_text() == "/operator/pi\n"
        assert (target / "keep").read_text() == "previous Pi"
    else:
        assert result.returncode == 0, result.stderr
        assert marker.read_text() == "bundled\n"
        assert not (target / "keep").exists()
        assert os.readlink(target / ".bin/pi") == "../pkg/cli"
        assert stat.S_IMODE((target / ".bin/pi").stat().st_mode) == 0o755
