"""Allow only the explicitly optional native FFmpeg fixture to skip in CI."""
import sys
import xml.etree.ElementTree as ET

OPTIONAL = ("tests.test_audio_input", "test_real_ffmpeg_normalizes_wave_for_asr")
REASON = "optional FFmpeg decoder is not installed"


def check(path):
    unexpected = []
    for case in ET.parse(path).findall(".//testcase"):
        skipped = case.find("skipped")
        if skipped is not None and (
            (case.get("classname"), case.get("name")) != OPTIONAL
            or skipped.get("message") != REASON
        ):
            unexpected.append((case.get("classname"), case.get("name")))
    assert not unexpected, f"CI requires all non-optional fixtures to run: {unexpected}"


if __name__ == "__main__":
    check(sys.argv[1])
