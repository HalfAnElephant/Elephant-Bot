"""Tencent official C2C/group media limits, in exact bytes.

qqbot-nodejs 429a0f1b798f3b0fcef3679e207dbc0469a1df2d
USAGE.md §10.3; src/protocol/utils/file-utils.ts MEDIA_FILE_TYPE_INFO.
The SDK labels these MB but defines each using 1024 * 1024 (MiB).
These constants do not establish a channel/DM multipart limit.
"""
MIB = 1024 * 1024
MEDIA_LIMITS = {"image": 30 * MIB, "audio": 20 * MIB, "video": 100 * MIB, "file": 100 * MIB}
MAX_FILE_BYTES = 100 * MIB


def limit_for(media_type: str) -> int:
    try:
        return MEDIA_LIMITS[media_type]
    except (KeyError, TypeError):
        raise ValueError("unsupported local media type") from None


def validate_size(size: int, media_type: str) -> None:
    if type(size) is not int or not 0 < size <= limit_for(media_type):
        raise ValueError("local media exceeds official type limit or is empty")
