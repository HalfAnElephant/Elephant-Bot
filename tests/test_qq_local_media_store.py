import os

import pytest
from sqlalchemy import insert

from qqbot.adapters.qq_official.local_media import LocalMediaStore, read_workspace_file, sniff_media
from qqbot.storage.schema import outbox

SCOPE = {"task_id": "task", "conversation_id": "conversation", "principal_key": "owner"}
PNG = b"\x89PNG\r\n\x1a\nexample"


def put(store, **kwargs):
    return store.put(PNG, filename="image.png", media_type="image", request_id="one", **(SCOPE | kwargs))


def test_scoped_immutable_restart(db, clock):
    store = LocalMediaStore(db, clock)
    record = put(store)
    assert put(store)["id"] == record["id"]
    assert (store.root / record["id"]).stat().st_mode & 0o777 == 0o600
    assert store.root.stat().st_mode & 0o777 == 0o700
    restored = LocalMediaStore(db, clock)
    assert restored.read(record["id"], **SCOPE)[1] == PNG
    for key in SCOPE:
        with pytest.raises(ValueError):
            restored.read(record["id"], **(SCOPE | {key: "foreign"}))
    with pytest.raises(ValueError, match="different file"):
        store.put(PNG + b"different", filename="image.png", media_type="image", request_id="one", **SCOPE)


def test_integrity_links_permissions_and_expiry(db, clock, tmp_path):
    store = LocalMediaStore(db, clock, ttl_seconds=1)
    record = put(store)
    path = store.root / record["id"]
    original = path.read_bytes()
    path.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="integrity"):
        store.read(record["id"], **SCOPE)
    path.write_bytes(original)
    path.chmod(0o644)
    with pytest.raises(ValueError, match="private"):
        store.read(record["id"], **SCOPE)
    path.chmod(0o600)
    alias = tmp_path / "hardlink"
    os.link(path, alias)
    with pytest.raises(ValueError, match="single-link"):
        store.read(record["id"], **SCOPE)
    alias.unlink()
    path.unlink()
    alias.write_bytes(PNG)
    path.symlink_to(alias)
    with pytest.raises(OSError):
        store.read(record["id"], **SCOPE)
    clock.advance(2)
    with pytest.raises(ValueError, match="expired"):
        store.read(record["id"], **SCOPE)
    assert store.cleanup() == 1
    assert alias.exists()  # Cleanup unlinks only owned entry, never follows symlinks.


def test_size_and_aggregate_budget(db, clock):
    store = LocalMediaStore(db, clock, max_bytes=len(PNG), task_budget=len(PNG), total_budget=2 * len(PNG))
    put(store)
    with pytest.raises(ValueError, match="budget"):
        store.put(PNG, filename="image.png", media_type="image", request_id="two", **SCOPE)
    put(store, task_id="two")
    with pytest.raises(ValueError, match="budget"):
        put(store, task_id="three")
    with pytest.raises(ValueError, match="size"):
        store.put(PNG + b"x", filename="image.png", media_type="image", request_id="size", **SCOPE)


@pytest.mark.parametrize("state", ["PENDING", "RETRYABLE", "SENDING", "UNKNOWN", "WAITING_FOR_TRIGGER"])
def test_cleanup_preserves_uncertain_or_pending_delivery(db, clock, state):
    import json
    store = LocalMediaStore(db, clock, ttl_seconds=1)
    record = put(store)
    with db.transaction() as c:
        c.execute(insert(outbox).values(id="delivery", dedupe_key="delivery", conversation_id="conversation",
            envelope_json=json.dumps({"official_content": {"media": {"local_ref": record["id"]}}}),
            state=state, created_at=clock.now().isoformat()))
    clock.advance(2)
    assert store.cleanup() == 0
    assert (store.root / record["id"]).exists()


def test_cleanup_expired_only(db, clock):
    store = LocalMediaStore(db, clock, ttl_seconds=1)
    record = put(store)
    assert store.cleanup() == 0
    clock.advance(2)
    assert store.cleanup() == 1
    assert not (store.root / record["id"]).exists()


@pytest.mark.parametrize("path", ["../secret.txt", "/etc/passwd", ".env", "outputs/../x.txt",
                                  "outputs/.secret.txt", "outputs/id_rsa", "outputs/cert.pem", "notes.txt"])
def test_workspace_paths_rejected(tmp_path, path):
    with pytest.raises(ValueError):
        read_workspace_file(tmp_path, path)


def test_workspace_read_rejects_symlinks_hardlinks_and_special_files(tmp_path):
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    path = outputs / "result.txt"
    path.write_bytes(b"result")
    assert read_workspace_file(tmp_path, "outputs/result.txt") == b"result"
    with pytest.raises(ValueError):
        read_workspace_file(tmp_path, "outputs/result.txt", max_bytes=3)
    alias = outputs / "link.txt"
    alias.symlink_to(path)
    with pytest.raises(OSError):
        read_workspace_file(tmp_path, "outputs/link.txt")
    alias.unlink()
    os.link(path, alias)
    with pytest.raises(ValueError):
        read_workspace_file(tmp_path, "outputs/result.txt")
    os.mkfifo(outputs / "fifo.txt")
    with pytest.raises(ValueError):
        read_workspace_file(tmp_path, "outputs/fifo.txt")
    nested = outputs / "nested"
    nested.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(OSError):
        read_workspace_file(tmp_path, "outputs/nested/result.txt")


@pytest.mark.parametrize("content,filename,kind", [(b"text", "bad.png", "image"), (PNG, "bad.txt", "file"),
    (PNG, "ok.png", "file"), (b"text", "../file.txt", "file"), (b"text", "file\n.txt", "file"),
    (b"{broken", "file.json", "file"), (b"text", "file.exe", "file")])
def test_sniff_rejects_mismatch(content, filename, kind):
    with pytest.raises(ValueError):
        sniff_media(content, filename, kind)


def test_sniff_known_types():
    assert sniff_media(PNG, "ok.png", "image") == "image/png"
    assert sniff_media(b"hello", "ok.txt", "file") == "text/plain"
    assert sniff_media(b"#!SILK_V3more", "ok.silk", "audio") == "audio/silk"
    assert sniff_media(b"\x00\x00\x00\x18ftypmp42rest", "ok.mp4", "video") == "video/mp4"


def test_office_signature_and_macro_rejection():
    import io
    import zipfile
    def office(*names):
        value = io.BytesIO()
        with zipfile.ZipFile(value, "w") as archive:
            for name in names:
                archive.writestr(name, b"<xml/>")
        return value.getvalue()
    docx = office("[Content_Types].xml", "word/document.xml")
    assert sniff_media(docx, "file.docx", "file").endswith("wordprocessingml.document")
    with pytest.raises(ValueError):
        sniff_media(docx, "file.xlsx", "file")
    with pytest.raises(ValueError):
        sniff_media(office("[Content_Types].xml", "word/document.xml", "word/vbaProject.bin"), "file.docx", "file")


def test_cleanup_old_orphans_not_unrelated_files(db, clock):
    from uuid import uuid4
    store = LocalMediaStore(db, clock, ttl_seconds=60)
    store.root.mkdir(mode=0o700)
    orphan = store.root / str(uuid4())
    orphan.write_bytes(PNG)
    old = clock.now().timestamp() - 61
    os.utime(orphan, (old, old))
    unrelated = store.root / "notes.txt"
    unrelated.write_text("keep")
    assert store.cleanup() == 1
    assert not orphan.exists() and unrelated.exists()


def test_multibyte_filename_byte_limit(db, clock):
    valid = "图" * 83 + ".png"
    invalid = "图" * 84 + ".png"
    assert sniff_media(PNG, valid, "image") == "image/png"
    store = LocalMediaStore(db, clock)
    with pytest.raises(ValueError, match="filename"):
        store.put(PNG, filename=invalid, media_type="image", request_id="long-name", **SCOPE)
    assert not store.root.exists()


@pytest.mark.parametrize("kind,name,prefix", [
    ("image", "picture.png", PNG),
    ("audio", "voice.silk", b"#!SILK_V3"),
    ("video", "movie.mp4", b"\x00\x00\x00\x18ftypmp42"),
    ("file", "report.pdf", b"%PDF-1.7"),
])
def test_official_exact_boundaries_are_streamed(db, clock, kind, name, prefix):
    import tempfile

    from qqbot.adapters.qq_official.local_limits import limit_for
    store = LocalMediaStore(db, clock)
    maximum = limit_for(kind)
    with tempfile.TemporaryFile("w+b") as source:
        source.write(prefix)
        source.truncate(maximum)
        record = store.put_file(source, filename=name, media_type=kind, request_id="boundary", **SCOPE)
        assert record["size"] == maximum
        metadata, snapshot = store.open(record["id"], **SCOPE)
        with snapshot:
            assert os.fstat(snapshot.fileno()).st_size == maximum
            assert snapshot.read(len(prefix)) == prefix
        assert metadata["size"] == maximum
        source.truncate(maximum + 1)
        with pytest.raises(ValueError):
            store.put_file(source, filename=name, media_type=kind, request_id="too-large", **SCOPE)


def test_workspace_snapshot_detaches_mutable_original(tmp_path):
    from qqbot.adapters.qq_official.local_media import snapshot_workspace_file
    (tmp_path / "outputs").mkdir()
    original = tmp_path / "outputs" / "file.txt"
    original.write_bytes(b"original bytes")
    snapshot, size, mime = snapshot_workspace_file(tmp_path, "outputs/file.txt", "file")
    try:
        original.write_bytes(b"changed")
        assert snapshot.read() == b"original bytes"
        assert size == 14 and mime == "text/plain"
    finally:
        snapshot.close()


def test_file_sniff_reads_bounded_chunks_and_json_is_lexical():
    import io

    from qqbot.adapters.qq_official.local_media import sniff_media_file
    class Bounded(io.BytesIO):
        def read(self, size=-1):
            assert 0 <= size <= 65536
            return super().read(size)
    assert sniff_media_file(Bounded(b"[" + b" " * 100000 + b"]"), "large.json", "file") == "application/json"
    assert sniff_media_file(Bounded(b"a" * 100000), "large.txt", "file") == "text/plain"
    with pytest.raises(ValueError):
        sniff_media_file(Bounded(b"{not closed"), "bad.json", "file")


@pytest.mark.parametrize("mutation", ["many_entries", "large_directory", "zip64", "dishonest_count"])
def test_office_directory_guard_runs_before_zip_parser(monkeypatch, mutation):
    import io
    import struct
    import zipfile

    from qqbot.adapters.qq_official.local_media import sniff_media_file
    raw = io.BytesIO()
    with zipfile.ZipFile(raw, "w") as archive:
        archive.writestr("[Content_Types].xml", b"<xml/>")
        archive.writestr("word/document.xml", b"<xml/>")
    value = bytearray(raw.getvalue())
    eocd = value.rfind(b"PK\x05\x06")
    if mutation == "many_entries":
        struct.pack_into("<HH", value, eocd + 8, 4097, 4097)
    elif mutation == "large_directory":
        struct.pack_into("<L", value, eocd + 12, 9 * 1024 * 1024)
    elif mutation == "zip64":
        struct.pack_into("<HH", value, eocd + 8, 65535, 65535)
    else:
        struct.pack_into("<HH", value, eocd + 8, 1, 1)
    def forbidden(*args, **kwargs):
        pytest.fail("untrusted central directory reached ZipFile")
    monkeypatch.setattr(zipfile, "ZipFile", forbidden)
    with pytest.raises(ValueError):
        sniff_media_file(io.BytesIO(value), "unsafe.docx", "file")
    with pytest.raises(ValueError):
        sniff_media(bytes(value), "unsafe.docx", "file")


def test_office_guard_does_not_cap_document_body(tmp_path):
    import zipfile

    from qqbot.adapters.qq_official.local_media import sniff_media_file
    path = tmp_path / "large.docx"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", b"<xml/>")
        with archive.open("word/document.xml", "w") as document:
            for _ in range(160):
                document.write(b" " * 65536)
    assert path.stat().st_size > 8 * 1024 * 1024
    with path.open("rb") as source:
        assert sniff_media_file(source, "large.docx", "file").endswith("wordprocessingml.document")
