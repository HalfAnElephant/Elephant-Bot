import importlib.util
import json
from pathlib import Path

import pytest
from sqlalchemy import func, select

from qqbot.domain import AccessContext, ConversationRef, PrincipalRef, Surface
from qqbot.memory import SocialService
from qqbot.storage.db import Database
from qqbot.storage.operations import backup, export_deletions, restore
from qqbot.storage.schema import inbox, outbox
from qqbot.storage.social_schema import social_records

spec = importlib.util.spec_from_file_location(
    "koishi_import", Path(__file__).parents[1] / "scripts/import_koishi_profile.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


@pytest.fixture
def profile_context(clock):
    access = AccessContext(
        principal=PrincipalRef(adapter_instance_id="a", identity_namespace="qq", remote_user_id="u"),
        conversation=ConversationRef(
            deployment_id="d",
            adapter_instance_id="a",
            bot_identity_id="b",
            surface=Surface.PRIVATE,
            remote_conversation_id="u",
        ),
        role="admin",
    )
    profile = module.Profile.model_validate_json(
        json.dumps(
            {
                "source": "koishi/personas/qq/USER.md",
                "observed_at": clock.now().isoformat(),
                "entries": [
                    {
                        "key": "technology",
                        "text": "偏好成熟且持续维护的工具",
                        "excerpt": "优先考虑成熟、广泛使用的技术方案。重视项目是否持续维护。",
                    }
                ],
            }
        )
    )
    return access, profile


def test_import_is_private_idempotent_and_not_platform_evidence(db, clock, profile_context):
    access, profile = profile_context
    assert module.import_profile(db, clock, access, profile)["inserted"] == 1
    assert module.import_profile(db, clock, access, profile)["existing"] == 1
    svc = SocialService(db, clock)
    (record,) = svc.retrieve(access, "工具")
    assert record.source_kind == "inference"
    assert not json.loads(record.value_json)["provenance"]["original_message_verified"]
    for other in (
        access.model_copy(update={"principal": access.principal.model_copy(update={"remote_user_id": "v"})}),
        access.model_copy(
            update={
                "conversation": access.conversation.model_copy(
                    update={"surface": Surface.GROUP, "remote_conversation_id": "g"}
                )
            }
        ),
    ):
        assert svc.retrieve(other, "工具") == []
        with pytest.raises(PermissionError):
            module.import_profile(db, clock, other, profile)
    with db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(inbox)).scalar_one() == 0
        assert conn.execute(select(func.count()).select_from(outbox)).scalar_one() == 0
    candidate = record.model_copy(update={"status": "candidate", "memory_id": "fabricated"})
    with pytest.raises(ValueError, match="missing"):
        svc.candidate(access, candidate, explicit=True)


def test_deleted_import_stays_deleted_after_restore(db, clock, profile_context, tmp_path):
    access, profile = profile_context
    module.import_profile(db, clock, access, profile)
    svc = SocialService(db, clock)
    (record,) = svc.retrieve(access, "工具")
    snapshot, ledger, target = (tmp_path / name for name in ["old.sqlite", "deletions.json", "new.sqlite"])
    backup(db.path, snapshot)
    svc.forget(access, record.memory_id)
    assert module.import_profile(db, clock, access, profile)["revoked"] == 1
    export_deletions(db.path, ledger)
    restore(snapshot, target, ledger)
    recovered = Database(str(target))
    try:
        assert module.import_profile(recovered, clock, access, profile)["revoked"] == 1
        assert SocialService(recovered, clock).retrieve(access, "工具") == []
        with recovered.engine.connect() as conn:
            raw = conn.execute(select(social_records.c.record_json)).scalar_one()
            assert json.loads(raw)["value_json"] == "null"
    finally:
        recovered.close()


def test_import_conflict_rolls_back_batch_and_rejects_secrets(db, clock, profile_context):
    access, profile = profile_context
    module.import_profile(db, clock, access, profile)
    changed = profile.model_copy(
        update={
            "entries": (
                module.Entry(key="new", text="新增条目", excerpt="新增来源"),
                profile.entries[0].model_copy(update={"text": "发生冲突"}),
            )
        }
    )
    with pytest.raises(ValueError, match="differs"):
        module.import_profile(db, clock, access, changed)
    unsafe = profile.model_copy(
        update={"entries": (module.Entry(key="secret", text="private", excerpt="password=do-not-save"),)}
    )
    with pytest.raises(ValueError, match="secret"):
        module.import_profile(db, clock, access, unsafe)
    with db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(social_records)).scalar_one() == 1
