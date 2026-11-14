"""Read-only progress facts assembled from host receipts, never model percentages."""
import json

from pydantic import ValidationError
from sqlalchemy import or_, select

from qqbot.domain.agents import ProgressSpec
from qqbot.domain.tasks import Artifact
from qqbot.storage.action_schema import operations
from qqbot.storage.agent_schema import agent_events
from qqbot.storage.job_schema import jobs
from qqbot.storage.task_schema import observations, runs, tasks, waits


def milestone_spec(payload_json):
    """Historical text-only events remain readable without becoming evidence-bearing milestones."""
    try:
        spec = ProgressSpec.model_validate_json(payload_json)
    except ValidationError:
        return None
    return spec if spec.evidence_refs else None


def registered_artifacts(conn, task_ids):
    """Only validated metadata already accepted in task results or verified receipts qualifies."""
    records = list(conn.execute(select(tasks.c.result_json).where(tasks.c.id.in_(task_ids),
        tasks.c.result_json.is_not(None))).scalars())
    records.extend(conn.execute(select(operations.c.result_json).where(operations.c.task_id.in_(task_ids),
        operations.c.state == "VERIFIED")).scalars())
    found = {}
    for raw in records:
        record = json.loads(raw)
        if not isinstance(record, dict):
            continue
        artifacts = record.get("artifacts") or ()
        if not isinstance(artifacts, (list, tuple)):
            continue
        for artifact in artifacts:
            try:
                item = Artifact.model_validate_json(json.dumps(artifact))
            except ValidationError:
                continue
            found[item.id] = item.model_dump()
    return found


def task_progress(graph, conn, task, root, *, access, stale, delivery_states):
    budget = {"used": json.loads(task["budget_json"]),
              "limits": None if task["kind"] == "goal" else json.loads(task["plan_json"]).get("budget", {}),
              "scope": "tree_usage" if task["kind"] == "goal" else "task"}
    progress = {"verified_completed": [], "current_action": None, "waiting_for": [],
                "last_effective_progress_at": None, "budget": budget,
                "delivery": {"states": delivery_states, "execution_complete": task["state"] in {
                    "SUCCEEDED", "PARTIAL", "BLOCKED", "FAILED", "CANCELLED", "EXPIRED"},
                    "delivered": "SENT" in delivery_states}}
    if stale:
        progress["current_action"] = "正在根据最新授权和资料重新核实"
        return progress
    tree = conn.execute(select(tasks).where(or_(tasks.c.root_task_id == root["id"], tasks.c.id == task["id"]))).mappings().all()
    descendants = {task["id"]}
    by_id = {item["id"]: item for item in tree}
    for row in tree:
        # Parent-before-child creation order is not a durable contract.
        if row["id"] == task["id"]:
            continue
        parent = row
        visited = {parent["id"]}
        while parent["parent_task_id"] in by_id and parent["parent_task_id"] not in visited:
            visited.add(parent["parent_task_id"])
            parent = by_id[parent["parent_task_id"]]
            if parent["id"] == task["id"]:
                descendants.add(row["id"])
                break
    visible = []
    for row in tree:
        if row["id"] not in descendants or row["goal_revision"] != root["goal_revision"]:
            continue
        if not graph.handoff_valid(row):
            continue
        if access is not None and (not graph.service.permissions.check(access, "task.read", "task:" + row["id"]).allowed
                or graph.service.context_validator and not graph.service.context_validator(access, json.loads(row["context_json"]))):
            continue
        visible.append(row)
    ids = [row["id"] for row in visible]
    times = []
    completed = []
    for row in visible:
        if row["kind"] != "goal" and row["state"] == "SUCCEEDED":
            completed.append({"kind": "task", "reference": row["id"], "label": row["goal"][:300]})
    for row in conn.execute(select(observations).join(runs, runs.c.id == observations.c.run_id).where(
            observations.c.task_id.in_(ids), runs.c.basis_goal_revision == root["goal_revision"])).mappings():
        completed.append({"kind": "observation", "reference": row["id"], "label": "已保存资料核对记录"})
        times.append(row["retrieved_at"])
    for row in conn.execute(select(operations).join(runs, runs.c.id == operations.c.run_id).where(
            operations.c.task_id.in_(ids), operations.c.state == "VERIFIED",
            runs.c.basis_goal_revision == root["goal_revision"])).mappings():
        completed.append({"kind": "operation", "reference": row["id"], "label": "已核实操作回执"})
        times.append(row["updated_at"])
    for artifact_id, artifact in registered_artifacts(conn, ids).items():
        completed.append({"kind": "artifact", "reference": artifact_id,
                          "label": artifact["relative_path"], "sha256": artifact["sha256"]})
    milestones = conn.execute(select(agent_events).where(agent_events.c.task_id.in_(ids),
        agent_events.c.source_revision == root["goal_revision"], agent_events.c.kind.in_({
            "milestone", "child_result", "finished", "children_ready"}))
        .order_by(agent_events.c.created_at.desc(), agent_events.c.seq.desc())).mappings().all()
    latest = None
    for row in milestones:
        if row["kind"] == "milestone":
            spec = milestone_spec(row["payload_json"])
            if spec is None:
                continue
            if latest is None:
                latest = spec.model_dump(mode="json")
        times.append(row["created_at"])
    progress["phase"] = latest["phase"] if latest else None
    progress["latest_milestone"] = latest
    progress["verified_completed"] = completed[-40:]
    progress["verified_counts"] = {kind: sum(item["kind"] == kind for item in completed)
                                    for kind in ("task", "observation", "operation", "artifact")}
    progress["last_effective_progress_at"] = max(times) if times else None
    waiting = []
    for row in conn.execute(select(waits).where(waits.c.task_id.in_(ids),
            waits.c.state.in_({"INTENT", "WAITING"}))).mappings():
        reason = {"input": "等待补充信息", "external": "等待外部条件满足"}.get(row["kind"], "等待执行条件满足")
        waiting.append({"kind": row["kind"], "task_id": row["task_id"], "reason": reason,
                        "expires_at": row["expires_at"]})
    for row in visible:
        if row["state"] == "WAITING_CHILDREN":
            waiting.append({"kind": "children", "task_id": row["id"], "reason": "等待执行工作返回"})
        if row["state"] == "NEEDS_RECONCILIATION":
            waiting.append({"kind": "reconciliation", "task_id": row["id"], "reason": "等待核实外部操作结果"})
    for row in conn.execute(select(operations.c.id, operations.c.task_id, operations.c.state).where(
            operations.c.task_id.in_(ids), operations.c.state.in_({"UNKNOWN", "PARTIAL", "RECONCILIATION"}))).mappings():
        waiting.append({"kind": "reconciliation", "task_id": row["task_id"], "reference": row["id"],
                        "state": row["state"], "reason": "等待核实外部操作结果"})
    for row in conn.execute(select(jobs.c.id, jobs.c.task_id, jobs.c.state).where(jobs.c.task_id.in_(ids),
            jobs.c.state.not_in({"EXITED", "FAILED", "CANCELLED"}))).mappings():
        waiting.append({"kind": "job", "task_id": row["task_id"], "reference": row["id"], "state": row["state"],
                        "reason": "等待受管进程结束"})
    progress["waiting_for"] = waiting[:40]
    active = conn.execute(select(runs.c.id).where(runs.c.task_id.in_(ids), runs.c.slot_reserved == 1)).first()
    settled_children = [row for row in visible if row["id"] != task["id"]]
    if task["state"] == "SUCCEEDED":
        action = "目标已完成，结果已送达" if progress["delivery"]["delivered"] else "目标已完成，等待结果投递"
    elif task["state"] in {"PARTIAL", "BLOCKED", "FAILED", "CANCELLED", "EXPIRED"}:
        action = {"PARTIAL": "已完成一部分", "BLOCKED": "暂时无法继续", "FAILED": "这次未能完成",
                  "CANCELLED": "已取消", "EXPIRED": "已超时停止"}[task["state"]]
    elif task["state"] == "WAITING_INPUT":
        action = "等待你补充信息"
    elif any(item["kind"] == "reconciliation" for item in waiting):
        action = "正在核实外部操作结果"
    elif any(item["kind"] == "job" for item in waiting):
        action = "正在等待受管进程返回"
    elif task["kind"] == "goal" and task["state"] == "WAITING_CHILDREN" and settled_children and all(row["state"] in {
            "SUCCEEDED", "PARTIAL", "BLOCKED", "FAILED", "CANCELLED", "EXPIRED"} for row in settled_children) and not active:
        action = "执行工作已结束，等待核验"
    elif task["state"] == "WAITING_EXTERNAL":
        action = "正在等待外部条件满足"
    elif active:
        action = "正在执行"
    else:
        action = {"QUEUED": "尚未开始", "WAITING_CHILDREN": "正在等待执行工作返回",
                  "CANCEL_REQUESTED": "正在停止", "CANCELLED": "已取消", "BLOCKED": "暂时无法继续",
                  "FAILED": "这次未能完成", "PARTIAL": "已完成一部分", "EXPIRED": "已超时停止"}.get(task["state"], "正在核实进度")
    progress["current_action"] = action
    return progress
