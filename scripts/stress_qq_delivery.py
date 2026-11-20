"""Run concurrent synthetic QQ conversations and Worker goals against real OpenCode Go."""
import argparse
import asyncio
import json
import os
import tempfile
import time
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

import httpx
from benchmark_control_plane import percentiles
from sqlalchemy import func, select

from elephant.app import create_app
from elephant.config import (
    AdapterConfig,
    ModelConfig,
    OrchestrationConfig,
    OwnerConfig,
    QQOwnerBinding,
    Settings,
)
from elephant.jobs.worker import Worker
from elephant.runtime.scheduler import Scheduler
from elephant.storage.agent_schema import dialogue_inputs
from elephant.storage.schema import inbox, outbox
from elephant.storage.task_schema import model_requests, runs, tasks

ENDPOINT = "https://opencode.ai/inference/go/openai/v1/chat/completions"
MODEL = "deepseek-v4.1-flash"
KEY_ENV = "OPENCODE_GO_API_KEY"
ROOT = Path(__file__).resolve().parents[1]


def task_attempts(conn, task_anchors, goals, children):
    by_root = {row.id: row.anchor for row in goals}
    by_child = {child: by_root[root] for child, root in children}
    attempts = {anchor: [] for anchor in sorted(task_anchors)}
    model_counts = dict(conn.execute(select(model_requests.c.run_id, func.count()).where(
        model_requests.c.kind != "tool").group_by(model_requests.c.run_id)).all())
    for row in conn.execute(select(runs).order_by(runs.c.started_at, runs.c.id)).mappings():
        claim = json.loads(row["request_json"] or "{}")
        if row["role"] == "worker":
            anchor = by_child.get(row["task_id"])
        elif claim.get("target_root_task_id"):
            anchor = by_root.get(claim["target_root_task_id"])
        else:
            anchor = json.loads(claim.get("event_json") or "{}").get("platform_message_id")
        if anchor in attempts:
            attempts[anchor].append({
                "role": row["role"], "purpose": row["purpose"], "state": row["state"], "task_id": row["task_id"],
                "started_at": row["started_at"], "deferred_at": claim.get("input_deferred_at"),
                "settled_at": row["quiescent_at"], "yielded_for_input": bool(claim.get("yielding_for_inputs")),
                "model_requests": model_counts.get(row["id"], 0),
            })
    return attempts


def task_latency_report(task_timelines, max_seconds):
    samples = {anchor: timeline["received_after_ms"] for anchor, timeline in task_timelines.items()
        if timeline["received_after_ms"] is not None}
    over_budget = sorted(anchor for anchor, elapsed in samples.items()
        if max_seconds is not None and elapsed > max_seconds * 1000)
    return percentiles(list(samples.values())), over_budget


async def trial(args, directory):
    chat_users = tuple(f"stress-{index}" for index in range(args.targets))
    quote_users = tuple(f"stress-quote-{index}" for index in range(args.quote_pairs))
    task_users = tuple(f"stress-task-{index}" for index in range(1 if args.shared_task_target else args.tasks))
    users = (*chat_users, *quote_users, *task_users)
    task_cases = [(task_users[index % len(task_users)], f"stress-goal-{index}") for index in range(args.tasks)]
    task_anchors = {anchor for _, anchor in task_cases}
    expected, submitted_at, submitted_utc, deliveries = {}, {}, {}, []
    config = AdapterConfig(instance_id="stress", kind="official", app_id="stress-app", bot_id="stress-bot",
        secret_env="ELEPHANT_STRESS_QQ_SECRET", allowed_users=users, streaming_enabled=True, typing_enabled=True,
        proactive_verified=args.proactive)
    os.environ["ELEPHANT_STRESS_QQ_SECRET"] = "local-fixture"

    async def receive(request):
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "local-fixture", "expires_in": 3600})
        path = request.url.path.split("/")
        if len(path) != 5 or path[2] != "users" or path[3] not in users:
            return httpx.Response(400)
        body = json.loads(request.content)
        ident = body.get("stream_msg_id") or f"local-{len(deliveries)}"
        deliveries.append((path[3], path[4], body, ident, time.perf_counter()))
        return httpx.Response(200, json={"id": ident})

    settings = Settings(database=str(directory / "core.db"), tasks_enabled=True, adapters=(config,),
        owner=OwnerConfig(qq_bindings=tuple(QQOwnerBinding(app_id=config.app_id, user_openid=target,
            verified_at=datetime.now(UTC).isoformat()) for target in task_users)),
        model=ModelConfig(model=MODEL, endpoint=ENDPOINT, api_key_env=KEY_ENV, timeout_seconds=90),
        orchestration=OrchestrationConfig(pi_executable=str(ROOT / "node_modules/.bin/pi"),
            model_request_limit=args.concurrency, leader_inbox_coalesce_seconds=0))
    settings = settings.model_copy(update={
        "execution": settings.execution.model_copy(update={
            "notify_before_tools": False, "warm_idle_max": max(2, args.concurrency)}),
        "skills": settings.skills.model_copy(update={"background_review_enabled": False}),
    })
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive), trust_env=False) as qq_client:
        core = create_app(settings, run_background=False, official_client=qq_client, model_client=httpx.AsyncClient(trust_env=False)).state.core
        adapter = core.adapters["stress"]
        inputs, quote_batches, quote_parents = [], [], {}
        dialogue_ids = []

        def message(target, anchor, text, reply_to=None):
            data = {"id": anchor, "author": {"user_openid": target}, "content": text,
                    "timestamp": datetime.now(UTC).isoformat()}
            if reply_to:
                data["message_reference"] = {"message_id": reply_to}
            return adapter.normalize({"op": 0, "id": anchor, "t": "C2C_MESSAGE_CREATE", "d": data})

        def enable(event):
            with core.db.transaction() as conn:
                dialogue_ids.append(core.sessions.dialogue(conn, event)["id"])

        for target in chat_users:
            for index in range(args.messages):
                anchor = f"{target}-msg-{index}"
                marker = f"ACK-{target}-{index}"
                expected[anchor] = (target, marker)
                inputs.append(message(target, anchor, f"独立的测试请求。只回复这一行，不要调用工具：{marker}"))
            enable(inputs[-1])
        for target in quote_users:
            anchor = f"{target}-primary"
            marker = f"ACK-{target}"
            expected[anchor] = (target, marker)
            primary = message(target, anchor, "这是引用补充测试。如果收到两条引用，本轮先用 task_outcome 的 "
                "covered_input_ids 记录两条补充的 ID，再输出一行，不要解释：" + marker)
            batch = [primary]
            for index in range(2):
                supplement = f"{target}-supplement-{index}"
                quote_parents[supplement] = anchor
                batch.append(message(target, supplement, f"第 {index + 1} 条补充，只补充当前测试。最终只答 {marker}。", anchor))
            quote_batches.append(batch)
            enable(primary)
        for index, (target, anchor) in enumerate(task_cases):
            inputs.append(message(target, anchor,
                f"请用 tasks_submit 把独立合成任务 {index} 委派给一位 Worker，由 Worker 计算 {index} 加 7。"
                "任务与子任务均不需要观察证据，success_checks 传空数组。不要读写文件或访问网络。等 Worker 完成后汇报结果。"))
            enable(inputs[-1])
        core.sessions.settings = settings.model_copy(update={"orchestration": settings.orchestration.model_copy(
            update={"leader_steering_dialogues": tuple(dialogue_ids)})})
        core.scheduler = scheduler = Scheduler(core, core.leader)
        fresh = []
        task_anchor = func.json_extract(tasks.c.event_json, "$.platform_message_id")
        goals_query = select(tasks.c.id, task_anchor.label("anchor"), tasks.c.state, tasks.c.created_at).where(
            tasks.c.kind == "goal", task_anchor.in_(task_anchors))

        async def submit(event):
            submitted_at[event.platform_message_id] = time.perf_counter()
            submitted_utc[event.platform_message_id] = datetime.now(UTC).isoformat()
            # Replay the same normalized callback while the other targets enqueue.
            for _ in range(2):
                _, accepted = await asyncio.to_thread(core.inbox.accept, event)
                fresh.append(accepted)

        async def submit_batch(batch):
            for event in batch:
                await submit(event)

        async def drive():
            while True:
                await core.tick(dispatch=False)
                await scheduler.step()
                await core.deliver(wait=False)
                await asyncio.sleep(.025)

        input_count = len(inputs) + sum(map(len, quote_batches))
        driver = asyncio.create_task(drive())
        started = time.perf_counter()
        timed_out = False
        retry_limit_reached = False
        driver_error = None
        try:
            try:
                async with asyncio.timeout(args.timeout):
                    await asyncio.gather(*(submit(event) for event in inputs),
                                         *(submit_batch(batch) for batch in quote_batches))
                    while True:
                        if driver.done():
                            error = driver.exception()
                            driver_error = Worker.rejection_detail(error) if isinstance(error, httpx.HTTPStatusError) else (
                                type(error).__name__ if error else "driver_stopped")
                            break
                        with core.db.engine.connect() as conn:
                            finished = conn.execute(select(dialogue_inputs.c.handled_at)).scalars().all()
                            goal_states = [row.state for row in conn.execute(goals_query)]
                            attempts = conn.scalar(select(func.count()).select_from(runs))
                            queued = conn.execute(select(outbox.c.id).where(outbox.c.state.in_(
                                ["PENDING", "RETRYABLE", "SENDING", "STREAM_PENDING"]))).first()
                        if attempts > input_count * 5 + args.tasks * 4:
                            retry_limit_reached = True
                            break
                        if (len(finished) == input_count and all(finished) and not queued and not scheduler.active
                                and all(state in {
                                    "SUCCEEDED", "FAILED", "EXPIRED", "CANCELLED", "BLOCKED", "PARTIAL",
                                } for state in goal_states)):
                            break
                        await asyncio.sleep(.15)
            except TimeoutError:
                timed_out = True
            with core.db.engine.connect() as conn:
                rows = conn.execute(select(outbox).order_by(outbox.c.enqueue_seq)).mappings().all()
                workers = conn.execute(select(runs.c.task_id).where(runs.c.role == "worker")).scalars().all()
                goals = conn.execute(goals_query).all()
                root_ids = [row[0] for row in goals]
                ingress_rank = dict(conn.execute(select(inbox.c.message_id, inbox.c.seq)).all())
                claims = dict(conn.execute(select(func.json_extract(dialogue_inputs.c.event_json, "$.platform_message_id"),
                    dialogue_inputs.c.claimed_attempt_id).where(dialogue_inputs.c.handled_at.is_not(None))).all())
                children = conn.execute(select(tasks.c.id, tasks.c.root_task_id).where(
                    tasks.c.root_task_id.in_(root_ids))).all()
                attempts_by_task = task_attempts(conn, task_anchors, goals, children)
                usage = Counter(conn.execute(select(model_requests.c.state).where(
                    model_requests.c.kind != "tool")).scalars())
            final = [item for item in deliveries if (
                item[1] == "stream_messages" and item[2].get("input_state") == 10
                or item[1] == "messages" and item[2].get("msg_type") != 6)]
            chat_replies = [item for item in final if item[0] in (*chat_users, *quote_users)]
            anchors = Counter(item[2].get("msg_id") for item in chat_replies)
            receipt_rows = {row["platform_message_id"]: row for row in rows if row["platform_message_id"]}
            rank = {ident: row["enqueue_seq"] for ident, row in receipt_rows.items()}
            envelopes = {ident: json.loads(row["envelope_json"]) for ident, row in receipt_rows.items()}
            task_final_samples = [(envelopes[ident]["anchor_message_id"], (receipt_rows[ident], when))
                for target, _, _, ident, when in final
                if target in task_users and ident in envelopes and envelopes[ident].get("purpose") == "result"
                and receipt_rows[ident]["origin_kind"] == "task"]
            task_finals = dict(task_final_samples)
            goals_by_anchor = {row.anchor: row for row in goals}
            task_timelines = {}
            for anchor, attempts in attempts_by_task.items():
                goal = goals_by_anchor.get(anchor)
                receipt, when = task_finals.get(anchor, ({}, None))
                task_timelines[anchor] = {
                    "submitted_at": submitted_utc.get(anchor), "goal_created_at": goal.created_at if goal else None,
                    "attempts": attempts, "final_enqueued_at": receipt.get("created_at"),
                    "final_sent_at": receipt.get("sent_at"),
                    "received_after_ms": (when - submitted_at[anchor]) * 1000 if when is not None and anchor in submitted_at else None,
                }
            task_latency, slow_tasks = task_latency_report(task_timelines, args.max_task_final_seconds)
            extra_task_finals = len(task_final_samples) - len(task_finals)
            task_missing = sorted(task_anchors - task_finals.keys())
            outbox_latency = [(datetime.fromisoformat(row["sent_at"]) - datetime.fromisoformat(row["created_at"]))
                              .total_seconds() * 1000 for row in rows if row["sent_at"]]
            by_target = defaultdict(list)
            reply_errors = []
            reply_order = defaultdict(list)
            task_contents = []
            for target, _, body, ident, _ in final:
                anchor = body.get("msg_id")
                content = body.get("content_raw") or body.get("content") or body.get("markdown", {}).get("content")
                if target in (*chat_users, *quote_users):
                    if expected.get(anchor) != (target, content) or ident not in rank:
                        reply_errors.append({"anchor": anchor, "expected": expected.get(anchor),
                                             "actual": str(content)[:160], "recognized_receipt": ident in rank})
                    if anchor in ingress_rank:
                        reply_order[target].append(ingress_rank[anchor])
                else:
                    task_contents.append((target, envelopes.get(ident, {}).get("anchor_message_id") or anchor, content))
                if ident in rank:
                    by_target[target].append(rank[ident])
            disordered = [target for target, sequence in by_target.items() if sequence != sorted(sequence)]
            reply_disorder = [target for target, sequence in reply_order.items() if sequence != sorted(sequence)]
            task_duplicates = sum(count - 1 for count in Counter(task_contents).values())
            root_duplicates = sum(count - 1 for count in Counter(row[1] for row in goals).values())
            separated_quotes = sorted(quote for quote, parent in quote_parents.items()
                                      if not claims.get(parent) or claims.get(quote) != claims[parent])
            missing = sorted(set(expected) - set(anchors))
            duplicates = {anchor: count for anchor, count in anchors.items() if count > 1}
            assigned = {root for child, root in children if child in workers}
            latency = [(when - submitted_at[body["msg_id"]]) * 1000
                       for _, _, body, _, when in chat_replies if body.get("msg_id") in submitted_at]
            return {"model": MODEL, "proactive": args.proactive, "elapsed_seconds": round(time.perf_counter() - started, 2), "timed_out": timed_out, "retry_limit_reached": retry_limit_reached, "driver_error": driver_error,
                "fresh_inputs": sum(fresh), "duplicate_callbacks_rejected": len(fresh) - sum(fresh),
                "chat_replies": len(chat_replies), "task_replies": len(task_contents), "latency_ms": percentiles(latency) if latency else {},
                "outbox_latency_ms": percentiles(outbox_latency),
                "task_final_latency_ms": task_latency, "task_timelines": task_timelines,
                "max_task_final_seconds": args.max_task_final_seconds, "slow_task_finals": slow_tasks,
                "missing_task_finals": task_missing, "typing_signals": sum(body.get("msg_type") == 6 for _, _, body, _, _ in deliveries),
                "native_stream_finals": sum(path == "stream_messages" for _, path, _, _, _ in final),
                "quote_batches": len(quote_batches), "separated_quotes": separated_quotes,
                "missing": missing, "duplicate_replies": duplicates, "unexpected_replies": reply_errors,
                "out_of_order_targets": disordered, "out_of_order_reply_targets": reply_disorder,
                "duplicate_task_replies": task_duplicates, "duplicate_task_finals": extra_task_finals, "duplicate_root_goals": root_duplicates, "outbox_states": dict(Counter(row["state"] for row in rows)),
                "worker_runs": len(workers), "assigned_goals": len(assigned), "goal_states": dict(Counter(row.state for row in goals)),
                "model_requests": dict(usage), "passed": not (
                    timed_out or retry_limit_reached or driver_error or missing or duplicates or reply_errors or disordered or reply_disorder or task_duplicates
                    or root_duplicates or task_missing or extra_task_finals or separated_quotes or slow_tasks or sum(fresh) != input_count
                    or len(assigned) != args.tasks or Counter(row.state for row in goals) != {"SUCCEEDED": args.tasks})}
        finally:
            driver.cancel()
            await asyncio.gather(driver, return_exceptions=True)
            await core.stop()
            core.db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", type=int, default=3)
    parser.add_argument("--messages", type=int, default=3, help="independent messages per QQ target")
    parser.add_argument("--tasks", type=int, default=2)
    parser.add_argument("--quote-pairs", type=int, default=0, help="extra targets with two real QQ references each")
    parser.add_argument("--shared-task-target", action="store_true", help="queue all tasks in the same QQ dialogue")
    parser.add_argument("--proactive", action="store_true", help="simulate a QQ app allowed to send proactive messages")
    parser.add_argument("--concurrency", type=int, default=4, help="maximum parallel model requests")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--max-task-final-seconds", type=int,
        help="fail if any task final exceeds this many seconds (default: 120 for a shared task target)")
    args = parser.parse_args()
    if args.max_task_final_seconds is None and args.shared_task_target:
        args.max_task_final_seconds = 120
    if args.max_task_final_seconds is not None and args.max_task_final_seconds <= 0:
        parser.error("--max-task-final-seconds must be positive")
    if not os.environ.get(KEY_ENV):
        parser.error(f"set {KEY_ENV} before running the live test")
    if not (1 <= args.targets <= 32 and 1 <= args.messages <= 12 and 1 <= args.tasks <= 24
            and 0 <= args.quote_pairs <= 8 and 1 <= args.concurrency <= 16 and args.timeout > 0):
        parser.error("supported ranges are 1–32 targets, 1–12 messages, 1–24 tasks and 1–16 model requests")
    with tempfile.TemporaryDirectory(prefix="elephant-qq-stress-") as directory:
        result = asyncio.run(trial(args, Path(directory)))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
