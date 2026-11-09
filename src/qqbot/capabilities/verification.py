"""GitHub readback evidence, separate from command exit status."""
import json
import re
from datetime import UTC, datetime

from .github import flag_values


class GitHubVerifier:
    def __init__(self, cli):
        self.cli = cli

    async def _api(self, context, endpoint):
        result = await self.cli({"argv": ["api", endpoint, "--method", "GET"]}, context)
        if result["exit_code"] or result.get("truncated"):
            raise ValueError("GitHub readback failed or was truncated")
        return json.loads(result["stdout"])

    def _number(self, arguments):
        argv = arguments.get("argv", [])
        if len(argv) >= 3 and argv[2].isdigit():
            return argv[2]
        match = next((re.search(r"/(?:pull|issues)/(\d+)", item) for item in argv
                      if re.search(r"/(?:pull|issues)/(\d+)", item)), None)
        if match:
            return match[1]
        if len(argv) > 1 and argv[0] == "api":
            match = re.search(r"/(?:pulls|issues|runs)/(\d+)", argv[1])
            return match[1] if match else None
        return None

    def _body(self, arguments):
        argv = arguments.get("argv", [])
        if argv[:1] == ["api"]:
            values = flag_values(argv, "--field", "--raw-field", "-f", "-F")
            body = next((value.split("=", 1)[1] for value in values if value.startswith("body=")), None)
            if flag_values(argv, "--input") == ["-"]:
                try:
                    body = json.loads(arguments.get("stdin", "")).get("body", body)
                except (ValueError, AttributeError):
                    pass
            return body
        values = flag_values(argv, "--body", "-b")
        return arguments.get("stdin", values[-1] if values else None)

    async def prepare(self, context, action, arguments):
        if not action.name.startswith("github.") or action.effect == "read":
            return {}
        actor = await self._api(context, "user")
        observation = {"actor": actor["login"], "observed_at": datetime.now(UTC).isoformat()}
        repository = next((r.removeprefix("repo:" + context.host + "/") for r in action.resources
                           if r.startswith("repo:" + context.host + "/")), context.repository)
        observation["repository"] = repository
        number = self._number(arguments)
        if number and repository and (".review." in action.name or action.name == "github.pr.merge"):
            pull = await self._api(context, f"repos/{repository}/pulls/{number}")
            observation.update(number=number, head_sha=pull["head"]["sha"])
            expected = arguments.get("reviewed_head_sha")
            if expected and expected != pull["head"]["sha"]:
                raise ValueError("PR head changed since the reviewed observation")
            if ".review." in action.name:
                reviews = await self._api(context, f"repos/{repository}/pulls/{number}/reviews?per_page=100")
                observation["review_ids"] = [review["id"] for review in reviews]
        if action.name == "github.run.rerun":
            argv = arguments["argv"]
            if len(argv) > 2 and argv[2].isdigit() and repository:
                run = await self._api(context, f"repos/{repository}/actions/runs/{argv[2]}")
                observation.update(number=argv[2], run_attempt=run["run_attempt"])
        return observation

    async def __call__(self, context, action, arguments, data):
        if action.name.startswith("workspace.process."):
            if data.get("process_ref"):
                return "VERIFIED", {**data, "effect": "process_started" if action.name.endswith("start") else "process_stopped"}
            return None
        if action.name.startswith("git."):
            if data.get("verified"):
                return "VERIFIED", data
            return None
        if action.name == "workspace.file.write":
            return "VERIFIED", data
        if not action.name.startswith("github."):
            return None
        pre = arguments.get("_preconditions", {})
        repository, number = pre.get("repository"), pre.get("number")
        actor = pre.get("actor")
        if not repository or not actor:
            return None
        if ".review." in action.name and number:
            reviews = await self._api(context, f"repos/{repository}/pulls/{number}/reviews?per_page=100")
            state = {"approve": "APPROVED", "request_changes": "CHANGES_REQUESTED", "comment": "COMMENTED"}.get(
                action.name.rsplit(".", 1)[-1])
            candidates = [review for review in reviews if review["id"] not in pre.get("review_ids", [])
                          and review.get("user", {}).get("login") == actor
                          and review.get("state") == state and review.get("submitted_at")
                          and review.get("commit_id") == pre.get("head_sha")]
            body = self._body(arguments)
            if body is not None:
                candidates = [review for review in candidates if (review.get("body") or "").strip() == body.strip()]
            if len(candidates) != 1:
                return None
            pull = await self._api(context, f"repos/{repository}/pulls/{number}")
            return "VERIFIED", {**data, "review": candidates[0], "reviewed_head_sha": pre["head_sha"],
                                "current_head_sha": pull["head"]["sha"],
                                "head_changed": pull["head"]["sha"] != pre["head_sha"]}
        if action.name == "github.pr.merge" and number:
            pull = await self._api(context, f"repos/{repository}/pulls/{number}")
            if pull.get("merged"):
                return "VERIFIED", {**data, "pull": pull, "effect": "merged"}
            if pull.get("auto_merge"):
                return "VERIFIED", {**data, "pull": pull, "effect": "auto_merge_enabled"}
            return None
        if action.name == "github.run.rerun" and number:
            run = await self._api(context, f"repos/{repository}/actions/runs/{number}")
            if run.get("run_attempt", 0) > pre["run_attempt"]:
                return "VERIFIED", {**data, "run": run, "effect": "rerun_accepted"}
            return None
        if action.name in {"github.issue.create", "github.pr.create"}:
            matches = re.findall(r"https://" + re.escape(context.host) + r"/([^\s/]+/[^\s/]+)/(issues|pull)/(\d+)",
                                 data.get("stdout", ""))
            matches = [match for match in matches if match[0] == repository]
            if len(matches) != 1:
                return None
            repo, kind, number = matches[0]
            endpoint = "pulls" if kind == "pull" else "issues"
            created = await self._api(context, f"repos/{repo}/{endpoint}/{number}")
            if created.get("user", {}).get("login") != actor or not created.get("id"):
                return None
            if created.get("created_at", "") < pre["observed_at"][:19] + "Z":
                return None
            body = self._body(arguments)
            if body is not None and (created.get("body") or "").strip() != body.strip():
                return None
            return ("VERIFIED" if data.get("exit_code") == 0 else "PARTIAL"), {**data, "created": created}
        return None
