"""GitHub readback evidence, separate from command exit status."""
import asyncio
import json
import re
from datetime import UTC, datetime
from urllib.parse import quote, urlencode, urlsplit

from .github import api_payload, flag_values, normalize_argv


class GitHubVerifier:
    def __init__(self, cli):
        self.cli = cli

    async def _api(self, context, endpoint, *, paginate=False):
        argv = ["api", endpoint, "--method", "GET"]
        if paginate:
            argv.append("--paginate")
        result = await self.cli({"argv": argv}, context)
        if result["exit_code"] or result.get("truncated"):
            raise ValueError("GitHub readback failed or was truncated")
        if not paginate:
            return json.loads(result["stdout"])
        # Older supported gh releases emit one JSON document per page without --slurp.
        pages, remaining = [], result["stdout"].strip()
        decoder = json.JSONDecoder()
        while remaining:
            page, consumed = decoder.raw_decode(remaining)
            pages.append(page)
            remaining = remaining[consumed:].lstrip()
        return pages

    def _number(self, arguments):
        argv = normalize_argv(arguments.get("argv", []))
        if argv[:1] == ["api"]:
            match = re.search(r"/(?:pulls|issues|runs)/(\d+)(?:/|$)", urlsplit(argv[1]).path)
            return match[1] if match else None
        targets = [item for item in argv[2:] if not item.startswith("-")]
        if len(targets) != 1:
            return None
        if targets[0].isdigit():
            return targets[0]
        match = re.search(r"/(?:pull|issues)/(\d+)(?:/|$)", urlsplit(targets[0]).path)
        return match[1] if match else None

    def _body(self, arguments):
        argv = normalize_argv(arguments.get("argv", []))
        if argv[:1] == ["api"]:
            payload = api_payload(argv, arguments.get("stdin", ""))
            return payload.get("body") if payload else None
        if flag_values(argv, "--body-file") == ["-"]:
            return arguments.get("stdin", "")
        values = flag_values(argv, "--body", "--comment")
        if values:
            return values[-1]
        return arguments.get("stdin") if argv[1:2] == ["review"] else None

    def _requested(self, action, arguments):
        argv = normalize_argv(arguments["argv"])
        operation = action.name.rsplit(".", 1)[-1]
        if argv[:1] == ["api"]:
            fields = api_payload(argv, arguments.get("stdin", ""))
        else:
            allowed = {"--repo", "--body", "--body-file", "--title"} if operation == "edit" else {
                "--repo", "--body", "--body-file"} if operation == "comment" else {"--repo", "--comment"}
            if action.name == "github.issue.close":
                allowed.add("--reason")
            options = {item.partition("=")[0] for item in argv[2:] if item.startswith("-")}
            if options - allowed:
                return None
            fields = {}
            body = self._body(arguments)
            if body is not None:
                fields["body" if operation in {"edit", "comment"} else "comment"] = body
            titles = flag_values(argv, "--title")
            if titles:
                fields["title"] = titles[-1]
            if operation in {"close", "reopen"}:
                fields["state"] = "closed" if operation == "close" else "open"
            reasons = flag_values(argv, "--reason")
            if reasons:
                reason = reasons[-1].replace(" ", "_")
                if reason not in {"completed", "not_planned"}:
                    return None
                fields["state_reason"] = reason
        allowed = {"body", "title"} if operation == "edit" else {"body"} if operation == "comment" else {
            "state", "state_reason", "comment"}
        if not fields or set(fields) - allowed or any(not isinstance(value, str) for value in fields.values()):
            return None
        return fields

    async def _comments(self, context, repository, number):
        pages = await self._api(context, f"repos/{repository}/issues/{number}/comments?per_page=100", paginate=True)
        if not isinstance(pages, list) or any(not isinstance(page, list) for page in pages):
            raise ValueError("GitHub comment readback was not a complete paginated list")
        return [item for page in pages for item in page]

    async def _prepare_dispatch(self, context, arguments, observation):
        argv = normalize_argv(arguments["argv"])
        repository = observation["repository"]
        if not repository:
            return
        if argv[:1] == ["api"]:
            match = re.search(r"/actions/workflows/([^/]+)/dispatches$", urlsplit(argv[1]).path)
            payload = api_payload(argv, arguments.get("stdin", ""))
            workflow = match[1] if match else None
            ref = payload.get("ref") if payload else None
        else:
            targets = [item for item in argv[2:] if not item.startswith("-")]
            workflow = targets[0] if len(targets) == 1 else None
            refs = flag_values(argv, "--ref")
            ref = refs[-1] if refs else None
        if not workflow or not (workflow.isdigit() or re.fullmatch(r"[\w.-]+\.ya?ml", workflow)):
            return
        workflow_data = await self._api(context, f"repos/{repository}/actions/workflows/{quote(workflow, safe='')}")
        if ref is None:
            ref = (await self._api(context, f"repos/{repository}"))["default_branch"]
        if not isinstance(ref, str) or not ref:
            return
        commit = await self._api(context, f"repos/{repository}/commits/{quote(ref, safe='')}")
        sha = commit.get("sha")
        if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", sha):
            return
        query = urlencode({"event": "workflow_dispatch", "actor": observation["actor"],
                           "branch": ref.removeprefix("refs/heads/").removeprefix("refs/tags/"),
                           "head_sha": sha, "per_page": 100})
        endpoint = f"repos/{repository}/actions/workflows/{workflow_data['id']}/runs?{query}"
        runs = await self._api(context, endpoint)
        if not isinstance(runs.get("workflow_runs"), list) or runs.get("total_count", 0) > len(runs["workflow_runs"]):
            return
        observation.update(workflow_id=workflow_data["id"], dispatch_ref=ref,
                           head_sha=sha, runs_endpoint=endpoint,
                           run_ids=[run["id"] for run in runs["workflow_runs"]])

    async def prepare(self, context, action, arguments):
        if not action.name.startswith("github.") or action.effect == "read":
            return {}
        actor = await self._api(context, "user")
        observation = {"actor": actor["login"], "observed_at": datetime.now(UTC).isoformat()}
        repository = next((r.removeprefix("repo:" + context.host + "/") for r in action.resources
                           if r.startswith("repo:" + context.host + "/")), context.repository)
        observation["repository"] = repository
        number = self._number(arguments)
        if action.name == "github.workflow.run":
            await self._prepare_dispatch(context, arguments, observation)
        if number and repository and action.name in {
                "github.issue.comment", "github.pr.comment", "github.issue.edit", "github.pr.edit",
                "github.issue.close", "github.pr.close", "github.issue.reopen", "github.pr.reopen"}:
            requested = self._requested(action, arguments)
            if requested:
                endpoint = "pulls" if action.name.startswith("github.pr.") else "issues"
                previous = await self._api(context, f"repos/{repository}/{endpoint}/{number}")
                observation.update(number=number, requested=requested, target_id=previous["id"],
                                   before={key: previous.get(key) for key in requested if key != "comment"})
                if action.name.endswith(".comment") or "comment" in requested:
                    comments = await self._comments(context, repository, number)
                    observation["comment_ids"] = [comment["id"] for comment in comments]
        if number and repository and (".review." in action.name or action.name == "github.pr.merge"):
            pull = await self._api(context, f"repos/{repository}/pulls/{number}")
            observation.update(number=number, head_sha=pull["head"]["sha"])
            expected = arguments.get("reviewed_head_sha")
            if expected and expected != pull["head"]["sha"]:
                raise ValueError("PR head changed since the reviewed observation")
            if ".review." in action.name:
                reviews = await self._api(context, f"repos/{repository}/pulls/{number}/reviews?per_page=100")
                observation["review_ids"] = [review["id"] for review in reviews]
        if action.name == "github.run.rerun" and number and repository:
            run = await self._api(context, f"repos/{repository}/actions/runs/{number}")
            observation.update(number=number, run_attempt=run["run_attempt"])
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
        if action.name == "github.workflow.run" and pre.get("workflow_id"):
            candidates = []
            for attempt in range(3):
                result = await self._api(context, pre["runs_endpoint"])
                runs = result.get("workflow_runs", [])
                if result.get("total_count", 0) > len(runs):
                    return None
                new = [run for run in runs if run["id"] not in pre["run_ids"]]
                candidates = [run for run in new if run.get("workflow_id") == pre["workflow_id"]
                              and run.get("event") == "workflow_dispatch"
                              and run.get("actor", {}).get("login", "").casefold() == actor.casefold()
                              and run.get("head_sha") == pre["head_sha"]
                              and run.get("head_branch") == pre["dispatch_ref"].removeprefix("refs/heads/").removeprefix("refs/tags/")
                              and run.get("created_at", "") >= pre["observed_at"][:19] + "Z"]
                if new or attempt == 2:
                    break
                await asyncio.sleep(0.5)
            if len(candidates) != 1:
                return None
            reported = re.findall(r"https://" + re.escape(context.host) + "/" + re.escape(repository)
                                  + r"/actions/runs/(\d+)", data.get("stdout", ""), re.IGNORECASE)
            try:
                response = json.loads(data.get("stdout", ""))
                if isinstance(response, dict) and response.get("workflow_run_id"):
                    reported.append(str(response["workflow_run_id"]))
            except ValueError:
                pass
            if reported and set(reported) != {str(candidates[0]["id"])}:
                return None
            return ("VERIFIED" if data.get("exit_code") == 0 else "PARTIAL"), {
                **data, "run": candidates[0], "effect": "dispatch_accepted", "head_sha": pre["head_sha"],
            }
        if pre.get("requested") and number:
            requested = pre["requested"]
            endpoint = "pulls" if action.name.startswith("github.pr.") else "issues"
            target = await self._api(context, f"repos/{repository}/{endpoint}/{number}")
            if target.get("id") != pre.get("target_id"):
                return None
            is_comment = action.name.endswith(".comment")
            fields = {} if is_comment else {key: value for key, value in requested.items() if key != "comment"}
            if any((target.get(key) or "") != value for key, value in fields.items()):
                return None
            comment = requested.get("body") if is_comment else requested.get("comment")
            confirmed = None
            if comment is not None:
                comments = await self._comments(context, repository, number)
                candidates = [item for item in comments if item.get("id") not in pre.get("comment_ids", [])
                              and item.get("user", {}).get("login", "").casefold() == actor.casefold()
                              and (item.get("body") or "").strip() == comment.strip()
                              and item.get("created_at", "") >= pre["observed_at"][:19] + "Z"]
                if len(candidates) != 1:
                    return None
                confirmed = candidates[0]
            if data.get("exit_code") != 0 and not confirmed and all(pre["before"].get(key) == value for key, value in fields.items()):
                return None
            return ("VERIFIED" if data.get("exit_code") == 0 else "PARTIAL"), {
                **data, "target": target, "confirmed_fields": fields,
                **({"comment": confirmed} if confirmed else {}),
            }
        if ".review." in action.name and number:
            reviews = await self._api(context, f"repos/{repository}/pulls/{number}/reviews?per_page=100")
            state = {"approve": "APPROVED", "request_changes": "CHANGES_REQUESTED", "comment": "COMMENTED"}.get(
                action.name.rsplit(".", 1)[-1])
            candidates = [review for review in reviews if review["id"] not in pre.get("review_ids", [])
                          and review.get("user", {}).get("login", "").casefold() == actor.casefold()
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
            expected_kind = "pull" if action.name == "github.pr.create" else "issues"
            matches = [match for match in matches if match[0].casefold() == repository.casefold() and match[1] == expected_kind]
            if len(matches) != 1:
                return None
            repo, kind, number = matches[0]
            endpoint = "pulls" if kind == "pull" else "issues"
            created = await self._api(context, f"repos/{repo}/{endpoint}/{number}")
            if created.get("user", {}).get("login", "").casefold() != actor.casefold() or not created.get("id"):
                return None
            expected_url = f"https://{context.host}/{repository}/{kind}/{number}"
            if created.get("html_url", "").casefold() != expected_url.casefold():
                return None
            if created.get("created_at", "") < pre["observed_at"][:19] + "Z":
                return None
            body = self._body(arguments)
            if body is not None and (not isinstance(body, str) or (created.get("body") or "").strip() != body.strip()):
                return None
            argv = normalize_argv(arguments["argv"])
            titles = flag_values(argv, "--title") if argv[:1] != ["api"] else []
            payload = api_payload(argv, arguments.get("stdin", "")) if argv[:1] == ["api"] else {}
            title = payload.get("title") if payload else (titles[-1] if titles else None)
            if title is not None and created.get("title") != title:
                return None
            return ("VERIFIED" if data.get("exit_code") == 0 else "PARTIAL"), {**data, "created": created}
        return None
