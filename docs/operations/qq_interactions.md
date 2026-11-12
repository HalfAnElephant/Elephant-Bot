# QQ official keyboards, callbacks, menus and panels

## Scope

The official adapter can serialize typed QQ keyboards. The application-owned callback service deliberately supports only the read-only `help` and `status` actions. It does **not** interpret callback data as commands or model instructions, and a click never approves a task, permission change or tool execution.

The authenticated endpoint `POST /api/v1/qq/{instance_id}/buttons` creates an expiring keyboard for a specific `target` and `principal`. It returns a `keyboard` object for use in an outbound official message; issuing it does not send a message. `actions` defaults to `["status", "help"]`, `ttl_seconds` defaults to 300 (maximum 3600), and an optional `group_key` makes the actions mutually exclusive in the database. An administrator must explicitly send the resulting structured message through the ordinary Outbox. Tokens have no authority outside the receiving app, adapter, conversation and principal.

Schema migration 0010 persists hashed tokens and interaction receipts. Token consumption and mutual exclusion occur in one SQLite write transaction. Replayed event IDs do not dispatch another action. After a worker claims an action, its state becomes RUNNING; an interrupted action is not blindly re-executed. Token expiration is enforced even when an old signed event is replayed. Server authorization is still required even if the QQ client hides a button from other users.

The platform `INTERACTION_CREATE` body must pass the adapter's signature verification before `parse_interaction` is called. The parser checks AppID, scene identities, matching event/data types and timestamp format. The business token is opaque random data. The acknowledgement uses the inner `d.id`, not the outer event ID, with a bounded single PUT. An uncertain ACK is not automatically repeated. Only types 11 and 12 require ACK; other interaction types are not treated as application commands.

## Menus and command panels

These are explicit administrator operations, protected by the existing admin authentication and permission service. Nothing is created, overwritten or deleted at startup.

| Local administrator endpoint | Official operation |
| --- | --- |
| GET/PUT `/api/v1/qq/{instance_id}/menu` | GET/PUT `/v2/menu` |
| GET/POST `/api/v1/qq/{instance_id}/panels` | GET/POST `/v2/panels` |
| GET/PUT/DELETE `/api/v1/qq/{instance_id}/panels/{panel_id}` | corresponding `/v2/panels/{panel_id}` |
| PUT `/api/v1/qq/{instance_id}/panels/{panel_id}/target` | PUT `/v2/panels/{panel_id}/target` |

The menu update body is `{"menu":{"items":[...]}}`. It replaces the complete global C2C menu. Top-level menu items support `send_message`, `link`, `switch` or one-level `menu`. Command panels accept `scope` (`c2c`, `group`, `channel`, `dm`), `target_type` and a `panel` with `items`. Only C2C and group support `specific` targets. The target update endpoint fetches the existing panel first to verify its scope and rejects global panels or mismatched user/group target arrays.

Display length and item-count limits are validated before the request. Link actions are restricted to HTTPS in menus/panels. Menu text sent by a user goes through the existing command permission checks; `only_admin` is a platform display restriction, not a local permission grant.

Read the current remote menu/panel before replacement and keep that returned snapshot with your deployment configuration. A write that returns `OUTCOME_UNKNOWN` may have taken effect: query current state before retrying. No automatic write retry or automatic reconciliation is performed. `RATE_LIMITED` is surfaced for manual retry. The API wrapper does not log raw upstream error bodies, credentials, or authorization headers.

## Verified protocol sources

Checked 2026-11-12:

- [Keyboard fields](https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_users_user_openid_messages.post.html)
- [Interaction event](https://bot.q.qq.com/wiki/develop/api-v2/autogen/event/interaction_create.html)
- [Interaction ACK](https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/interactions_interaction_id.put.html)
- [Global menu update](https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_menu.put.html)
- [Panel create](https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_panels.post.html)
- [Panel list](https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_panels.get.html)
- [Panel detail](https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_panels_panel_id.get.html)
- [Panel update](https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_panels_panel_id.put.html)
- [Panel target update](https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_panels_panel_id_target.put.html)

Unit and transport contract tests use mocks. Real permission, moderation, menu rendering and cross-client acceptance still require an authorized QQ application and a controlled recipient. Conservative keyboard row/column engineering bounds are not a claim about every account's platform limit. Mini-program jump buttons and arbitrary business callback actions are not yet supported by this safe application dispatcher.

## Scoped workflow buttons

`workflow_keyboard` and the authenticated buttons API's `workflow_buttons` field support bounded application workflows:
- `choice`: `payload.value`, optional `task_id` and `question_id`. A task choice enters the existing task-input service with current ownership/permission checks; plain choices return a receipt only.
- `page`: `payload.pages` (up to 20 pages, 4000 characters each), `page` index. Navigation issues fresh scoped single-use buttons, not arbitrary commands.
- `cancel_prompt`: `payload.task_id`. Reads the visible task and checks cancellation permission, then issues a separate confirmation/cancel pair. Only the server can issue the final `cancel_task` button through the public helpers/API.
- `dismiss`: closes the pending confirmation with no task mutation.

Every callback ACK has a 2.5-second total deadline including token refresh. A failed/unknown ACK cannot execute the action; duplicate callbacks can recover pending ACKs only while the original capability remains valid. Permission is checked again after ACK and before queueing the reply; TaskService independently authorizes task input/cancellation. Concurrent/repeated clicks cannot claim an action twice. Crashes after claim deliberately remain non-replayed rather than risking duplicate effects. These workflows are not blanket approval for payments, commands, or unrelated tasks.
Task-directed workflow buttons additionally require the clicker's principal to be the task's original requester in the same conversation, even when group-level task visibility is broader. This release does not implement arbitrary workflow callbacks, financial/security approvals, dynamic database pagination, or general form submission. A `choice` is ordinary user input, not a reusable privilege grant.
