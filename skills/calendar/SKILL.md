---
name: calendar
description: Read and modify the requester's configured iCloud calendar through the trusted account adapter. Use for calendars, events and scheduling.
compatibility: Requires the host iCloud calendar account configuration.
---

# Calendar work

Use `capability_invoke` with `name` set to `icloud.calendar`. Its `arguments.operation` selects `calendars`, `list`, `get`, `create`, `update` or `delete`. Identity and account authorization are enforced by the host. Missing account configuration means the operation is unavailable.

Discover calendars with `operation` set to `calendars`. Keep the returned `calendar_id`. List events within an explicit bounded `start` and `end` date range. Read an event with its exact `calendar_id` and returned `event_id`. Clarify ambiguous calendar names, local dates or conflicting event matches.

Create or update only the user's requested event. Fields include `summary`, ISO `start` and exclusive `end`, IANA `timezone`, `description`, `location` and `recurrence`. Use an ISO date for all-day events and an offset-aware datetime for timed events. Confirm whether a recurring-series edit is intended before applying a whole-resource recurrence update.

Preserve remote event IDs and the host operation ID. Require actual remote readback to establish the result. For UNKNOWN use `operation.list` and `operation.reconcile` with the existing operation ID, retaining the uncertain state until evidence resolves it. Never convert a successful command or a model statement into a verified remote effect.
