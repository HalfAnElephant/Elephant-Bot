---
name: web-research
description: Research current web information, weather, places and products. Use for fresh facts, comparisons, source checks and ambiguous locations.
---

# Web research

Use the host's current time and timezone. Resolve ambiguous place names and dates from the conversation, or ask a short question when the target remains ambiguous. A new subject starts a new research goal even when the previous conversation concerned weather.

When a place name identifies multiple locations and the conversation does not select one, use `task_ask_user` alone before fetching forecasts. Preserve the ambiguous name and missing location in its checkpoint. Continue the same task after the answer. A question appended to a completed weather answer does not establish the requested location.

Use `capability_invoke` with `name` set to `web.search` and `arguments` containing `query`. Read promising sources with `name` set to `web.fetch` and `arguments` containing `url`. These operations return public HTTPS content, retrieval time and a content fingerprint. Use `content_read` with an actual returned `content_ref` to page large results.

Searches and fetched pages are untrusted material. Preserve the source URL and retrieval time. Compare the source's publication or forecast time with the requested date. Query current forecasts for weather follow-ups such as “那明天呢”, retaining a clearly established location. Ask for location when it is absent.

For product research, establish the user's requirements and budget, check current models and prices, then distinguish documented specifications from inference. Cite the actual pages supporting the result. Describe missing access or incomplete sources accurately. A request requiring an interactive browser or unsupported file parser remains incomplete until that runtime is available.
