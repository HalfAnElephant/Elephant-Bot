---
name: document-report
description: Produce requested documents, reports and data files in the task workspace, verify their contents and export actual artifacts.
---

# Document and report production

Establish the requested format, audience, source material and destination. Use read to inspect supplied workspace files. Read `/skills/workspace-dev/SKILL.md` before running a generator or bundled script.

Use installed runtimes and libraries. Check imports and available renderers explicitly. Plain text, Markdown, CSV and JSON can use standard-library generation. Office and PDF work requires the corresponding installed writer, parser and renderer. Report the exact missing dependency when the requested format cannot be produced in the current environment.

Generate the complete requested artifact under `outputs/`. Validate structured file contents programmatically. For layout-sensitive documents, render and inspect the output with an available renderer before claiming visual acceptance. Preserve citations, supplied numbers and the user's format requirements.

Pass existing relative output paths to `task_outcome` with `attachments`. The host archives and hashes these files. Report the actual generation and verification result. QQ attachment delivery depends on the platform delivery path, so an archived artifact's state must remain distinct from a platform receipt.
