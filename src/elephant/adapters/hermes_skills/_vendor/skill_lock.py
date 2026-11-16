# ruff: noqa
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nous Research
# Selected upstream skill_refusal from Hermes 67664e236ddcc45824b871d575677287599af152.
def skill_refusal(name: str) -> str:
    return (f"Skill '{name}' is locked by the user, so the agent cannot change it: skill_manage, "
            f"write_file, patch and terminal all refuse. Don't retry or work around it (no copying it "
            f"under another name); tell the user what you would change. They can unlock it with "
            f"`hermes curator unlock {name}` or the lock switch in Desktop.")
