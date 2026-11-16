"""Process-injected mail fixtures; no mailbox or network configuration is exposed to tools."""
from dataclasses import dataclass


@dataclass(frozen=True)
class MailBinding:
    owner: str
    principal: str
    conversation: str
    workspace: str
    account_id: str
    folder: str


@dataclass(frozen=True)
class SyntheticMessage:
    uid: int
    sender_address: str
    subject: str
    body: str
    body_complete: bool = True


class SyntheticMailSource:
    def __init__(self, binding: MailBinding, *, baseline: int):
        self.binding = binding
        self.baseline = baseline
        self.messages: list[SyntheticMessage] = []
        self.enabled = True

    def allows(self, binding: MailBinding):
        return self.enabled and self.binding == binding

    def watermark(self):
        return max((self.baseline, *(message.uid for message in self.messages)))

    def read(self):
        """No raw email data is ever passed through to the task."""
        return tuple(self.messages)
