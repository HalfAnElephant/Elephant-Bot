from datetime import UTC, datetime, timedelta


class Clock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class FakeClock(Clock):
    def __init__(self, now: datetime | None = None):
        self.value = now or datetime(2026, 11, 8, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)
