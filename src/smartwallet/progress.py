from __future__ import annotations

import time
from dataclasses import dataclass, field

from rich.console import Console
from rich.live import Live
from rich.table import Table


@dataclass
class ProgressDashboard:
    """Low-overhead live stage dashboard; updates are throttled to avoid log I/O."""
    total_stages: int = 10
    console: Console = field(default_factory=Console)
    started: float = field(default_factory=time.monotonic)
    stage: str = "starting"
    stage_number: int = 0
    completed: int = 0
    total: int = 0
    _last_render: float = 0.0
    _live: Live | None = None

    def __enter__(self):
        self._live = Live(self.render(), console=self.console, refresh_per_second=4)
        self._live.__enter__()
        return self

    def __exit__(self, *args):
        if self._live:
            self._live.__exit__(*args)

    def update(self, stage: str, number: int, completed: int = 0, total: int = 0) -> None:
        self.stage, self.stage_number = stage, number
        self.completed, self.total = completed, total
        now = time.monotonic()
        if self._live and now - self._last_render >= 0.2:
            self._last_render = now
            self._live.update(self.render())

    def render(self):
        table = Table(title="Smartwallet pipeline", expand=True)
        table.add_column("Stage")
        table.add_column("Progress")
        table.add_column("Elapsed")
        pct = 100.0 * self.completed / self.total if self.total else 0.0
        table.add_row(f"[{self.stage_number}/{self.total_stages}] {self.stage}", f"{pct:5.1f}% ({self.completed}/{self.total})", f"{time.monotonic()-self.started:,.1f}s")
        return table
