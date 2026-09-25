from __future__ import annotations

import time
from dataclasses import dataclass, field

from rich.console import Console
from rich.live import Live
from rich.table import Table


@dataclass
class ProgressDashboard:
    """Low-overhead live stage dashboard; updates are throttled to avoid log I/O."""
    total_stages: int = 12
    console: Console = field(default_factory=Console)
    started: float = field(default_factory=time.monotonic)
    stage: str = "starting"
    stage_number: int = 0
    completed: int = 0
    total: int = 0
    api_metrics: dict[str, int] = field(default_factory=dict)
    llm_done: int = 0
    llm_total: int = 0
    label_success: int = 0
    label_failed: int = 0
    stage_started: float = field(default_factory=time.monotonic)
    _last_render: float = 0.0
    _live: Live | None = None
    _stage_timings: dict[str, float] = field(default_factory=dict)

    def __enter__(self):
        self._live = Live(self.render(), console=self.console, refresh_per_second=4)
        self._live.__enter__()
        return self

    def __exit__(self, *args):
        if self._live:
            self._live.__exit__(*args)

    def update(self, stage: str, number: int, completed: int = 0, total: int = 0, **metrics) -> None:
        previous_number = self.stage_number
        self.stage, self.stage_number = stage, number
        if number != previous_number:
            self.stage_started = time.monotonic()
        self.completed, self.total = completed, total
        self.api_metrics.update(metrics.get("api_metrics", {}))
        self.llm_done, self.llm_total = metrics.get("llm_done", self.llm_done), metrics.get("llm_total", self.llm_total)
        self.label_success, self.label_failed = metrics.get("label_success", self.label_success), metrics.get("label_failed", self.label_failed)
        now = time.monotonic()
        if self._live and now - self._last_render >= 0.2:
            self._last_render = now
            self._live.update(self.render())

    def stage_done(self, stage: str, elapsed: float) -> None:
        self._stage_timings[stage] = elapsed

    def callback(self, stage: str, number: int, total: int):
        completed = 0
        def tick(**metrics):
            nonlocal completed
            completed += 1
            self.update(stage, number, completed, total, **metrics)
        return tick

    def render(self):
        table = Table(title="Smartwallet pipeline", expand=True)
        table.add_column("Stage")
        table.add_column("Progress")
        table.add_column("Elapsed / ETA")
        table.add_column("API")
        pct = 100.0 * self.completed / self.total if self.total else 0.0
        elapsed = time.monotonic()-self.started
        stage_elapsed = time.monotonic() - self.stage_started
        eta = stage_elapsed * (100.0 / pct - 1.0) if pct > 0 else 0.0
        api = self.api_metrics
        table.add_row(f"[{self.stage_number}/{self.total_stages}] {self.stage}", f"{pct:5.1f}% ({self.completed}/{self.total})", f"{elapsed:,.1f}s / {eta:,.1f}s", f"{api.get('success',0)} ok, {api.get('cache_hit',0)} cache, {api.get('retry',0)} retry, {api.get('failed',0)} fail")
        return table
