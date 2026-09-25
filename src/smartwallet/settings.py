from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import find_dotenv, load_dotenv


@dataclass(frozen=True)
class Settings:
    hub_base_url: str
    api_hub_key: str
    llm_api_key: str
    llm_base_url: str
    llm_model: str
    db_path: Path
    raw_dir: Path
    report_dir: Path
    http_timeout: float
    http_retries: int
    concurrency: int
    episode_gap_seconds: int
    max_episode_events: int
    bridge_check_min_usd: float

    @classmethod
    def load(cls, env_file: str | Path = ".env") -> "Settings":
        env_path = Path(env_file)
        if env_path == Path(".env") and not env_path.exists():
            found = find_dotenv(".env", usecwd=True)
            if found:
                env_path = Path(found)
        load_dotenv(dotenv_path=env_path, override=False)
        hub_key = os.getenv("API_HUB_KEY", "").strip()
        if not hub_key:
            raise RuntimeError("API_HUB_KEY is required in the environment or .env")
        return cls(
            hub_base_url="https://hub.arbitron.dev",
            api_hub_key=hub_key,
            llm_api_key=os.getenv("FREE_LLM_API", "").strip(),
            llm_base_url=os.getenv("FREE_LLM_BASE_URL", "http://159.194.241.69:3001/v1").rstrip("/"),
            llm_model=os.getenv("FREE_LLM_MODEL", "llama-3.3-70b-versatile").strip(),
            db_path=Path(os.getenv("SMARTWALLET_DB", "data/smartwallet.db")),
            raw_dir=Path(os.getenv("SMARTWALLET_RAW_DIR", "data/raw")),
            report_dir=Path(os.getenv("SMARTWALLET_REPORT_DIR", "data/reports")),
            http_timeout=float(os.getenv("SMARTWALLET_HTTP_TIMEOUT", "60")),
            http_retries=max(1, int(os.getenv("SMARTWALLET_HTTP_RETRIES", "4"))),
            concurrency=max(1, int(os.getenv("SMARTWALLET_CONCURRENCY", "8"))),
            episode_gap_seconds=max(60, int(os.getenv("SMARTWALLET_EPISODE_GAP_SECONDS", "21600"))),
            max_episode_events=max(2, int(os.getenv("SMARTWALLET_MAX_EPISODE_EVENTS", "50"))),
            bridge_check_min_usd=float(os.getenv("SMARTWALLET_BRIDGE_CHECK_MIN_USD", "100000")),
        )
