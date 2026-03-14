import os
from dataclasses import dataclass


@dataclass
class Settings:
    telegram_token: str
    openai_api_key: str
    database_path: str
    openai_base_url: str
    openai_model: str


def load_settings() -> Settings:
    telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    openai_api_key = os.getenv("OPENAI_API_KEY", "")
    database_path = os.getenv("DATABASE_PATH", "data/bot.sqlite3")
    openai_base_url = os.getenv("OPENAI_API_BASE", "")
    openai_model = os.getenv("OPENAI_MODEL", "gpt-4.1")

    if not telegram_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    if not openai_api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    os.makedirs(os.path.dirname(database_path), exist_ok=True)

    return Settings(
        telegram_token=telegram_token,
        openai_api_key=openai_api_key,
        database_path=database_path,
        openai_base_url=openai_base_url,
        openai_model=openai_model,
    )

