from __future__ import annotations

from pydantic import BaseModel
from dotenv import load_dotenv
import os

load_dotenv()


class Settings(BaseModel):
    ozon_base_url: str = os.getenv("OZON_BASE_URL", "https://api-seller.ozon.ru")
    ozon_client_id: str = os.getenv("OZON_CLIENT_ID", os.getenv("client_id", "")).strip()
    ozon_api_key: str = os.getenv("OZON_API_KEY", os.getenv("token", "")).strip()

    ozon_timeout_sec: float = float(os.getenv("OZON_TIMEOUT_SEC", "30"))
    ozon_limit_per_page: int = int(os.getenv("OZON_LIMIT_PER_PAGE", "1000"))
    
    autorus_state_path: str = os.getenv("AUTORUS_STATE_PATH", "state_autorus.json").strip()
    autorus_allow_autologin: bool = os.getenv("AUTORUS_ALLOW_AUTOLOGIN", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }

    def validate_required(self) -> None:
        missing = []
        if not self.ozon_client_id:
            missing.append("OZON_CLIENT_ID (или client_id)")
        if not self.ozon_api_key:
            missing.append("OZON_API_KEY (или token)")
        if missing:
            raise RuntimeError(f"Отсутствуют переменные окружения: {', '.join(missing)}")


settings = Settings()
settings.validate_required()
