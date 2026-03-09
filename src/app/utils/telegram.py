
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Optional

import requests


@dataclass(frozen=True)
class TelegramTarget:
    chat_id: int
    message_thread_id: Optional[int] = None


def _parse_topic_link(link: str) -> Optional[TelegramTarget]:
    link = (link or "").strip()
    if not link:
        return None

    m = re.search(r"t\.me/c/(\d+)/(\d+)", link)
    if m:
        internal = m.group(1)
        msg_id = int(m.group(2))
        chat_id = int(f"-100{internal}")
        return TelegramTarget(chat_id=chat_id, message_thread_id=msg_id)

    m = re.search(r"t\.me/[^/]+/(\d+)", link)
    if m:
        msg_id = int(m.group(1))
        return TelegramTarget(chat_id=0, message_thread_id=msg_id)

    return None


def load_telegram_target() -> Optional[TelegramTarget]:
    raw_chat_id = (
        os.getenv("tg_chat_id")
        or os.getenv("TG_CHAT_ID")
        or os.getenv("TELEGRAM_CHAT_ID")
        or ""
    ).strip()
    raw_thread_id = (
        os.getenv("tg_topic_id")
        or os.getenv("TG_MESSAGE_THREAD_ID")
        or os.getenv("TELEGRAM_MESSAGE_THREAD_ID")
        or ""
    ).strip()

    if raw_chat_id:
        try:
            chat_id = int(raw_chat_id)
        except Exception:
            chat_id = 0
        if raw_thread_id:
            try:
                thread_id = int(raw_thread_id)
            except Exception:
                thread_id = None
            return TelegramTarget(chat_id=chat_id, message_thread_id=thread_id)
        return TelegramTarget(chat_id=chat_id, message_thread_id=None)

    link = (os.getenv("TG_TOPIC_LINK") or os.getenv("TELEGRAM_TOPIC_LINK") or "").strip()
    tgt = _parse_topic_link(link)
    if tgt is None:
        return None

    if tgt.chat_id == 0 and raw_chat_id:
        try:
            return TelegramTarget(chat_id=int(raw_chat_id), message_thread_id=tgt.message_thread_id)
        except Exception:
            return None

    if tgt.chat_id == 0:
        return None

    return tgt


class TelegramNotifier:
    def __init__(self, *, token: Optional[str] = None, target: Optional[TelegramTarget] = None, timeout: float = 30.0):
        self.token = (token or os.getenv("tg_bot_token") or os.getenv("telegram_bot_token") or "").strip()
        self.target = target or load_telegram_target()
        self.timeout = timeout

    def enabled(self) -> bool:
        return bool(self.token and self.target and self.target.chat_id)

    def _api_url(self, method: str) -> str:
        return f"https://api.telegram.org/bot{self.token}/{method}"

    def send_message(self, text: str) -> None:
        if not self.enabled():
            return
        payload = {"chat_id": self.target.chat_id, "text": text}
        if self.target.message_thread_id is not None:
            payload["message_thread_id"] = self.target.message_thread_id
        requests.post(self._api_url("sendMessage"), json=payload, timeout=self.timeout).raise_for_status()

    def send_document(self, file_path: str, *, caption: Optional[str] = None) -> None:
        if not self.enabled():
            return
        data = {"chat_id": str(self.target.chat_id)}
        if caption:
            data["caption"] = caption
        if self.target.message_thread_id is not None:
            data["message_thread_id"] = str(self.target.message_thread_id)

        with open(file_path, "rb") as f:
            files = {"document": (os.path.basename(file_path), f)}
            requests.post(self._api_url("sendDocument"), data=data, files=files, timeout=self.timeout).raise_for_status()
