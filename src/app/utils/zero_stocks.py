from __future__ import annotations

import os
import time
import sqlite3
from typing import List

from ..logging_setup import setup_logging
from ..ozon_client import OzonClient
from .telegram import TelegramNotifier

BATCH_SIZE = 100
MAX_ITEMS_PER_MINUTE = 8_000


class _RateLimiter:
    def __init__(self, max_items_per_minute: int) -> None:
        self.max_items_per_minute = max_items_per_minute
        self._window_start = time.monotonic()
        self._window_items = 0


    def acquire(self, items: int) -> None:
        while True:
            now = time.monotonic()
            elapsed = now - self._window_start

            if elapsed >= 60:
                self._window_start = now
                self._window_items = 0
                elapsed = 0

            if self._window_items + items <= self.max_items_per_minute:
                self._window_items += items
                return

            time.sleep(max(0.0, 60 - elapsed) + 0.05)


def _chunked(seq: List[dict], size: int) -> List[List[dict]]:
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def main() -> None:
    tg = TelegramNotifier()
    log = setup_logging()

    warehouse_id = (
        os.getenv("OZON_WAREHOUSE_ID")
        or os.getenv("warehouse_id")
        or ""
    ).strip()

    if not warehouse_id:
        raise RuntimeError(
            "Не найден warehouse_id. "
            "Добавь OZON_WAREHOUSE_ID в .env"
        )

    warehouse_id = int(warehouse_id)

    con = sqlite3.connect("data/app.db")
    cur = con.execute(
        """
        SELECT offer_id, product_id
        FROM ozon_products
        WHERE archived = 0
        AND moderate_status = 'approved'
        AND product_id IS NOT NULL;
        """
    )

    items = []
    for offer_id, product_id in cur.fetchall():
        items.append({
            "offer_id": str(offer_id),
            "product_id": int(product_id),
            "stock": 0,
            "warehouse_id": warehouse_id,
        })

    con.close()

    if not items:
        log.info("[OZON][ZERO_STOCK] Нет товаров для обновления")
        return

    log.info(
        "[OZON][ZERO_STOCK] Всего товаров для обнуления: %s",
        len(items),
    )

    batches = _chunked(items, BATCH_SIZE)
    limiter = _RateLimiter(MAX_ITEMS_PER_MINUTE)

    oz = OzonClient()
    try:
        total_ok = 0
        total_bad = 0

        for i, batch in enumerate(batches, start=1):
            limiter.acquire(len(batch))

            payload = {"stocks": batch}

            try:
                resp = oz._post("/v2/products/stocks", payload)
            except Exception as e:
                log.exception(
                    "[OZON][ZERO_STOCK] batch=%s ошибка: %s",
                    i, e
                )
                total_bad += len(batch)
                continue

            results = resp.get("result") or []

            for r in results:
                if r.get("updated") and not r.get("errors"):
                    total_ok += 1
                else:
                    total_bad += 1
                    log.warning("[OZON][ZERO_STOCK][ITEM] %s", r)

            log.info(
                "[OZON][ZERO_STOCK] batch=%s/%s ok=%s bad=%s",
                i, len(batches), total_ok, total_bad
            )

        log.info(
            "[OZON][ZERO_STOCK] Завершено ok=%s bad=%s",
            total_ok, total_bad
        )
        tg.send_message(
            f"Обнуление остатков завершено\n"
            f"Всего товаров: {len(items)}\n"
            f"Успешно: {total_ok}\n"
            f"С ошибками: {total_bad}"
        )
        print(f"Обнуление остатков завершено\nСообщение отправлено")

    finally:
        oz.close()


if __name__ == "__main__":
    main()