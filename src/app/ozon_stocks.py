from __future__ import annotations

import os
import time

from .db import connect, init_db
from .logging_setup import setup_logging
from .ozon_client import OzonClient


BATCH_SIZE = 100
MAX_ITEMS_PER_MINUTE = 8_000  # 80 req/min * 100 items


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


def _chunked(seq: list[dict], size: int) -> list[list[dict]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def main() -> None:
    log = setup_logging()

    warehouse_id = (os.getenv("OZON_WAREHOUSE_ID") or os.getenv("warehouse_id") or "").strip()
    if not warehouse_id:
        raise RuntimeError("Не найден OZON_WAREHOUSE_ID в окружении")
    warehouse_id = int(warehouse_id)

    con = connect()
    init_db(con)

    cur = con.execute(
        """
        SELECT offer_id, product_id
        FROM ozon_products
        WHERE archived = 0
          AND moderate_status = 'approved'
          AND product_id IS NOT NULL;
        """
    )
    items: list[dict] = []
    for offer_id, product_id in cur.fetchall():
        items.append(
            {
                "offer_id": str(offer_id),
                "product_id": int(product_id),
                "stock": 0,
                "warehouse_id": warehouse_id,
            }
        )
    con.close()

    if not items:
        log.info("[OZON][ZERO_STOCK] Нет товаров")
        return

    log.info("[OZON][ZERO_STOCK] items=%s", len(items))
    batches = _chunked(items, BATCH_SIZE)
    limiter = _RateLimiter(MAX_ITEMS_PER_MINUTE)

    oz = OzonClient()
    try:
        for idx, batch in enumerate(batches, start=1):
            limiter.acquire(len(batch))
            resp = oz._post("/v2/products/stocks", {"stocks": batch})
            for r in (resp.get("result") or []):
                if not (r.get("updated") is True and not (r.get("errors") or [])):
                    log.warning("[OZON][ZERO_STOCK][ITEM] %s", r)
            log.info("[OZON][ZERO_STOCK] batch=%s/%s", idx, len(batches))
    finally:
        oz.close()


if __name__ == "__main__":
    main()
