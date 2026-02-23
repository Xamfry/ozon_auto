from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Optional, Sequence

import sqlite3

from .logging_setup import setup_logging
from .ozon_client import OzonClient


@dataclass(frozen=True)
class PriceUpdateItem:
    offer_id: str
    product_id: int
    price: int


@dataclass(frozen=True)
class StockUpdateItem:
    offer_id: str
    product_id: int
    stock: int
    warehouse_id: int


def _chunked(seq: Sequence, size: int) -> list[list]:
    return [list(seq[i : i + size]) for i in range(0, len(seq), size)]


class _RateLimiter:
    """Ограничение по количеству товаров в минуту (items/min)."""

    def __init__(self, max_items_per_minute: int) -> None:
        self.max_items_per_minute = max_items_per_minute
        self._window_start = time.monotonic()
        self._window_items = 0

    def acquire(self, items: int) -> None:
        if items <= 0:
            return

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


class OzonUpdater(OzonClient):
    """
    Обновление цен/остатков.

    Используем:
    - POST /v1/product/import/prices (до 1000 товаров за запрос)
    - POST /v2/products/stocks       (до 100 товаров за запрос)
    """

    def import_prices(self, items: list[PriceUpdateItem]) -> dict:
        if not items:
            return {"result": []}
        if len(items) > 1000:
            raise ValueError("import_prices: максимум 1000 товаров за запрос")

        payload = {
            "prices": [
                {
                    "offer_id": it.offer_id,
                    "product_id": it.product_id,
                    "price": str(int(it.price)),
                    "old_price": "0",
                    "min_price": "0",
                    "auto_action_enabled": "UNKNOWN",
                    "currency_code": "RUB",
                }
                for it in items
            ]
        }
        return self._post("/v1/product/import/prices", payload)

    def update_stocks(self, items: list[StockUpdateItem]) -> dict:
        if not items:
            return {"result": []}
        if len(items) > 100:
            raise ValueError("update_stocks: максимум 100 товаров за запрос")

        payload = {
            "stocks": [
                {
                    "offer_id": it.offer_id,
                    "product_id": it.product_id,
                    "stock": int(it.stock),
                    "warehouse_id": int(it.warehouse_id),
                }
                for it in items
            ]
        }
        return self._post("/v2/products/stocks", payload)


def collect_price_updates(con: sqlite3.Connection) -> list[PriceUpdateItem]:
    """
    Берём ozon_price_calc и сравниваем с price_current.
    Если одинаково — не отправляем (экономим лимиты).
    """
    cur = con.execute(
        """
        SELECT offer_id, product_id, price_current, ozon_price_calc
        FROM ozon_products
        WHERE
            archived = 0
            AND moderate_status = 'approved'
            AND ozon_price_calc IS NOT NULL
            AND product_id IS NOT NULL
        ORDER BY offer_id;
        """
    )

    out: list[PriceUpdateItem] = []
    for offer_id, product_id, price_current, ozon_price_calc in cur.fetchall():
        try:
            new_price = int(ozon_price_calc)
        except Exception:
            continue

        if price_current is not None:
            try:
                if int(round(float(price_current))) == new_price:
                    continue
            except Exception:
                pass

        out.append(
            PriceUpdateItem(
                offer_id=str(offer_id),
                product_id=int(product_id),
                price=new_price,
            )
        )
    return out


def collect_stock_updates(
    con: sqlite3.Connection,
    *,
    warehouse_id: int,
) -> list[StockUpdateItem]:
    cur = con.execute(
        """
        SELECT offer_id, product_id, supplier_qty
        FROM ozon_products
        WHERE
            archived = 0
            AND moderate_status = 'approved'
            AND supplier_qty IS NOT NULL
            AND product_id IS NOT NULL
        ORDER BY offer_id;
        """
    )

    out: list[StockUpdateItem] = []
    for offer_id, product_id, supplier_qty in cur.fetchall():
        try:
            qty = int(supplier_qty)
        except Exception:
            continue

        qty = max(0, qty)

        out.append(
            StockUpdateItem(
                offer_id=str(offer_id),
                product_id=int(product_id),
                stock=qty,
                warehouse_id=int(warehouse_id),
            )
        )
    return out


def push_prices_to_ozon(
    con: sqlite3.Connection,
    *,
    max_items_per_minute: int = 10_000,
    dry_run: bool = False,
) -> None:
    """Цены: батчи по 1000, троттлинг по items/min."""
    log = setup_logging()
    items = collect_price_updates(con)
    if not items:
        log.info("[OZON][PRICE] nothing to update")
        return

    log.info("[OZON][PRICE] items=%s", len(items))
    batches = _chunked(items, 1000)

    limiter = _RateLimiter(max_items_per_minute=max_items_per_minute)
    oz = OzonUpdater()
    try:
        ok = 0
        bad = 0

        for batch_idx, batch in enumerate(batches, start=1):
            limiter.acquire(len(batch))

            if dry_run:
                log.info("[OZON][PRICE] DRY_RUN batch=%s size=%s", batch_idx, len(batch))
                continue

            try:
                resp = oz.import_prices(batch)
            except Exception as e:
                log.exception("[OZON][PRICE] batch=%s failed: %s", batch_idx, e)
                bad += len(batch)
                continue

            for r in (resp.get("result") or []):
                if r.get("updated") is True and not (r.get("errors") or []):
                    ok += 1
                else:
                    bad += 1
                    log.warning("[OZON][PRICE][ITEM] %s", r)

            log.info("[OZON][PRICE] batch=%s/%s ok=%s bad=%s", batch_idx, len(batches), ok, bad)

        log.info("[OZON][PRICE] done ok=%s bad=%s", ok, bad)
    finally:
        oz.close()


def push_stocks_to_ozon(
    con: sqlite3.Connection,
    *,
    warehouse_id: Optional[int] = None,
    max_items_per_minute: int = 8_000,
    dry_run: bool = False,
) -> None:
    """
    Остатки: батчи по 100.

    В API есть ограничение: максимум 80 запросов в минуту,
    то есть максимум 8000 товаров/мин при batch_size=100.
    """
    log = setup_logging()

    if warehouse_id is None:
        env = (os.getenv("warehouse_id") or "").strip()
        if not env:
            raise RuntimeError(
                "warehouse_id is required. "
                "Pass warehouse_id=... or set warehouse_id env var."
            )
        warehouse_id = int(env)

    items = collect_stock_updates(con, warehouse_id=int(warehouse_id))
    if not items:
        log.info("[OZON][STOCK] nothing to update")
        return

    log.info("[OZON][STOCK] items=%s warehouse_id=%s", len(items), warehouse_id)
    batches = _chunked(items, 100)

    limiter = _RateLimiter(max_items_per_minute=max_items_per_minute)
    oz = OzonUpdater()
    try:
        ok = 0
        bad = 0

        for batch_idx, batch in enumerate(batches, start=1):
            limiter.acquire(len(batch))

            if dry_run:
                log.info("[OZON][STOCK] DRY_RUN batch=%s size=%s", batch_idx, len(batch))
                continue

            try:
                resp = oz.update_stocks(batch)
            except Exception as e:
                log.exception("[OZON][STOCK] batch=%s failed: %s", batch_idx, e)
                bad += len(batch)
                continue

            for r in (resp.get("result") or []):
                if r.get("updated") is True and not (r.get("errors") or []):
                    ok += 1
                else:
                    bad += 1
                    log.warning("[OZON][STOCK][ITEM] %s", r)

            log.info("[OZON][STOCK] batch=%s/%s ok=%s bad=%s", batch_idx, len(batches), ok, bad)

        log.info("[OZON][STOCK] done ok=%s bad=%s", ok, bad)
    finally:
        oz.close()