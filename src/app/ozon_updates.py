from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

import sqlite3

from .logging_setup import setup_logging
from .ozon_client import OzonClient


@dataclass(frozen=True)
class PriceUpdateItem:
    offer_id: str
    product_id: int
    price_rub: int
    qty: int


@dataclass(frozen=True)
class StockUpdateItem:
    offer_id: str
    product_id: int
    stock: int
    warehouse_id: int
    price_rub: int


def _chunked(seq: Sequence, size: int) -> list[list]:
    return [list(seq[i : i + size]) for i in range(0, len(seq), size)]


class _RateLimiter:
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


def _get_update_logger() -> logging.Logger:
    Path("data").mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("ozon_update")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "[%(asctime)s] [update] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if not logger.handlers:
        fh = logging.FileHandler("data/update.log", encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.propagate = False

    return logger


class OzonUpdater(OzonClient):
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
                    "price": str(int(it.price_rub)),
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


def _load_ignored_offer_ids() -> set[str]:
    raw = (
        os.getenv("IGNORE_OFFER_IDS")
        or os.getenv("ignore_offer_ids")
        or os.getenv("SKIP_OFFER_IDS")
        or os.getenv("skip_offer_ids")
        or ""
    )
    out: set[str] = set()
    for chunk in raw.replace(";", ",").replace("\n", ",").split(","):
        value = chunk.strip()
        if value:
            out.add(value)
    return out


def _env_warehouse_id() -> int:
    raw = (os.getenv("OZON_WAREHOUSE_ID") or os.getenv("warehouse_id") or "").strip()
    if not raw:
        raise RuntimeError("warehouse_id not set. Set OZON_WAREHOUSE_ID in .env")
    return int(raw)


def collect_price_updates(con: sqlite3.Connection) -> list[PriceUpdateItem]:
    cur = con.execute(
        """
        SELECT offer_id, product_id, price_current, ozon_price_calc, COALESCE(supplier_qty, 0) AS supplier_qty
        FROM ozon_products
        WHERE
            archived = 0
            AND moderate_status = 'approved'
            AND ozon_price_calc IS NOT NULL
            AND product_id IS NOT NULL
        ORDER BY offer_id;
        """
    )

    ignored_offer_ids = _load_ignored_offer_ids()
    out: list[PriceUpdateItem] = []
    for offer_id, product_id, price_current, ozon_price_calc, supplier_qty in cur.fetchall():
        if str(offer_id) in ignored_offer_ids:
            continue
        try:
            qty = int(supplier_qty or 0)
        except Exception:
            qty = 0

        # NEW: если остаток 0 — цену не трогаем
        if qty <= 0:
            continue

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
                price_rub=new_price,
                qty=qty,
            )
        )
    return out


def collect_stock_updates(con: sqlite3.Connection, *, warehouse_id: int) -> list[StockUpdateItem]:
    cur = con.execute(
        """
        SELECT offer_id, product_id, COALESCE(supplier_qty, 0) AS supplier_qty, COALESCE(ozon_price_calc, 0) AS ozon_price_calc
        FROM ozon_products
        WHERE
            archived = 0
            AND moderate_status = 'approved'
            AND product_id IS NOT NULL
        ORDER BY offer_id;
        """
    )

    ignored_offer_ids = _load_ignored_offer_ids()
    out: list[StockUpdateItem] = []
    for offer_id, product_id, supplier_qty, ozon_price_calc in cur.fetchall():
        if str(offer_id) in ignored_offer_ids:
            continue
        try:
            qty = int(supplier_qty)
        except Exception:
            qty = 0

        qty = max(0, qty)

        out.append(
            StockUpdateItem(
                offer_id=str(offer_id),
                product_id=int(product_id),
                stock=qty,
                warehouse_id=int(warehouse_id),
                price_rub=int(ozon_price_calc or 0),
            )
        )
    return out


def push_prices_to_ozon(
    con: sqlite3.Connection,
    *,
    max_items_per_minute: int = 10_000,
) -> None:
    log = setup_logging()
    ulog = _get_update_logger()

    items = collect_price_updates(con)
    if not items:
        log.info("[OZON][PRICE] nothing to update")
        return

    log.info("[OZON][PRICE] items=%s", len(items))
    batches = _chunked(items, 1000)
    limiter = _RateLimiter(max_items_per_minute=max_items_per_minute)

    oz = OzonUpdater()
    try:
        for batch_idx, batch in enumerate(batches, start=1):
            limiter.acquire(len(batch))
            resp = oz.import_prices(batch)

            for r in (resp.get("result") or []):
                if r.get("updated") is True and not (r.get("errors") or []):
                    # пишем в update.log только то, что реально обновили
                    offer_id = str(r.get("offer_id") or "")
                    it = next((x for x in batch if x.offer_id == offer_id), None)
                    if it:
                        ulog.info(f"[{it.offer_id}] [{it.price_rub}] [{it.qty}]")
                else:
                    log.warning("[OZON][PRICE][ITEM] %s", r)

            log.info("[OZON][PRICE] batch=%s/%s", batch_idx, len(batches))
    finally:
        oz.close()


def push_stocks_to_ozon(
    con: sqlite3.Connection,
    *,
    warehouse_id: Optional[int] = None,
    max_items_per_minute: int = 8_000,
) -> None:
    log = setup_logging()
    ulog = _get_update_logger()

    if warehouse_id is None:
        warehouse_id = _env_warehouse_id()

    items = collect_stock_updates(con, warehouse_id=int(warehouse_id))
    if not items:
        log.info("[OZON][STOCK] nothing to update")
        return

    log.info("[OZON][STOCK] items=%s warehouse_id=%s", len(items), warehouse_id)
    batches = _chunked(items, 100)
    limiter = _RateLimiter(max_items_per_minute=max_items_per_minute)

    oz = OzonUpdater()
    try:
        for batch_idx, batch in enumerate(batches, start=1):
            limiter.acquire(len(batch))
            resp = oz.update_stocks(batch)

            for r in (resp.get("result") or []):
                if r.get("updated") is True and not (r.get("errors") or []):
                    offer_id = str(r.get("offer_id") or "")
                    it = next((x for x in batch if x.offer_id == offer_id), None)
                    if it:
                        ulog.info(f"[{it.offer_id}] [{it.price_rub}] [{it.stock}]")
                else:
                    log.warning("[OZON][STOCK][ITEM] %s", r)

            log.info("[OZON][STOCK] batch=%s/%s", batch_idx, len(batches))
    finally:
        oz.close()
