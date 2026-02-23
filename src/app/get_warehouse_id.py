from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .logging_setup import setup_logging
from .ozon_client import OzonClient


def _extract_warehouses(resp: Any) -> list[dict]:
    """Ozon обычно возвращает {"result": [...]} для /v1/warehouse/list."""
    if isinstance(resp, dict):
        result = resp.get("result")
        if isinstance(result, list):
            return [x for x in result if isinstance(x, dict)]
    if isinstance(resp, list):
        return [x for x in resp if isinstance(x, dict)]
    return []


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Получить список складов из Ozon Seller API (/v1/warehouse/list) "
            "и сохранить его в JSON."
        )
    )
    parser.add_argument(
        "--out",
        default="data/ozon_warehouses.json",
        help="Куда сохранить JSON (по умолчанию: data/ozon_warehouses.json)",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Сохранять JSON с отступами (читаемо).",
    )
    args = parser.parse_args()

    log = setup_logging()
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    oz = OzonClient()
    try:
        # В запросе параметры не обязательны.
        resp = oz._post("/v1/warehouse/list", {})
    finally:
        oz.close()

    warehouses = _extract_warehouses(resp)

    payload = {
        "raw": resp,  # сохраняем полный ответ, чтобы ничего не потерять
        "warehouses": warehouses,
        "count": len(warehouses),
    }

    if args.pretty:
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        out_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    log.info("[OZON][WAREHOUSE] saved=%s count=%s", str(out_path), len(warehouses))

    # Короткий вывод в консоль: id + название + адрес (если есть)
    for w in warehouses:
        wid = w.get("warehouse_id") or w.get("id")
        name = w.get("name") or w.get("title") or ""
        addr = (
            w.get("address")
            or w.get("address_full")
            or w.get("full_address")
            or w.get("place")
            or ""
        )
        log.info("[OZON][WAREHOUSE] id=%s name=%s addr=%s", wid, name, addr)


if __name__ == "__main__":
    main()