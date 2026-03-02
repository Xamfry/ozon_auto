from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .logging_setup import setup_logging
from .ozon_client import OzonClient


def _extract_warehouses(resp: Any) -> list[dict]:
    if isinstance(resp, dict):
        result = resp.get("result")
        if isinstance(result, list):
            return [x for x in result if isinstance(x, dict)]
    if isinstance(resp, list):
        return [x for x in resp if isinstance(x, dict)]
    return []


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Сохранить список складов Ozon (/v1/warehouse/list) в JSON."
    )
    parser.add_argument(
        "--out",
        default="data/ozon_warehouses.json",
        help="Куда сохранить JSON (по умолчанию: data/ozon_warehouses.json)",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Сохранять JSON с отступами.",
    )
    args = parser.parse_args()

    log = setup_logging()
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    oz = OzonClient()
    try:
        resp = oz._post("/v1/warehouse/list", {})
    finally:
        oz.close()

    warehouses = _extract_warehouses(resp)

    payload = {
        "raw": resp,
        "warehouses": warehouses,
        "count": len(warehouses),
    }

    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None),
        encoding="utf-8",
    )

    log.info("[OZON][WAREHOUSE] saved=%s count=%s", str(out_path), len(warehouses))


if __name__ == "__main__":
    main()
