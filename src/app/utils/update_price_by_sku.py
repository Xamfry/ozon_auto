from __future__ import annotations

import argparse
import sqlite3

from ..logging_setup import setup_logging
from ..ozon_client import OzonClient


def update_price_by_sku(article: str, new_price: int) -> None:
    log = setup_logging()

    con = sqlite3.connect("data/app.db")
    cur = con.execute(
        """
        SELECT product_id, price_current
        FROM ozon_products
        WHERE offer_id = ?
        """,
        (article,),
    )
    row = cur.fetchone()

    if not row:
        raise RuntimeError(f"Товар с артикулом {article} не найден в БД")

    product_id, price_current = row

    if not product_id:
        raise RuntimeError(f"У товара {article} отсутствует product_id")

    if price_current is not None:
        try:
            if int(round(float(price_current))) == int(new_price):
                log.info(
                    "[UTIL][PRICE] Цена уже установлена: article=%s price=%s",
                    article,
                    new_price,
                )
                return
        except Exception:
            pass

    payload = {
        "prices": [
            {
                "offer_id": article,
                "product_id": int(product_id),
                "price": str(int(new_price)),
                "old_price": "0",
                "min_price": "0",
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
            }
        ]
    }

    oz = OzonClient()
    try:
        resp = oz._post("/v1/product/import/prices", payload)
    finally:
        oz.close()

    result = (resp.get("result") or [{}])[0]

    if result.get("updated") and not result.get("errors"):
        con.execute(
            """
            UPDATE ozon_products
            SET price_current = ?
            WHERE offer_id = ?
            """,
            (int(new_price), article),
        )
        con.commit()

        log.info(
            "[UTIL][PRICE] Успешно обновлено: article=%s price=%s",
            article,
            new_price,
        )
    else:
        raise RuntimeError(
            f"Ozon вернул ошибку: {result}"
        )

    con.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Обновление цены товара в Ozon по артикулу (offer_id)"
    )
    parser.add_argument(
        "--article",
        required=True,
        help="Артикул товара (offer_id в Ozon)",
    )
    parser.add_argument(
        "--price",
        required=True,
        type=int,
        help="Новая цена товара",
    )

    args = parser.parse_args()

    update_price_by_sku(
        article=args.article.strip(),
        new_price=int(args.price),
    )


if __name__ == "__main__":
    main()