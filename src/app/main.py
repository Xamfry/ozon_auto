from __future__ import annotations

from pathlib import Path
import os
import tempfile
from datetime import datetime, timezone, timedelta

from .autorus_pw_session import AutorusPwSession
from .db import connect, init_db
from .ozon_client import OzonClient
from .ozon_updates import push_prices_to_ozon, push_stocks_to_ozon
from .pricing import DimensionsMM, PriceInput, calculate_ozon_price
from .repositories.ozon_details import OzonDetailsRepo, OzonProductDetails
from .repositories.ozon_products import OzonProductsRepo
from .utils.telegram import TelegramNotifier


def chunked(seq: list[str], size: int) -> list[list[str]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def _has_dimensions(row) -> bool:
    return bool(row.length_mm and row.width_mm and row.height_mm and row.weight_g)


def main() -> None:
    con = connect()
    init_db(con)

    products_repo = OzonProductsRepo(con)
    details_repo = OzonDetailsRepo(con)

    tg = TelegramNotifier()
    try:
        tg.send_message(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Start tg stage 1: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception:
        # телеграм не должен ломать основной процесс
        print(f"Failed stage 1 to Telegram")

    warehouse_id = None
    raw_wh = (os.getenv("OZON_WAREHOUSE_ID") or os.getenv("warehouse_id") or "").strip()
    if raw_wh:
        try:
            warehouse_id = int(raw_wh)
        except Exception:
            warehouse_id = None

    # 1) Ozon sync
    oz = OzonClient()
    try:
        base_list = oz.list_products_all(include_archived=False, visibility="ALL")
        offer_ids = [p.offer_id for p in base_list]

        info_rows = []
        for batch in chunked(offer_ids, 1000):
            info_rows.extend(oz.get_product_info_list_by_offer_ids(batch))

        approved = [
            x for x in info_rows
            if (not x.archived) and (x.moderate_status == "approved")
        ]

        products_repo.upsert_many(approved)

        approved_offer_ids = [x.offer_id for x in approved]
        attrs_rows = []
        for batch in chunked(approved_offer_ids, 1000):
            attrs_rows.extend(oz.get_attributes_by_offer_ids(batch))

        details = [
            OzonProductDetails(
                offer_id=a.offer_id,
                product_id=a.product_id,
                name=a.name,
                weight_g=a.weight_g,
                length_mm=a.length_mm,
                width_mm=a.width_mm,
                height_mm=a.height_mm,
            )
            for a in attrs_rows
        ]
        details_repo.upsert_many(details)

        print(f"Ozon: non-archived={len(base_list)}")
        print(f"Ozon: approved in DB={len(approved)}")
        print(f"Ozon: details saved={len(details)}")
    finally:
        oz.close()

    # 2) Supplier + pricing
    rows = products_repo.list_for_supplier_sync()
    print(f"Supplier sync candidates: {len(rows)}")
    try:
        tg.send_message(
            "Ozon: non-archived={0}\nOzon: approved in DB={1}\nOzon: details saved={2}\nSupplier sync candidates: {3}".format(
                len(base_list), len(approved), len(details), len(rows)
            )
        )
        print(f"Start tg stage 2: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception:
        print(f"Failed stage 2 to Telegram")


    done = 0
    skipped = 0
    failed = 0
    done_offer_ids: list[str] = []

    profile_dir = "data/autorus_profile"
    if not Path(profile_dir).exists():
        raise RuntimeError(
            "Autorus profile not found: data/autorus_profile. "
            "Run: python -m src.app.bootstrap_autorus_profile"
        )

    with AutorusPwSession(profile_dir=profile_dir, headless=False) as supplier:
        # health-check: must not be guest mode
        supplier.page.goto(
            "https://b2b.autorus.ru/search?pcode=AT-HDR-08&whCode=",
            wait_until="domcontentloaded",
            timeout=60_000,
        )
        # if supplier.is_guest_mode():
        #     raise RuntimeError(
        #         "Autorus: profile is not authorized (guest mode). "
        #         "Run bootstrap_autorus_profile again and login manually."
        #     )

        for row in rows:
            # базовые фильтры
            if not _has_dimensions(row):
                skipped += 1
                continue
            if row.commission_fbs_percent is None:
                skipped += 1
                continue

            pcode = (row.offer_id or "").strip()
            if not pcode:
                skipped += 1
                continue

            parts_url = (row.supplier_parts_url or "").strip() or None

            supplier.log.info(
                "[ITEM] offer_id=%s pcode=%s parts_url=%s",
                row.offer_id,
                pcode,
                "yes" if parts_url else "no",
            )

            try:
                snapshot = supplier.fetch_product_snapshot(pcode=pcode, parts_url=parts_url)
                offer = snapshot.offer
                if offer is None:
                    skipped += 1
                    continue

                products_repo.update_supplier_fields(
                    offer_id=row.offer_id,
                    supplier_brand=snapshot.brand,
                    supplier_number=snapshot.number,
                    supplier_parts_url=snapshot.parts_url,
                    supplier_price_rub=float(offer.price_rub),
                    supplier_qty=int(offer.qty),
                )

                inp = PriceInput(
                    закуп=float(offer.price_rub),
                    markup_percent=float(row.markup_percent or 0.0),
                )
                dims = DimensionsMM(
                    length_mm=int(row.length_mm),
                    width_mm=int(row.width_mm),
                    height_mm=int(row.height_mm),
                    weight_g=int(row.weight_g),
                )

                res = calculate_ozon_price(
                    inp=inp,
                    dims=dims,
                    commission_percent=float(row.commission_fbs_percent),
                )
                products_repo.update_ozon_price_calc(row.offer_id, res.final_price)

                done += 1
                done_offer_ids.append(row.offer_id)

            except Exception as e:
                failed += 1
                supplier.log.exception(
                    "[FAIL] offer_id=%s pcode=%s: %s",
                    row.offer_id,
                    pcode,
                    e,
                )

    # 3) Push updates to Ozon (сначала цены, потом остатки)
    push_prices_to_ozon(con)
    push_stocks_to_ozon(con)

    msg3 = f"Supplier sync done={done}, skipped={skipped}, failed={failed}"
    print(msg3)

    # Дополнение к п3: дамп обновлённых товаров в .txt и отправка в Telegram
    tmp_path = None
    try:
        wh = warehouse_id if warehouse_id is not None else 0
        MSK = timezone(timedelta(hours=3))
        ts = datetime.now(MSK).strftime("%d-%m-%Y_%H-%M-%S")
        fname = f"autorus_bot_{ts}_{wh}.txt"

        # ВАЖНО: в сам файл НЕ пишем строку Supplier sync done=...
        # Она уходит как текст (caption) вместе с вложенным .txt
        lines: list[str] = []
        if done_offer_ids:
            placeholders = ",".join(["?"] * len(done_offer_ids))
            cur = con.execute(
                f"""
                SELECT offer_id, COALESCE(supplier_price_rub, 0), COALESCE(supplier_qty, 0), COALESCE(ozon_price_calc, 0)
                FROM ozon_products
                WHERE offer_id IN ({placeholders})
                ORDER BY offer_id
                """,
                done_offer_ids,
            )
            for offer_id, sup_price, sup_qty, oz_price in cur.fetchall():
                lines.append(f"[{offer_id}] supplier_price={sup_price} qty={sup_qty} ozon_price={oz_price}")
        else:
            lines.append("No updated items.")

        tmp_dir = Path(tempfile.gettempdir())
        tmp_file = tmp_dir / fname  # fname уже "ДД-ММ-ГГГГ_ЧЧ-ММ-СС_{wh}.txt"
        tmp_path = str(tmp_file)

        tmp_file.write_text("\n".join(lines), encoding="utf-8")

        # Отправляем файл + текст одним сообщением
        tg.send_document(tmp_path, caption=msg3)
        print(f"Sent Telegram stage 3 with document: {tmp_path}")
    except Exception as e:
        print(f"Stage 3: send_document failed: {e!r}")
        # если файл не ушёл — хотя бы текст пункта 3
        try:
            tg.send_message(msg3)
            print(f"Sent Telegram stage 3 without document")
        except Exception as e2:
            print(f"Stage 3: send_message failed: {e2!r}")

    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                pass

    # отдельное сообщение не шлём, чтобы не дублировать п3

    con.close()


if __name__ == "__main__":
    main()