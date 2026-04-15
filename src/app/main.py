from __future__ import annotations

from pathlib import Path
import os
import tempfile
from datetime import datetime, timezone, timedelta
import time
from urllib.parse import quote

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


def _fetch_offer_ids_by_visibility(oz: OzonClient, visibility: str) -> set[str]:
    offer_ids: set[str] = set()
    last_id = ""

    while True:
        resp = oz._post(
            "/v3/product/list",
            {
                "filter": {"visibility": visibility},
                "limit": 1000,
                "last_id": last_id,
            },
        )
        result = resp.get("result") or {}
        items = result.get("items") or []
        last_id = result.get("last_id") or ""

        for item in items:
            offer_id = str(item.get("offer_id") or "").strip()
            if offer_id:
                offer_ids.add(offer_id)

        if not last_id:
            break

    return offer_ids


def _norm_brand(value: str | None) -> str:
    return "".join(ch for ch in (value or "").upper() if ch.isalnum())


def _build_autorus_parts_url(brand: str | None, article: str | None) -> str | None:
    brand = (brand or "").strip()
    article = (article or "").strip()
    if not brand or not article:
        return None
    return f"https://b2b.autorus.ru/parts/{quote(brand, safe='')}/{quote(article, safe='')}"


def _load_ignored_offer_ids() -> set[str]:
    raw = (
        os.getenv("IGNORE_OFFER_IDS")
        or os.getenv("ignore_offer_ids")
        or os.getenv("SKIP_OFFER_IDS")
        or os.getenv("skip_offer_ids")
        or ""
    )
    parts: list[str] = []
    for chunk in raw.replace(";", ",").replace("\n", ",").split(","):
        value = chunk.strip()
        if value:
            parts.append(value)
    return set(parts)


def get_sale_stats_after_push() -> dict[str, int]:
    oz = OzonClient()
    try:
        all_ids = _fetch_offer_ids_by_visibility(oz, "ALL")
        selling_ids = _fetch_offer_ids_by_visibility(oz, "IN_SALE")
        ready_ids = _fetch_offer_ids_by_visibility(oz, "EMPTY_STOCK")

        failed_ids = _fetch_offer_ids_by_visibility(oz, "STATE_FAILED")
        validation_failed_ids = _fetch_offer_ids_by_visibility(oz, "VALIDATION_STATE_FAIL")
        
        revision_ids = _fetch_offer_ids_by_visibility(oz, "PARTIAL_APPROVED")
        removed_ids = _fetch_offer_ids_by_visibility(oz, "REMOVED_FROM_SALE")
        
        archived_ids = _fetch_offer_ids_by_visibility(oz, "ARCHIVED")

        stats_cache = {
            "all": len(all_ids | archived_ids),
            "selling": len(selling_ids),
            "ready": len(ready_ids),
            "errors": len(failed_ids | validation_failed_ids),
            "revision": len(revision_ids),
            "removed": len(removed_ids),
            "archived": len(archived_ids),
        }
        return stats_cache
    finally:
        oz.close()


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
                ozon_brand=a.brand,
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
    ignored_offer_ids = _load_ignored_offer_ids()
    rows_all = products_repo.list_for_supplier_sync()
    rows = [r for r in rows_all if r.offer_id not in ignored_offer_ids]
    print(f"Supplier sync candidates: {len(rows)}")
    if ignored_offer_ids:
        print(f"Ignored offer_ids: {len(ignored_offer_ids)}")
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
            "Run: python -m src.app.utils.bootstrap_autorus_profile"
        )

    with AutorusPwSession(profile_dir=profile_dir, headless=False) as supplier:
        supplier.page.goto(
            "https://b2b.autorus.ru/search?pcode=AT-HDR-08&whCode=",
            wait_until="domcontentloaded",
            timeout=60_000,
        )


        for row in rows:
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

            parts_url = _build_autorus_parts_url(row.ozon_brand, row.offer_id) or (row.supplier_parts_url or "").strip() or None

            supplier.log.info(
                "[ITEM] offer_id=%s brand=%s pcode=%s parts_url=%s",
                row.offer_id,
                row.ozon_brand,
                pcode,
                parts_url or "-",
            )

            try:
                snapshot = supplier.fetch_product_snapshot(pcode=pcode, parts_url=parts_url)
                if _norm_brand(snapshot.brand) and _norm_brand(row.ozon_brand) and _norm_brand(snapshot.brand) != _norm_brand(row.ozon_brand):
                    raise RuntimeError(
                        f"Brand mismatch: ozon={row.ozon_brand!r}, autorus={snapshot.brand!r}"
                    )
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

    tmp_path = None
    try:
        wh = warehouse_id if warehouse_id is not None else 0
        MSK = timezone(timedelta(hours=3))
        ts = datetime.now(MSK).strftime("%d-%m-%Y_%H-%M-%S")
        fname = f"autorus_bot_{ts}_{wh}.txt"

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
        tmp_file = tmp_dir / fname  # fname "ДД-ММ-ГГГГ_ЧЧ-ММ-СС_{wh}.txt"
        tmp_path = str(tmp_file)

        tmp_file.write_text("\n".join(lines), encoding="utf-8")

        tg.send_document(tmp_path, caption=msg3)
        print(f"Sent Telegram stage 3 with document: {tmp_path}")
    except Exception as e:
        print(f"Stage 3: send_document failed: {e!r}")
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

    # 4) Пауза и статистика
    print(f"Pause 20 seconds")
    for _ in range(1, 21):
        print(f"sleep {_} ...")
        time.sleep(1)
    
    try:
        stats = get_sale_stats_after_push()
        msg4 = (
            f"Ozon stats:\n"
            f"1) Все товары={stats['all']}\n"
            f"2) В продаже={stats['selling']}\n"
            f"3) Готовы к продаже={stats['ready']}\n"
            f"4) Ошибки={stats['errors']}\n"
            f"5) На доработку={stats['revision']}\n"
            f"6) Сняты с продажи={stats['removed']}\n"
            f"7) Архив={stats['archived']}"
        )
        print(msg4)
        tg.send_message(msg4)
    except Exception as e:
        print(f"Stage 4 failed: {e!r}")

    con.close()


if __name__ == "__main__":
    main()