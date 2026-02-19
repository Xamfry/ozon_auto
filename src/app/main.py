from __future__ import annotations

from .ozon_client import OzonClient
from .db import connect, init_db
from .repositories.ozon_products import OzonProductsRepo
from .repositories.ozon_details import OzonDetailsRepo, OzonProductDetails
from pathlib import Path
from .autorus_pw_session import AutorusPwSession
from .pricing import PriceInput, DimensionsMM, calculate_ozon_price


def chunked(seq: list[str], size: int) -> list[list[str]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def main() -> None:
    con = connect()
    init_db(con)

    products_repo = OzonProductsRepo(con)
    details_repo = OzonDetailsRepo(con)

    # --------- 1) Ozon: обновить список товаров + статусы/комиссии/цены + габариты ---------
    oz = OzonClient()
    try:
        base_list = oz.list_products_all(include_archived=False, visibility="ALL")
        offer_ids = [p.offer_id for p in base_list]

        info_rows = []
        for batch in chunked(offer_ids, 1000):
            info_rows.extend(oz.get_product_info_list_by_offer_ids(batch))

        approved = [x for x in info_rows if (not x.archived) and (x.moderate_status == "approved")]
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

        print(f"Ozon: неархивных: {len(base_list)}")
        print(f"Ozon: approved в БД: {len(approved)}")
        print(f"Ozon: деталей записано: {len(details)}")
    finally:
        oz.close()

    # --------- 2) Supplier + pricing: пройти по товарам из БД ---------
    rows = products_repo.list_for_supplier_sync()
    print(f"Supplier sync кандидатов: {len(rows)}")

    # headless=True для Linux сервера
    state_path = "data/state_autorus.json"
    if not Path(state_path).exists() and Path("state_autorus.json").exists():
        state_path = "state_autorus.json"

    with AutorusPwSession(state_path=state_path, headless=True) as s:
        s.ensure_logged_in(allow_autologin=False)
        for r in rows:
            
            # Проверка габаритов
            if not (r.length_mm and r.width_mm and r.height_mm and r.weight_g):
                # нет габаритов -> не считаем цену
                continue

            # Проверка комиссии
            if r.commission_fbs_percent is None:
                continue

            pcode = r.offer_id.strip()

            # 2.1 parts_url: если есть -> сразу parts; если нет -> search -> parts
            supplier_brand = None
            supplier_number = None
            parts_url = (r.supplier_parts_url or "").strip() or None
            s.log.info(f"[ITEM] offer_id={r.offer_id} pcode={pcode} parts_url={'yes' if parts_url else 'no'}")
            s._sleep()
            try:
                if not parts_url:
                    result = s.search_pcode(pcode)
                    if "parts_url" in result:
                        supplier_brand = result["brand"]
                        supplier_number = result["number"]
                        parts_url = result["parts_url"]
                    else:
                        resolved = s.resolve_from_search_detail(result["search_detail_url"])
                        supplier_brand = resolved["brand"]
                        supplier_number = resolved["number"]
                        parts_url = resolved["parts_url"]

                offer = s.get_first_offer_from_parts(parts_url)
                if offer is None:
                    # не нашли склад/оффер
                    continue

                # сохраним цену/остатки поставщика
                products_repo.update_supplier_fields(
                    offer_id=r.offer_id,
                    supplier_brand=supplier_brand,
                    supplier_number=supplier_number,
                    supplier_parts_url=parts_url,
                    supplier_price_rub=float(offer.price_rub),
                    supplier_qty=int(offer.qty),
                )

                # 2.2 рассчитать цену Ozon
                inp = PriceInput(
                    закуп=float(offer.price_rub),
                    markup_percent=float(r.markup_percent or 0.0),
                )
                dims = DimensionsMM(
                    length_mm=int(r.length_mm),
                    width_mm=int(r.width_mm),
                    height_mm=int(r.height_mm),
                    weight_g=int(r.weight_g),
                )

                res = calculate_ozon_price(
                    inp=inp,
                    dims=dims,
                    commission_percent=float(r.commission_fbs_percent),
                )

                products_repo.update_ozon_price_calc(r.offer_id, res.final_price)
            except Exception as e:
                s.log.exception(f"[FAIL] offer_id={r.offer_id} pcode={pcode}: {e}")
                continue

    con.close()


if __name__ == "__main__":
    main()
