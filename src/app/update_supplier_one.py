from __future__ import annotations

from .db import connect, init_db
from .autorus_pw_session import AutorusPwSession
from .repositories.supplier_autorus import SupplierAutorusRepo, SupplierAutorusRow


def main() -> None:
    pcode = input("pcode поставщика (пример AT-HDR-08): ").strip()
    if not pcode:
        raise SystemExit("pcode пустой")

    con = connect()
    init_db(con)
    repo = SupplierAutorusRepo(con)

    # headless=True — для linux сервера без GUI
    with AutorusPwSession(profile_dir="data/autorus_profile", headless=True) as s:
        ref = s.search_pcode(pcode)
        offer = s.get_first_offer_from_parts(ref.parts_url)

    if offer is None:
        print("Не найден первый склад на странице parts (offers пусто).")
        con.close()
        return

    row = SupplierAutorusRow(
        pcode=pcode,
        brand=ref.brand,
        number=ref.number,
        parts_url=ref.parts_url,
        price_rub=float(offer.price_rub),
        qty=int(offer.qty),
    )
    repo.upsert(row)

    print("Сохранено в БД:")
    print(row)

    con.close()


if __name__ == "__main__":
    main()
