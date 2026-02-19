from __future__ import annotations

from ..autorus_pw_client import AutorusPwClient


def main() -> None:
    pcode = input("pcode (пример AT-HDR-08): ").strip()
    if not pcode:
        raise SystemExit("pcode пустой")

    c = AutorusPwClient(state_path="state_autorus.json", headless=False)

    ref = c.search_pcode(pcode)
    print("REF:", ref)

    offer = c.get_first_warehouse_offer(ref.parts_url)
    if offer is None:
        print("FIRST_WAREHOUSE: None")
        return

    print(
        "FIRST_WAREHOUSE:",
        f"warehouse='{offer.warehouse}' qty={offer.qty} price={offer.price_rub} deadline='{offer.deadline}'",
    )


if __name__ == "__main__":
    main()
