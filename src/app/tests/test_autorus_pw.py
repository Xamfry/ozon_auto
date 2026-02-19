from __future__ import annotations

from ....tmp.autorus_pw_client import AutorusPwClient


def main() -> None:
    pcode = input("pcode (пример AT-HDR-08): ").strip()
    if not pcode:
        raise SystemExit("pcode пустой")

    c = AutorusPwClient(state_path="state_autorus.json", headless=False)

    ref = c.search_pcode(pcode)
    print("REF:", ref)

    page = c.get_parts_page(ref.parts_url)
    print("TITLE:", page.title)
    print("URL:", page.url)

    for o in page.offers:
        print(f"- wh='{o.warehouse}' qty={o.qty} price={o.price_rub} deadline='{o.deadline}'")


if __name__ == "__main__":
    main()
