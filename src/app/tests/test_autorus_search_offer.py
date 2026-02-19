from __future__ import annotations
from ....tmp.autorus_pw_client import AutorusPwClient

def main() -> None:
    pcode = input("pcode: ").strip()
    c = AutorusPwClient(state_path="state_autorus.json", headless=False)

    offer = c.get_first_instock_offer_from_search(pcode)
    print("OFFER:", offer)

if __name__ == "__main__":
    main()
