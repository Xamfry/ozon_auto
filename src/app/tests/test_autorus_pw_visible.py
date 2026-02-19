from __future__ import annotations

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup


STATE_PATH = "state_autorus.json"
BASE = "https://b2b.autorus.ru"


def text(el) -> str:
    return " ".join(el.get_text(" ", strip=True).split()) if el else ""


def main() -> None:
    pcode = input("pcode: ").strip()
    url_search = f"{BASE}/search?pcode={pcode}&whCode="

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=200)
        context = browser.new_context(storage_state=STATE_PATH, locale="ru-RU")
        page = context.new_page()
        page.set_default_timeout(120_000)

        print("Открываю SEARCH:", url_search)
        page.goto(url_search, wait_until="domcontentloaded")
        page.wait_for_timeout(1500)

        html = page.content()
        soup = BeautifulSoup(html, "lxml")

        brand_el = soup.select_one('input[name="brand"]')
        number_el = soup.select_one('input[name="number"]')

        brand = (brand_el.get("value") or "").strip() if brand_el else text(soup.select_one(".article-brand"))
        number = (number_el.get("value") or "").strip() if number_el else text(soup.select_one(".article-number"))

        if not brand or not number:
            print("Не нашёл brand/number на странице поиска.")
            input("Нажми Enter для выхода...")
            context.close()
            browser.close()
            return

        parts_url = f"{BASE}/parts/{brand}/{number}"
        print("ПЕРЕХОЖУ НА PARTS:", parts_url)

        page.goto(parts_url, wait_until="commit")
        page.wait_for_timeout(3000)

        print("Текущий URL:", page.url)
        input("Окно оставлено открытым. Нажми Enter для выхода...")

        context.close()
        browser.close()


if __name__ == "__main__":
    main()
