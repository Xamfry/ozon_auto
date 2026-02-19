from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


@dataclass(frozen=True)
class AutorusPartRef:
    brand: str
    number: str
    parts_url: str


@dataclass(frozen=True)
class AutorusOffer:
    warehouse: str
    qty: int
    price_rub: float
    deadline: str


@dataclass(frozen=True)
class AutorusPartPage:
    brand: str
    number: str
    title: str
    offers: list[AutorusOffer]
    url: str


class AutorusPwClient:
    """
    Autorus client через Playwright.

    Важно:
    - НЕ логинится сам
    - использует готовый storage_state (state_autorus.json)
    """

    BASE = "https://b2b.autorus.ru"

    def __init__(self, state_path: str = "state_autorus.json", headless: bool = True) -> None:
        self.state_path = state_path
        self.headless = headless

    # --------------------- utils ---------------------
    @staticmethod
    def _text(el) -> str:
        return " ".join(el.get_text(" ", strip=True).split()) if el else ""

    @staticmethod
    def _parse_int(s: str) -> int:
        digits = "".join(ch for ch in (s or "") if ch.isdigit())
        return int(digits) if digits else 0

    @staticmethod
    def _parse_price(s: str) -> float:
        cleaned = "".join(ch for ch in (s or "") if ch.isdigit() or ch in ",.")
        cleaned = cleaned.replace(",", ".")
        try:
            return float(cleaned) if cleaned else 0.0
        except Exception:
            return 0.0

    # --------------------- public ---------------------
    def search_pcode(self, pcode: str, wh_code: str = "") -> AutorusPartRef:
        """
        1) По pcode ищем бренд + номер на странице /search.

        Самый устойчивый способ (по твоему HTML):
        - ищем строку результата с нужным pcode (td.resultPartCode a)
        - берём data-brand/data-number из img.searchResultImg
        """
        url = f"{self.BASE}/search?pcode={pcode}&whCode={wh_code}"

        def norm(s: str) -> str:
            return "".join(ch for ch in (s or "").upper() if ch.isalnum())

        wanted = norm(pcode)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(storage_state=self.state_path, locale="ru-RU")
            page = context.new_page()
            page.set_default_timeout(120_000)

            try:
                page.goto(url, wait_until="domcontentloaded")
                page.wait_for_selector("tr[class*='resultTr'], .article-brand, input[name='brand']", timeout=120_000)
            except PWTimeoutError:
                Path("data").mkdir(exist_ok=True)
                page.screenshot(path="data/search_timeout.png", full_page=True)
                with open("data/search_timeout.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                context.close()
                browser.close()
                raise RuntimeError("Timeout на странице поиска. Сохранено: data/search_timeout.*")

            html = page.content()
            soup = BeautifulSoup(html, "lxml")

            brand = ""
            number = ""

            # Вариант 1: ровно тот результат, где pcode совпал
            for tr in soup.select("tr[class*='resultTr']"):
                pcode_el = tr.select_one(".resultPartCode a")
                if not pcode_el:
                    continue
                if norm(self._text(pcode_el)) != wanted:
                    continue

                img = tr.select_one("img.searchResultImg")
                if img and img.get("data-brand") and img.get("data-number"):
                    brand = str(img.get("data-brand") or "").strip()
                    number = str(img.get("data-number") or "").strip()
                    break

            # Вариант 2: скрытые input (иногда встречается)
            if not brand or not number:
                brand_el = soup.select_one("input[name='brand']")
                number_el = soup.select_one("input[name='number']")
                if brand_el and number_el:
                    brand = (brand_el.get("value") or "").strip()
                    number = (number_el.get("value") or "").strip()

            # Вариант 3: span.article-brand / span.article-number
            if not brand or not number:
                brand = self._text(soup.select_one(".article-brand"))
                number = self._text(soup.select_one(".article-number"))

            context.close()
            browser.close()

            if not brand or not number:
                Path("data").mkdir(exist_ok=True)
                with open("data/search_unexpected.html", "w", encoding="utf-8") as f:
                    f.write(html)
                raise RuntimeError("Не удалось извлечь brand/number из search. Сохранено: data/search_unexpected.html")

            parts_url = f"{self.BASE}/parts/{quote(brand)}/{quote(number)}"
            return AutorusPartRef(brand=brand, number=number, parts_url=parts_url)

    def get_parts_page(self, parts_url: str) -> AutorusPartPage:
        """
        2) Открываем /parts/{brand}/{number} и парсим блоки складов:
        .distrInfoBlockWrapper -> срок/наличие/склад/цена
        """
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                storage_state=self.state_path,
                locale="ru-RU",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"
                ),
            )
            page = context.new_page()
            page.set_default_timeout(120_000)

            try:
                # commit — чтобы не зависать на ресурсах
                page.goto(parts_url, wait_until="commit", timeout=120_000)
                page.wait_for_selector(".distrInfoBlockWrapper, .article-brand, .article-number", timeout=120_000)
            except PWTimeoutError:
                Path("data").mkdir(exist_ok=True)
                page.screenshot(path="data/parts_timeout.png", full_page=True)
                with open("data/parts_timeout.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                context.close()
                browser.close()
                raise RuntimeError("Timeout на странице parts. Сохранено: data/parts_timeout.*")

            html = page.content()
            current_url = page.url
            soup = BeautifulSoup(html, "lxml")

            brand = self._text(soup.select_one(".article-brand"))
            number = self._text(soup.select_one(".article-number"))
            title_el = soup.select_one(".goodsInfoTitle") or soup.select_one("h1")
            title = self._text(title_el) or f"{brand} {number}".strip()

            offers: list[AutorusOffer] = []
            for w in soup.select(".distrInfoBlockWrapper"):
                deadline = self._text(w.select_one(".distrInfoDeadline div:nth-of-type(2)"))
                qty_txt = self._text(w.select_one(".distrInfoAvailability .fr-text-nowrap"))
                qty = self._parse_int(qty_txt)
                warehouse = self._text(w.select_one(".distrInfoRoute .fr-text-nowrap"))
                price_txt = self._text(w.select_one(".distrInfoPrice"))
                price = self._parse_price(price_txt)
                offers.append(AutorusOffer(warehouse=warehouse, qty=qty, price_rub=price, deadline=deadline))

            context.close()
            browser.close()

            if not brand or not number:
                Path("data").mkdir(exist_ok=True)
                with open("data/parts_unexpected.html", "w", encoding="utf-8") as f:
                    f.write(html)
                raise RuntimeError(
                    "Открылась не та страница (brand/number не найдены). "
                    f"Текущий URL: {current_url}. "
                    "Сохранено: data/parts_unexpected.html"
                )

            return AutorusPartPage(brand=brand, number=number, title=title, offers=offers, url=current_url)

    def get_first_warehouse_offer(self, parts_url: str) -> AutorusOffer | None:
        """Берём только 1-й склад (первый .distrInfoBlockWrapper)."""
        page = self.get_parts_page(parts_url)
        return page.offers[0] if page.offers else None
