from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote
import random
import time
import os
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from .logging_setup import setup_logging


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


class AutorusPwSession:
    BASE = "https://b2b.autorus.ru"

    def __init__(self, state_path: str = "state_autorus.json", headless: bool = True) -> None:
        self.state_path = state_path
        self.headless = headless
        self._p = None
        self._browser = None
        self._context = None
        self._page = None
        self.log = setup_logging()
        self.delay_min = 2
        self.delay_max = 3

    def __enter__(self) -> "AutorusPwSession":
        state_file = Path(self.state_path)
        if not state_file.exists():
            raise RuntimeError(f"Autorus: state-файл не найден: {state_file.resolve()}")

        state_file = Path(self.state_path)
        if not state_file.exists():
            raise RuntimeError(f"Autorus: state-файл не найден: {state_file.resolve()}")

        self._p = sync_playwright().start()
        self._browser = self._p.chromium.launch(headless=self.headless)
        self._context = self._browser.new_context(
            storage_state=self.state_path,
            locale="ru-RU",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"
            ),
        )
        self._page = self._context.new_page()
        self._page.set_default_timeout(120_000)
        st = state_file.stat()
        self.log.info(
            "[STATE] path=%s size=%s mtime=%s",
            state_file.resolve(),
            st.st_size,
            int(st.st_mtime),
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._p:
            self._p.stop()
    
    def _sleep(self) -> None:
        time.sleep(random.uniform(self.delay_min, self.delay_max))

    def _is_guest_mode(self, page) -> bool:
        # на сохранённой странице есть блок "Вы работаете в гостевом режиме"
        txt = page.locator("body").inner_text(timeout=2000)
        return "гостевом режиме" in txt.lower()

    def _save_debug(self, name: str, html: str) -> None:
        import os
        os.makedirs("data", exist_ok=True)
        with open(f"data/{name}", "w", encoding="utf-8") as f:
            f.write(html)

    def _build_parts_url(self, brand: str, number: str) -> str:
        # бренд может содержать пробел/&, поэтому кодируем сегменты
        return f"https://b2b.autorus.ru/parts/{quote(brand, safe='')}/{quote(number, safe='')}"
    
    @property
    def page(self):
        if self._page is None:
            raise RuntimeError("Playwright page not initialized. Use 'with AutorusPwSession(...) as s:'")
        return self._page


    # ---------- utils ----------
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

    # ---------- steps ----------
    def search_pcode(self, pcode: str):
        """
        1) /search?pcode=...
        2) если есть goodsInfoTitle -> сразу brand/number
        3) если таблица результатов -> берём первую строку и идём на /search/BRAND/PCODE
        4) retry: сначала pcode как есть, потом без дефисов
        """

        def _try(one: str):
            self.log.info(f"[SEARCH] pcode={one}")
            url = f"https://b2b.autorus.ru/search?pcode={quote(one)}&whCode="
            self._page.goto(url, wait_until="domcontentloaded", timeout=60000)
            self._sleep()

            if self._is_guest_mode(self._page):
                html = self._page.content()
                self._save_debug("search_guest_mode.html", html)
                raise RuntimeError("Autorus: гостевой режим. Обнови state_autorus.json (supplier_login).")

            # Вариант A: “как у AT-HDR-08” — есть блок товара
            title = self._page.locator("span.goodsInfoTitle").first
            if title.count():
                brand = title.locator("span.article-brand").inner_text().strip()
                number = title.locator("span.article-number").inner_text().strip()
                parts_url = self._build_parts_url(brand, number)
                self.log.info(f"[SEARCH] direct hit brand={brand} number={number} parts={parts_url}")
                return {"brand": brand, "number": number, "parts_url": parts_url}

            # Вариант B: список брендов (как в твоём search_unexpected.html)
            # <tr class="startSearching" data-link="/search/3RG/31311">
            row = self._page.locator("table.globalCase tbody tr.startSearching").first
            if row.count():
                data_link = row.get_attribute("data-link") or ""
                if not data_link:
                    html = self._page.content()
                    self._save_debug("search_unexpected_no_datalink.html", html)
                    raise RuntimeError("Autorus: не нашёл data-link в первой строке поиска.")

                search_detail_url = "https://b2b.autorus.ru" + data_link
                self.log.info(f"[SEARCH] list hit -> {search_detail_url}")
                return {"search_detail_url": search_detail_url}

            # Ничего не подошло
            html = self._page.content()
            self._save_debug("search_unexpected.html", html)
            raise RuntimeError("Autorus: неожиданный HTML на /search (сохранено data/search_unexpected.html)")

        # 1) пробуем как есть
        try:
            return _try(pcode)
        except RuntimeError as e:
            # гостевой режим не ретраим
            if "гостевой режим" in str(e).lower():
                raise

        # 2) если есть дефисы — пробуем без них
        p2 = pcode.replace("-", "").replace(" ", "")
        if p2 != pcode:
            return _try(p2)

        # если уже без дефисов, пробросим исходную ошибку
        raise

    
    def get_first_offer_from_parts(self, parts_url: str) -> AutorusOffer | None:
        assert self._page is not None

        # commit — чтобы не зависать на тяжёлых ресурсах
        self._page.goto(parts_url, wait_until="commit")
        try:
            self._page.wait_for_selector(".distrInfoBlockWrapper", timeout=120_000)
        except PWTimeoutError:
            Path("data").mkdir(exist_ok=True)
            self._page.screenshot(path="data/parts_timeout.png", full_page=True)
            with open("data/parts_timeout.html", "w", encoding="utf-8") as f:
                f.write(self._page.content())
            raise RuntimeError("Timeout на /parts. Сохранено data/parts_timeout.*")

        html = self._page.content()
        soup = BeautifulSoup(html, "lxml")

        w = soup.select_one(".distrInfoBlockWrapper")
        if not w:
            Path("data").mkdir(exist_ok=True)
            with open("data/parts_no_offers.html", "w", encoding="utf-8") as f:
                f.write(html)
            return None

        deadline = self._text(w.select_one(".distrInfoDeadline div:nth-of-type(2)"))
        qty_txt = self._text(w.select_one(".distrInfoAvailability .fr-text-nowrap"))
        qty = self._parse_int(qty_txt)
        warehouse = self._text(w.select_one(".distrInfoRoute .fr-text-nowrap"))
        price_txt = self._text(w.select_one(".distrInfoPrice"))
        price = self._parse_price(price_txt)

        return AutorusOffer(warehouse=warehouse, qty=qty, price_rub=price, deadline=deadline)

    def resolve_from_search_detail(self, search_detail_url: str):
        self.log.info(f"[SEARCH-DETAIL] {search_detail_url}")
        self._page.goto(search_detail_url, wait_until="domcontentloaded", timeout=60000)
        self._sleep()

        if self._is_guest_mode(self._page):
            html = self._page.content()
            self._save_debug("search_detail_guest_mode.html", html)
            raise RuntimeError("Autorus: гостевой режим на search detail. Обнови state_autorus.json (supplier_login).")

        title = self._page.locator("span.goodsInfoTitle").first
        if title.count():
            brand = title.locator("span.article-brand").inner_text().strip()
            number = title.locator("span.article-number").inner_text().strip()
            parts_url = self._build_parts_url(brand, number)
            self.log.info(f"[SEARCH-DETAIL] resolved brand={brand} number={number} parts={parts_url}")
            return {"brand": brand, "number": number, "parts_url": parts_url}

        html = self._page.content()
        self._save_debug("search_detail_unexpected.html", html)
        raise RuntimeError("Autorus: не нашёл goodsInfoTitle на /search/BRAND/PCODE (см. data/search_detail_unexpected.html)")

    def ensure_logged_in(self, allow_autologin: bool = False) -> None:
        self.page.goto(f"{self.BASE}/", wait_until="domcontentloaded", timeout=60_000)
        self._sleep()

        if not self._is_guest_mode(self.page):
            return

        if not allow_autologin:
            html = self.page.content()
            self._save_debug("guest_mode.html", html)
            raise RuntimeError(
                "Autorus: гостевой режим. State не применился или протух. "
                f"Проверь path={Path(self.state_path).resolve()} / пересоздай state."
            )

        self._login_via_modal_and_save_state()

    def _login_via_modal_and_save_state(self) -> None:
        login = os.getenv("AUTORUS_LOGIN", "").strip()
        password = os.getenv("AUTORUS_PASSWORD", "").strip()
        if not login or not password:
            raise RuntimeError("Autorus: гостевой режим, но нет AUTORUS_LOGIN/AUTORUS_PASSWORD в .env")

        # Открываем главную и кликаем по кнопке/ссылке логина (открывает модалку)
        self.page.goto(f"{self.BASE}/", wait_until="domcontentloaded", timeout=60_000)
        self._sleep()

        # Открыть модалку логина
        btn = self.page.locator("#logInModal")
        if btn.count() == 0:
            raise RuntimeError("Autorus: не найден #logInModal на странице (изменилась верстка).")
        btn.first.click()

        # Ждём поля модалки
        try:
            self.page.wait_for_selector("input#login.modalWindowControl", timeout=30_000)
            self.page.wait_for_selector("input#oass.modalWindowControl", timeout=30_000)
            self.page.wait_for_selector("input#go.modalWindowSubmitBtn", timeout=30_000)
        except PWTimeoutError:
            raise RuntimeError("Autorus: модалка логина не появилась или селекторы изменились.")

        # Заполняем
        self.page.locator("input#login.modalWindowControl").fill(login)
        self.page.locator("input#oass.modalWindowControl").fill(password)

        # Отправляем
        self.page.locator("input#go.modalWindowSubmitBtn").click()

        # Дать странице время проставить cookies/редиректы
        self.page.wait_for_timeout(2500)

        # Сохраняем state (cookies + storage)
        Path(os.path.dirname(self.state_path) or ".").mkdir(parents=True, exist_ok=True)
        
        self._context.storage_state(path=self.state_path)
