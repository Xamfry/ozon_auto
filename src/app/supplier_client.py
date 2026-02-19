from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any
from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page


@dataclass(frozen=True)
class SupplierItem:
    article: str
    price: Optional[float]
    stock: Optional[int]
    raw: Dict[str, Any]  # для отладки (что нашли)


class SupplierClient:
    """
    Вариант без API: Playwright + сохранённая сессия (storage_state).
    """

    def __init__(self, state_path: str = "state_autorus.json", headless: bool = True) -> None:
        self.state_path = state_path
        self.headless = headless
        self._p = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    def __enter__(self) -> "SupplierClient":
        self._p = sync_playwright().start()
        self._browser = self._p.chromium.launch(headless=self.headless)
        self._context = self._browser.new_context(storage_state=self.state_path)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._p:
            self._p.stop()

    def _new_page(self) -> Page:
        assert self._context is not None
        page = self._context.new_page()
        page.set_default_timeout(60_000)
        return page

    @staticmethod
    def _parse_price(value: str) -> Optional[float]:
        try:
            s = (value or "").strip().replace(" ", "").replace(",", ".")
            if not s:
                return None
            # вырезаем валюту/мусор
            s = "".join(ch for ch in s if ch.isdigit() or ch == ".")
            return float(s) if s else None
        except Exception:
            return None

    @staticmethod
    def _parse_int(value: str) -> Optional[int]:
        try:
            s = (value or "").strip()
            if not s:
                return None
            s = "".join(ch for ch in s if ch.isdigit())
            return int(s) if s else None
        except Exception:
            return None

    def find_by_article(self, article: str) -> SupplierItem:
        """
        ВАЖНО: селекторы и URL зависят от того, как устроен поиск у поставщика.
        Сейчас это каркас:
        - открываем сайт
        - вводим артикул в поиск
        - пытаемся вытащить результат

        Следующий шаг: ты даёшь реальный HTML/Network и мы фиксируем:
        - URL поиска
        - селектор поля
        - селекторы цены/остатка
        """
        page = self._new_page()

        # 1) открыть главную/каталог, где есть поиск
        page.goto("https://b2b.autorus.ru/", wait_until="domcontentloaded")

        # 2) попытка найти поле поиска (пока универсально, скорее всего надо будет заменить)
        # Примеры возможных селекторов:
        # input[type="search"], input[name="search"], input[placeholder*="Поиск"]
        search = page.locator('input[type="search"], input[name*="search"], input[placeholder*="Поиск"], input[placeholder*="поиск"]').first
        if search.count() == 0:
            raise RuntimeError("Не найдено поле поиска. Нужно уточнить селектор по странице.")

        search.fill(article)
        search.press("Enter")

        # 3) ждём обновления (это тоже может требовать точного ожидания)
        page.wait_for_timeout(1500)

        # 4) попытка вытащить данные по “первому результату”
        # Эти селекторы почти наверняка придётся подстроить.
        price_text = None
        stock_text = None

        # пробуем найти цену по типичным классам/атрибутам
        price_candidates = page.locator('[class*="price"], [data-testid*="price"], [class*="Price"]').first
        if price_candidates.count() > 0:
            price_text = price_candidates.inner_text().strip()

        stock_candidates = page.locator('[class*="stock"], [data-testid*="stock"], [class*="quantity"], [class*="Qty"]').first
        if stock_candidates.count() > 0:
            stock_text = stock_candidates.inner_text().strip()

        price = self._parse_price(price_text or "")
        stock = self._parse_int(stock_text or "")

        raw = {
            "price_text": price_text,
            "stock_text": stock_text,
            "url": page.url,
        }

        page.close()
        return SupplierItem(article=article, price=price, stock=stock, raw=raw)
