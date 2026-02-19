from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import random
import time
from urllib.parse import quote, urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PWTimeoutError
from playwright.sync_api import sync_playwright

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


@dataclass(frozen=True)
class SupplierProductSnapshot:
    pcode: str
    brand: str | None
    number: str | None
    parts_url: str | None
    offer: AutorusOffer | None


class AutorusPwSession:
    BASE = "https://b2b.autorus.ru"

    def __init__(self, profile_dir: str = "data/autorus_profile", headless: bool = True) -> None:
        self.profile_dir = str(Path(profile_dir).resolve())
        self.headless = headless
        self._p = None
        self._context = None
        self._page = None
        self.log = setup_logging()
        self.delay_min = 2
        self.delay_max = 3

    def __enter__(self):
        self._p = sync_playwright().start()

        # ВАЖНО: persistent context, а не storage_state
        self._context = self._p.chromium.launch_persistent_context(
            user_data_dir=self.profile_dir,
            headless=self.headless,
            locale="ru-RU",
        )
        self._page = self._context.new_page()
        self._page.set_default_timeout(120_000)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._context:
            self._context.close()
        if self._p:
            self._p.stop()

    @property
    def page(self):
        if self._page is None:
            raise RuntimeError("Page is not initialized")
        return self._page

    def _sleep(self) -> None:
        time.sleep(random.uniform(self.delay_min, self.delay_max))

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

    @staticmethod
    def _normalize_pcode(value: str) -> str:
        return "".join(ch for ch in (value or "").upper() if ch.isalnum())

    @staticmethod
    def _variants_for_search(pcode: str) -> list[str]:
        base = (pcode or "").strip()
        compact = "".join(ch for ch in base if ch.isalnum())
        out = [base]
        if compact and compact != base:
            out.append(compact)
        return [x for x in out if x]

    def _save_debug(self, name: str, html: str) -> None:
        Path("data").mkdir(exist_ok=True)
        with open(f"data/{name}", "w", encoding="utf-8") as f:
            f.write(html)

    def _is_guest_mode(self) -> bool:
        txt = self.page.locator("body").inner_text(timeout=2000)
        return "гостевом режиме" in txt.lower()

    def _ensure_not_guest_or_raise(self, stage: str) -> None:
        if self._is_guest_mode():
            html = self.page.content()
            self._save_debug(f"{stage}_guest_mode.html", html)
            raise RuntimeError("Autorus: guest mode. Refresh state_autorus.json.")

    def _build_parts_url(self, brand: str, number: str) -> str:
        return f"{self.BASE}/parts/{quote(brand, safe='')}/{quote(number, safe='')}"

    def _extract_search_resolution(self, pcode: str, html: str, current_url: str) -> dict | None:
        wanted = self._normalize_pcode(pcode)
        soup = BeautifulSoup(html, "lxml")

        title = soup.select_one("span.goodsInfoTitle")
        if title:
            brand = self._text(title.select_one("span.article-brand")) or self._text(soup.select_one(".article-brand"))
            number = self._text(title.select_one("span.article-number")) or self._text(soup.select_one(".article-number"))
            if brand and number:
                return {
                    "brand": brand,
                    "number": number,
                    "parts_url": self._build_parts_url(brand, number),
                }

        for tr in soup.select("tr[class*='resultTr']"):
            pcode_el = tr.select_one(".resultPartCode a")
            if pcode_el and self._normalize_pcode(self._text(pcode_el)) != wanted:
                continue
            img = tr.select_one("img.searchResultImg")
            if img and img.get("data-brand") and img.get("data-number"):
                brand = str(img.get("data-brand") or "").strip()
                number = str(img.get("data-number") or "").strip()
                if brand and number:
                    return {
                        "brand": brand,
                        "number": number,
                        "parts_url": self._build_parts_url(brand, number),
                    }

        first_row = soup.select_one("table.globalCase tbody tr.startSearching")
        if first_row and first_row.get("data-link"):
            return {"search_detail_url": urljoin(current_url, str(first_row.get("data-link")))}

        return None

    def _resolve_parts_ref_by_pcode(self, pcode: str) -> AutorusPartRef:
        last_error: Exception | None = None

        for one in self._variants_for_search(pcode):
            try:
                search_url = f"{self.BASE}/search?pcode={quote(one)}&whCode="
                self.log.info("[SUPPLIER] search pcode=%s", one)
                self.page.goto(search_url, wait_until="domcontentloaded", timeout=60_000)
                self._sleep()
                self._ensure_not_guest_or_raise("search")

                html = self.page.content()
                resolved = self._extract_search_resolution(one, html, self.page.url)
                if not resolved:
                    continue
                if "parts_url" in resolved:
                    return AutorusPartRef(
                        brand=resolved["brand"],
                        number=resolved["number"],
                        parts_url=resolved["parts_url"],
                    )

                detail_url = resolved["search_detail_url"]
                self.log.info("[SUPPLIER] search detail=%s", detail_url)
                self.page.goto(detail_url, wait_until="domcontentloaded", timeout=60_000)
                self._sleep()
                self._ensure_not_guest_or_raise("search_detail")

                detail_html = self.page.content()
                detail_soup = BeautifulSoup(detail_html, "lxml")
                title = detail_soup.select_one("span.goodsInfoTitle")
                brand = self._text(title.select_one("span.article-brand")) if title else self._text(detail_soup.select_one(".article-brand"))
                number = self._text(title.select_one("span.article-number")) if title else self._text(detail_soup.select_one(".article-number"))
                if brand and number:
                    return AutorusPartRef(
                        brand=brand,
                        number=number,
                        parts_url=self._build_parts_url(brand, number),
                    )
            except Exception as e:
                last_error = e

        self._save_debug("search_unresolved.html", self.page.content())
        if last_error:
            raise RuntimeError(f"Autorus: failed to resolve pcode={pcode}: {last_error}") from last_error
        raise RuntimeError(f"Autorus: failed to resolve pcode={pcode}")

    def _fetch_first_offer_from_parts(self, parts_url: str) -> tuple[str | None, str | None, AutorusOffer | None]:
        self.log.info("[SUPPLIER] parts=%s", parts_url)
        self.page.goto(parts_url, wait_until="domcontentloaded", timeout=120_000)
        self._sleep()
        self._ensure_not_guest_or_raise("parts")

        try:
            self.page.wait_for_selector(".distrInfoBlockWrapper, .article-brand, .article-number", timeout=120_000)
        except PWTimeoutError as e:
            self._save_debug("parts_timeout.html", self.page.content())
            raise RuntimeError("Autorus: timeout on /parts page.") from e

        html = self.page.content()
        soup = BeautifulSoup(html, "lxml")
        brand = self._text(soup.select_one(".article-brand")) or None
        number = self._text(soup.select_one(".article-number")) or None

        block = soup.select_one(".distrInfoBlockWrapper")
        if not block:
            self._save_debug("parts_no_offers.html", html)
            return brand, number, None

        offer = AutorusOffer(
            warehouse=self._text(block.select_one(".distrInfoRoute .fr-text-nowrap")),
            qty=self._parse_int(self._text(block.select_one(".distrInfoAvailability .fr-text-nowrap"))),
            price_rub=self._parse_price(self._text(block.select_one(".distrInfoPrice"))),
            deadline=self._text(block.select_one(".distrInfoDeadline div:nth-of-type(2)")),
        )
        return brand, number, offer

    # New full-cycle supplier fetch
    def fetch_product_snapshot(self, pcode: str, parts_url: str | None = None) -> SupplierProductSnapshot:
        existing_parts = (parts_url or "").strip() or None
        if existing_parts:
            brand, number, offer = self._fetch_first_offer_from_parts(existing_parts)
            return SupplierProductSnapshot(
                pcode=pcode,
                brand=brand,
                number=number,
                parts_url=existing_parts,
                offer=offer,
            )

        resolved = self._resolve_parts_ref_by_pcode(pcode)
        brand, number, offer = self._fetch_first_offer_from_parts(resolved.parts_url)
        return SupplierProductSnapshot(
            pcode=pcode,
            brand=resolved.brand or brand,
            number=resolved.number or number,
            parts_url=resolved.parts_url,
            offer=offer,
        )

    # Backward-compatible wrappers
    def search_pcode(self, pcode: str):
        ref = self._resolve_parts_ref_by_pcode(pcode)
        return {"brand": ref.brand, "number": ref.number, "parts_url": ref.parts_url}

    def resolve_from_search_detail(self, search_detail_url: str):
        self.page.goto(search_detail_url, wait_until="domcontentloaded", timeout=60_000)
        self._sleep()
        self._ensure_not_guest_or_raise("search_detail")

        html = self.page.content()
        soup = BeautifulSoup(html, "lxml")
        title = soup.select_one("span.goodsInfoTitle")
        brand = self._text(title.select_one("span.article-brand")) if title else self._text(soup.select_one(".article-brand"))
        number = self._text(title.select_one("span.article-number")) if title else self._text(soup.select_one(".article-number"))
        if not brand or not number:
            self._save_debug("search_detail_unexpected.html", html)
            raise RuntimeError("Autorus: cannot resolve brand/number from search detail.")

        parts_url = self._build_parts_url(brand, number)
        return {"brand": brand, "number": number, "parts_url": parts_url}

    def get_first_offer_from_parts(self, parts_url: str) -> AutorusOffer | None:
        _, _, offer = self._fetch_first_offer_from_parts(parts_url)
        return offer

