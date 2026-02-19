from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import settings


class OzonApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class OzonProductListItem:
    product_id: int
    offer_id: str
    archived: bool
    has_fbo_stocks: bool
    has_fbs_stocks: bool
    quants: list  # как есть из API (позже можно типизировать)

@dataclass(frozen=True)
class OzonProductInfo:
    product_id: int
    offer_id: str
    name: Optional[str]
    weight_g: Optional[int]
    length_mm: Optional[int]
    width_mm: Optional[int]
    height_mm: Optional[int]

@dataclass(frozen=True)
class OzonProductInfoRow:
    product_id: int
    offer_id: str
    price_current: Optional[float]
    archived: bool
    moderate_status: Optional[str]
    validation_status: Optional[str]
    status: Optional[str]
    description_category_id: Optional[int]
    commission_fbs_percent: Optional[float]

@dataclass(frozen=True)
class OzonAttributesRow:
    product_id: int
    offer_id: str
    name: Optional[str]
    weight_g: Optional[int]
    length_mm: Optional[int]  # depth
    width_mm: Optional[int]
    height_mm: Optional[int]
    

class OzonClient:
    def __init__(self) -> None:
        self._client = httpx.Client(
            base_url=settings.ozon_base_url,
            timeout=settings.ozon_timeout_sec,
            headers={
                "Client-Id": str(settings.ozon_client_id),
                "Api-Key": str(settings.ozon_api_key),
                "Content-Type": "application/json",
            },
        )

    def close(self) -> None:
        self._client.close()
        
    @staticmethod
    def _to_int(v) -> Optional[int]:
        try:
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return int(round(v))
            s = str(v).strip().replace(",", ".")
            if not s:
                return None
            return int(round(float(s)))
        except Exception:
            return None

    @staticmethod
    def _extract_dims_mm(item: dict) -> tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
        """
        Ozon в разных методах/версиях может отдавать габариты под разными ключами.
        Стараемся достать из нескольких вариантов.
        Единицы чаще всего мм и граммы (в требованиях к карточкам у Ozon это типично). :contentReference[oaicite:1]{index=1}
        """
        weight = None
        length = None
        width = None
        height = None

        # Вариант A: item["volume_weight"] / item["vwc"] подобные структуры
        vw = item.get("volume_weight") or item.get("vwc") or {}
        if isinstance(vw, dict):
            weight = weight or OzonClient._to_int(vw.get("weight") or vw.get("weight_g") or vw.get("weight_gram"))
            length = length or OzonClient._to_int(vw.get("length") or vw.get("length_mm"))
            width  = width  or OzonClient._to_int(vw.get("width")  or vw.get("width_mm"))
            height = height or OzonClient._to_int(vw.get("height") or vw.get("height_mm"))

        # Вариант B: item["dimensions"]
        dims = item.get("dimensions") or {}
        if isinstance(dims, dict):
            weight = weight or OzonClient._to_int(dims.get("weight") or dims.get("weight_g"))
            length = length or OzonClient._to_int(dims.get("length") or dims.get("depth") or dims.get("length_mm"))
            width  = width  or OzonClient._to_int(dims.get("width") or dims.get("width_mm"))
            height = height or OzonClient._to_int(dims.get("height") or dims.get("height_mm"))

        # Вариант C: плоские поля
        weight = weight or OzonClient._to_int(item.get("weight") or item.get("weight_g"))
        length = length or OzonClient._to_int(item.get("length") or item.get("depth") or item.get("length_mm"))
        width  = width  or OzonClient._to_int(item.get("width") or item.get("width_mm"))
        height = height or OzonClient._to_int(item.get("height") or item.get("height_mm"))

        return weight, length, width, height

    @retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(5))
    def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = self._client.post(path, json=payload)
        if r.status_code >= 400:
            raise OzonApiError(f"HTTP {r.status_code}: {r.text[:500]}")
        data = r.json()
        # У Ozon часто ошибки лежат в json-структуре, даже при 200
        if isinstance(data, dict) and data.get("error"):
            raise OzonApiError(f"API error: {data.get('error')}")
        return data
    
    @staticmethod
    def _parse_price(value) -> float | None:
        if value is None:
            return None
        try:
            s = str(value).strip().replace(",", ".")
            if not s:
                return None
            return float(s)
        except Exception:
            return None

    def list_products_all(
        self,
        *,
        include_archived: bool = False,
        visibility: str = "ALL",
        limit: Optional[int] = None,
    ) -> List[OzonProductListItem]:
        """
        Получить все товары из /v3/product/list.
        Чтобы исключить архив — фильтруем по полю 'archived' из ответа.
        """
        page_limit = limit or settings.ozon_limit_per_page
        if page_limit < 1 or page_limit > 1000:
            raise ValueError("limit должен быть в диапазоне 1..1000")

        out: List[OzonProductListItem] = []
        last_id = ""

        while True:
            payload = {
                "filter": {
                    "visibility": visibility,
                },
                "limit": page_limit,
                "last_id": last_id,
            }

            resp = self._post("/v3/product/list", payload)
            result = resp.get("result") or {}
            items = result.get("items") or []
            last_id = result.get("last_id") or ""

            for it in items:
                item = OzonProductListItem(
                    product_id=int(it.get("product_id")),
                    offer_id=str(it.get("offer_id") or ""),
                    archived=bool(it.get("archived")),
                    has_fbo_stocks=bool(it.get("has_fbo_stocks")),
                    has_fbs_stocks=bool(it.get("has_fbs_stocks")),
                    quants=it.get("quants") or [],
                )
                if not include_archived and item.archived:
                    continue
                if not item.offer_id:
                    # offer_id бывает пустым на некоторых сущностях — пропускаем
                    continue
                out.append(item)

            # пагинация: если last_id пустой — данных больше нет
            if not last_id:
                break

        return out
    
    @staticmethod
    def _extract_fbs_commission_percent(it: dict) -> float | None:
        for c in it.get("commissions") or []:
            if c.get("sale_schema") == "FBS" and c.get("percent") is not None:
                return float(c["percent"])
        return None
    
    def get_product_info_list_by_offer_ids(self, offer_ids: list[str]) -> list[OzonProductInfoRow]:
        if not offer_ids:
            return []
        if len(offer_ids) > 1000:
            raise ValueError("offer_ids: максимум 1000 за запрос")

        payload = {"offer_id": offer_ids}
        data = self._post("/v3/product/info/list", payload)

        items = data.get("items") or []
        out: list[OzonProductInfoRow] = []
        for it in items:
            statuses = it.get("statuses") or {}
            product_id = int(it.get("id") or it.get("product_id") or 0)
            offer_id = str(it.get("offer_id") or "")
            price_current = self._parse_price(it.get("price"))
            if not offer_id or not product_id:
                continue

            cat_id = it.get("description_category_id")
            cat_id = int(cat_id) if cat_id is not None else None

            out.append(
                OzonProductInfoRow(
                    product_id=product_id,
                    offer_id=offer_id,
                    price_current=price_current,
                    archived=bool(it.get("is_archived") or it.get("archived") or False),
                    moderate_status=statuses.get("moderate_status"),
                    validation_status=statuses.get("validation_status"),
                    status=statuses.get("status"),
                    description_category_id=cat_id,
                    commission_fbs_percent=self._extract_fbs_commission_percent(it),
                )
            )
        return out
    
    def get_attributes_by_offer_ids(self, offer_ids: list[str]) -> list[OzonAttributesRow]:
        """
        /v4/product/info/attributes
        В твоём ответе структура: {"result":[...]} и поля height/depth/width/weight. :contentReference[oaicite:5]{index=5}
        """
        if not offer_ids:
            return []
        if len(offer_ids) > 1000:
            raise ValueError("offer_ids: максимум 1000 за запрос")

        payload = {"filter": {"offer_id": offer_ids}, "limit": 1000, "last_id": ""}
        data = self._post("/v4/product/info/attributes", payload)

        result = data.get("result") or []
        out: list[OzonAttributesRow] = []
        for it in result:
            unit_dim = it.get("dimension_unit")
            unit_w = it.get("weight_unit")

            # ожидаем mm и g (как в твоём JSON) :contentReference[oaicite:6]{index=6}
            def to_int(v):
                try:
                    return int(v) if v is not None else None
                except Exception:
                    return None

            height = to_int(it.get("height"))
            depth  = to_int(it.get("depth"))   # считаем как length_mm
            width  = to_int(it.get("width"))
            weight = to_int(it.get("weight"))

            out.append(
                OzonAttributesRow(
                    product_id=int(it.get("id") or 0),
                    offer_id=str(it.get("offer_id") or ""),
                    name=it.get("name"),
                    weight_g=weight if unit_w == "g" else weight,
                    length_mm=depth if unit_dim == "mm" else depth,
                    width_mm=width if unit_dim == "mm" else width,
                    height_mm=height if unit_dim == "mm" else height,
                )
            )
        return [x for x in out if x.offer_id and x.product_id]
