from __future__ import annotations
from dataclasses import dataclass
from math import ceil
from typing import Optional, Dict, Any


R_COEF = 0.623  # шаг 11


@dataclass(frozen=True)
class PriceInput:
    закуп: float                 # X
    other_costs: float = 120.0   # B
    tax_rate: float = 0.07       # N
    profit_rate: float = 0.15    # E
    delivery_fixed: float = 25.0 # C
    pvz_ship_fixed: float = 25.0 # F
    returns_rate: float = 0.0    # S временно 0
    daily_payout_rate: float = 0.05  # z
    acquiring_rate: float = 0.015    # эквайринг

    # из БД/товара
    markup_percent: float = 0.0  # доп. наценка (если нужна отдельно)


@dataclass(frozen=True)
class DimensionsMM:
    length_mm: int   # depth из Ozon attributes
    width_mm: int
    height_mm: int
    weight_g: int


@dataclass(frozen=True)
class PriceResult:
    final_price: int
    steps: Dict[str, Any]


def volume_liters_from_mm(length_mm: int, width_mm: int, height_mm: int) -> float:
    # мм^3 -> литры: делим на 1_000_000
    v = (float(length_mm) * float(width_mm) * float(height_mm)) / 1_000_000.0
    # защита от нуля
    return max(v, 0.001)


def volume_liters_ozon_round(volume_l: float) -> int:
    """
    По твоим правилам:
    0.001-1 => 1
    1.001-2 => 2
    и т.д.
    """
    return max(1, int(ceil(volume_l)))


def calc_logistics_rub(price_before_logistics: float, volume_l: float) -> float:
    """
    ШАГ 6.
    Тарифы применяются для товаров стоимостью от 301 ₽.
    Объём округляем вверх по правилам.
    """
    if price_before_logistics < 301:
        # Пока не задано, что делать <301.
        # Вариант: считать как для 1 литра или 0.
        # Сейчас делаем 0, чтобы было явно.
        return 0.0

    liters = volume_liters_ozon_round(volume_l)

    if liters <= 1:
        return 81.34
    if liters <= 2:
        return 99.64
    if liters <= 3:
        return 117.94
    if liters <= 190:
        return 117.94 + (liters - 3) * 23.39
    if liters <= 1000:
        # стоимость до 190:
        base_190 = 117.94 + (190 - 3) * 23.39
        return base_190 + (liters - 190) * 6.1
    return 9432.87


def apply_commission_multiplier(price: float, commission_percent: float) -> float:
    return price * (1.0 + commission_percent / 100.0)


def calculate_ozon_price(
    inp: PriceInput,
    dims: DimensionsMM,
    commission_percent: float,
) -> PriceResult:
    steps: Dict[str, Any] = {}

    # объём
    volume_l = volume_liters_from_mm(dims.length_mm, dims.width_mm, dims.height_mm)
    steps["volume_l_raw"] = volume_l
    steps["volume_l_rounded"] = volume_liters_ozon_round(volume_l)

    price = float(inp.закуп)
    steps["step1_закуп"] = price

    # шаг 2
    price += float(inp.other_costs)
    steps["step2_plus_other_costs"] = price

    # шаг 3
    price *= (1.0 + float(inp.tax_rate))
    steps["step3_tax_1_07"] = price

    # шаг 4
    price *= (1.0 + float(inp.profit_rate))
    steps["step4_profit_1_15"] = price

    # (опциональная наценка из БД)
    if inp.markup_percent and inp.markup_percent != 0:
        price *= (1.0 + float(inp.markup_percent) / 100.0)
        steps["step4b_markup_percent"] = price

    # шаг 5
    price += float(inp.delivery_fixed)
    steps["step5_plus_delivery_25"] = price

    # шаг 6 (логистика)
    logistics = calc_logistics_rub(price_before_logistics=price, volume_l=volume_l)
    steps["step6_logistics_rub"] = logistics
    price += logistics
    steps["step6_plus_logistics"] = price

    # шаг 7
    price += float(inp.pvz_ship_fixed)
    steps["step7_plus_pvz_25"] = price

    # шаг 8 (возвраты 0)
    # если позже будет %, то обычно это либо +фикс, либо / (1 - rate)
    steps["step8_returns"] = float(inp.returns_rate)
    # price += 0

    # шаг 9
    price *= (1.0 + float(inp.daily_payout_rate))
    steps["step9_daily_payout_1_05"] = price

    # шаг 10: комиссия + эквайринг
    steps["step10_commission_percent"] = commission_percent
    price = apply_commission_multiplier(price, commission_percent)
    steps["step10_after_commission"] = price

    price *= (1.0 + float(inp.acquiring_rate))
    steps["step10_after_acquiring_1_015"] = price

    # шаг 11
    price /= R_COEF
    steps["step11_div_0_623"] = price
    
    # шаг 12, скидка 50%
    price *= 1.5  
    steps["step12_after_1_5_discount"] = price

    final_price = int(round(price))
    steps["final_rounded"] = final_price

    return PriceResult(final_price=final_price, steps=steps)
