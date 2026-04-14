# Старт

1) заполнить шаблон `.env`

2) Для создания сессии использовать

    `python -m src.app.supplier_client`

3) Для запуска скрипта использовать

    `python -m src.app.main`

## Ручные скрипты

1) Скрипт экстренного обнуления остатков

    `python -m src.app.utils.zero_stocks`

2) Скрипт ручного обновления цены конкретного товара

    `python -m src.app.utils.update_price_by_sku --article АРТИКУЛ --price НОВАЯ_ЦЕНА`
