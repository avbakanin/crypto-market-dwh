# Источник: Binance Spot API — klines

- Endpoint: `GET https://api.binance.com/api/v3/klines`
- Зеркало для рыночных данных: `https://data-api.binance.vision`
- Параметры: `symbol`, `interval`, `startTime`, `endTime`, `limit` (по умолчанию 500, максимум 1000)
- Документация: https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints

## Поля (массив из 12 элементов)

| # | Поле | Тип в ответе | Пример (BTCUSDT, 2026-11-19 15:00 UTC) |
|---|------|--------------|------------------------------------------|
| 0 | open_time (мс, UTC) | int | 1795100400000 |
| 1 | open | string | "63558.00000000" |
| 2 | high | string | "63916.01000000" |
| 3 | low | string | "62780.07000000" |
| 4 | close | string | "63103.30000000" |
| 5 | volume (BTC) | string | "1213.38723000" |
| 6 | close_time (мс, UTC) | int | 1795103999999 |
| 7 | quote_volume (USDT) | string | "76724531.04007310" |
| 8 | trades_count | int | 206590 |
| 9 | taker_buy_base_volume | string | "631.83302000" |
| 10 | taker_buy_quote_volume | string | "39946372.67800280" |
| 11 | ignore | string | "0" |

## Наблюдения для Data Quality

1. **Ключ свечи — open_time.** Интервал `[open_time, open_time + 1h − 1ms]`; следующая свеча начинается с `close_time + 1 мс`.
2. **Последняя свеча может быть не закрыта.** 2026-11-19 в 17:22 UTC свеча 17:00 имела volume 509 BTC против ~1200 у закрытых. Правило: загружать только свечи с `close_time < now`.
3. **Цены и объёмы приходят строками**, чтобы не терять точность (quote_volume — до 16 значащих цифр). Хранить как `NUMBER(38, 8)`, не конвертировать во float.
4. **Время в Unix epoch, миллисекунды, UTC.** При конвертации в Python всегда указывать `tz=timezone.utc`.
5. **Объём выборки:** год часовых свечей ≈ 8760 строк, это минимум 9 запросов по 1000 на одну монету.
6. **Опечатка в тикере может молча вернуть другой инструмент.** `BTCUSD` (вместо `BTCUSDT`) отдаёт валидные данные с похожей ценой (+0.2%), но объёмом в ~16 000 раз меньше. Защита: фиксированный список тикеров (`dim_symbol` + dbt-тест `relationships`) и минимальный порог ликвидности.