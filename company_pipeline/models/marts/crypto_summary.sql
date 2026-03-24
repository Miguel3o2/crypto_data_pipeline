SELECT
    coin,
    price_usd,
    ROUND(change_24h::numeric, 2)   AS change_24h_pct,
    CASE
        WHEN change_24h > 0 THEN 'UP'
        WHEN change_24h < 0 THEN 'DOWN'
        ELSE 'FLAT'
    END                             AS trend,
    timestamp
FROM public.crypto_prices
ORDER BY price_usd DESC