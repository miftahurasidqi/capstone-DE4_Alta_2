WITH dayly_transactions AS (
    SELECT
        transaction_date,
        channel_id,
        COUNT(transaction_id) AS total_transactions
    FROM
        {{ ref('stg_sales_transactions') }}
    GROUP BY
        transaction_date,
        channel_id
)

SELECT
    cp.channel_id,
    ch.channel_name,
    cp.transaction_date,
    cp.total_impressions,
    cp.total_clicks,
    t.total_transactions,
    CASE 
        WHEN cp.total_clicks = 0 THEN 0
        ELSE ROUND((t.total_transactions * 1.0) / cp.total_clicks * 100, 2)
    END AS conversion_rate
FROM
    {{ ref('stg_channel_performances') }} cp
LEFT JOIN 
    {{ ref('stg_channels') }} AS ch
    ON cp.channel_id = ch.channel_id
LEFT JOIN 
    dayly_transactions AS t
    ON cp.transaction_date = t.transaction_date
    AND cp.channel_id = t.channel_id
ORDER BY
    cp.transaction_date,
    cp.channel_id
