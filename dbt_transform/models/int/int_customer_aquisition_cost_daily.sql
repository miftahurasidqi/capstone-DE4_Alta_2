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
    ac.channel_id,
    ch.channel_name,
    ac.date,
    ac.total_cost,
    t.total_transactions,
    CASE
        WHEN t.total_transactions > 0 
        THEN ROUND(ac.total_cost / t.total_transactions, 4)
        ELSE 0
    END AS cost_per_customer
FROM
    {{ ref('stg_acquisition_costs') }} AS ac
LEFT JOIN
    {{ ref('stg_channels') }} AS ch
    ON ac.channel_id = ch.channel_id
LEFT JOIN
    dayly_transactions AS t
    ON ac.date = t.transaction_date
    AND ac.channel_id = t.channel_id
ORDER BY
    ac.date,
    ac.channel_id
