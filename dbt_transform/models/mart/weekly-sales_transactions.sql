SELECT
    DATE_TRUNC(DATE(transaction_date), WEEK) AS week,
    channel_id,
    channel_name,
    SUM(total_transactions) AS total_transactions_weekly,
    ROUND(SUM(daily_sales_amount), 4) AS weekly_revenue
FROM
    {{ ref('fct_sales_transactions_daily') }}
GROUP BY
    week,
    channel_id,
    channel_name
ORDER BY
    week,
    channel_id


