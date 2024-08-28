SELECT
    DATE_TRUNC(DATE(transaction_date), WEEK) AS week,
    channel_id,
    channel_name,
    SUM(total_transactions) AS total_transactions_weekly,
    ROUND(SUM(cost_per_customer), 4) AS total_cost_per_customer,
    ROUND(SUM(total_profit), 4) AS total_profit,
    ROUND(SUM(roi), 4) AS total_roi
FROM
    {{ ref('fct_channel_performances_metrics_details') }}
GROUP BY
    week,
    channel_id,
    channel_name
ORDER BY
    week,
    channel_id

