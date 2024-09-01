WITH dayly_sales_transactions AS (
    SELECT
        transaction_date,
        channel_id,
        channel_name,
        ROUND(SUM(quantity), 4) AS total_quantity,
        ROUND(SUM(sales_amount), 4) AS total_sales_amount,
        ROUND(SUM(profit), 4) AS total_profit,
    FROM
        {{ ref('int_sales_transactions_details') }}
    GROUP BY
        transaction_date,
        channel_id,
        channel_name
)

SELECT
    cp.channel_id,
    cp.channel_name,
    cp.transaction_date,
    cp.total_impressions,
    cp.total_clicks,
    cp.total_transactions,
    cp.conversion_rate,
    ac.total_cost,
    ac.cost_per_transactions,
    st.total_profit,
    CASE
        WHEN ac.total_cost = 0 THEN NULL  -- Menghindari pembagian dengan nol
        ELSE ROUND(((st.total_profit - ac.total_cost) / ac.total_cost), 4)
    END AS roi
FROM
    {{ ref('int_channel_performance_metrics') }} cp
LEFT JOIN 
    dayly_sales_transactions AS st
    ON cp.transaction_date = st.transaction_date
    AND cp.channel_id = st.channel_id
LEFT JOIN 
    {{ ref('int_customer_aquisition_cost_daily') }} ac
    ON cp.transaction_date = ac.date
    AND cp.channel_id = ac.channel_id
ORDER BY
    cp.transaction_date,
    cp.channel_id
