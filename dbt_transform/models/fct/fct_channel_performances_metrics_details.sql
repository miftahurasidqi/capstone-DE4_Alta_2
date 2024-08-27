WITH dayly_sales_transactions AS (
    SELECT
        transaction_date,
        channel_id,
        channel_name,
        ROUND(SUM(sales_amount), 4) AS total_sales_amount
    FROM
        {{ ref('int_sales_transactions_details') }}
    GROUP BY
        transaction_date,
        channel_id,
        channel_name
), customer_aquisition_cost_daily AS (
    SELECT
        date AS transaction_date,
        channel_id,
        ROUND(SUM(total_cost), 4) AS total_cost,
        ROUND(AVG(cost_per_customer), 4) AS cost_per_customer
    FROM
        {{ ref('int_customer_aquisition_cost_daily') }}
    GROUP BY
        date,
        channel_id
)

SELECT
    cp.channel_id,
    cp.channel_name,
    cp.transaction_date,
    cp.total_clicks,
    cp.total_transactions,
    cp.conversion_rate,
    ac.total_cost,
    ac.cost_per_customer,
    st.total_sales_amount,
    CASE
        WHEN ac.total_cost = 0 THEN NULL  -- Menghindari pembagian dengan nol
        ELSE ROUND(((st.total_sales_amount - ac.total_cost) / ac.total_cost), 4)
    END AS roi
FROM
    {{ ref('int_channel_performance_metrics') }} cp
LEFT JOIN 
    dayly_sales_transactions AS st
    ON cp.transaction_date = st.transaction_date
    AND cp.channel_id = st.channel_id
LEFT JOIN 
    customer_aquisition_cost_daily AS ac
    ON cp.transaction_date = ac.transaction_date
    AND cp.channel_id = ac.channel_id
ORDER BY
    cp.transaction_date,
    cp.channel_id
