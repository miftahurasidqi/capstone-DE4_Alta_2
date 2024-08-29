-- mart_sales_by_channel.sql

WITH sales_data AS (
    SELECT 
        channel_id,
        transaction_date,
        channel_name,
        total_transactions AS total_orders,
        daily_sales_amount AS sales_amount,
        quantity_sold,
        daily_profit
    FROM 
        {{ ref('fct_sales_transactions_daily') }}
)

SELECT *
FROM sales_data
