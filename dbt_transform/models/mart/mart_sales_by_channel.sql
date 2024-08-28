-- Agregat Data Penjualan Menurut Saluran

-- mdl_sales_by_channel.sql

WITH sales_data AS (
    SELECT 
        channel_id,
        channel_name,
        sum(total_transactions) AS total_orders,
        SUM(daily_sales_amount) AS total_sales_amount,
        SUM(quantity_sold) AS total_quantity_sold
    FROM 
        {{ ref('fct_sales_transactions_daily') }}
    GROUP BY 
        channel_id, channel_name
),

channel_performances_metrics AS (
    SELECT 
        cp.channel_id,
        ROUND(sum(conversion_rate), 4) AS total_conversion_rate
    FROM 
        {{ ref('fct_channel_performances_metrics_details') }} cp
    GROUP BY 
        cp.channel_id
)

SELECT 
    sd.channel_id,
    sd.channel_name,
    sd.total_orders,
    sd.total_sales_amount,
    sd.total_quantity_sold,
    ROUND(COALESCE(sd.total_sales_amount / NULLIF(sd.total_orders, 0), 0), 4) AS average_order_value,
    cp.total_conversion_rate

FROM sales_data sd
LEFT JOIN channel_performances_metrics cp
    ON sd.channel_id = cp.channel_id