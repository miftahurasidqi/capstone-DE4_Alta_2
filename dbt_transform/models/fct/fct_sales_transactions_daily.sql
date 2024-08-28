SELECT 
    transaction_date,
    channel_id,
    channel_name,
    count(*) AS total_transactions,
    ROUND(sum(quantity), 4) AS quantity_sold,
    ROUND(sum(sales_amount), 4) AS daily_sales_amount,
    ROUND(sum(profit), 4) AS daily_profit
FROM {{ ref('int_sales_transactions_details') }}
GROUP BY
    transaction_date,
    channel_id,
    channel_name