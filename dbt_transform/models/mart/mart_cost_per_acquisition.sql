SELECT 
    channel_id,
    channel_name,
    SUM(total_transactions) as total_orders,
    SUM(total_cost) as total_cost,
    SUM(cost_per_customer) as cost_per_acquisition
FROM 
    {{ ref('int_customer_aquisition_cost_daily') }}
GROUP BY
    channel_id,
    channel_name