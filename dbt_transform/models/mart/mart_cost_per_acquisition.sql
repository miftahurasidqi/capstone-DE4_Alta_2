SELECT 
    channel_id,
    channel_name,
    SUM(total_transactions) as total_customer_transactions,
    SUM(total_cost) as total_cost,
    SUM(cost_per_transactions) as cost_per_acquisition
FROM 
    {{ ref('fct_channel_performances_metrics_details') }}
GROUP BY
    channel_id,
    channel_name