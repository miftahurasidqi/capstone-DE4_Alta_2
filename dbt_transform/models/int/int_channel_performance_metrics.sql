select  
    cp.channel_id,
    c.channel_name,
    cp.transaction_date,
    cp.total_impressions,
    cp.total_clicks,
    dst.total_transactions,
    dst.daily_sales_amount

FROM 
    {{ ref('stg_channel_performances')}} cp
LEFT JOIN 
    {{ ref('stg_channels')}} c ON cp.channel_id = c.channel_id
LEFT JOIN 
    {{ ref('int_dayliy_sales_transaction')}} dst ON cp.channel_id = dst.channel_id AND cp.transaction_date = dst.transaction_date
