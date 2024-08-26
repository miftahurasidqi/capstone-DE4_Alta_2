select 
    transaction_date,
    channel_id,
    channel_name,
    count(*) as total_transactions,
    sum(total_sales_amount) as daily_sales_amount
from {{ ref('int_sales_transactions') }}
group by
    transaction_date,
    channel_id,
    channel_name