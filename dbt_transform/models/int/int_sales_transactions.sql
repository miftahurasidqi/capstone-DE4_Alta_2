-- select *
-- from {{ ref('stg_sales_transactions') }}

select
    st.transaction_id,
    st.transaction_date,
    st.customer_id,
    c.customer_name,
    c.normalized_phone,
    c.age,
    st.channel_id,
    ch.channel_name,
    p.product_id,
    p.product_name,
    st.quantity,
    p.price unit_price,
    (st.quantity * p.price) AS total_sales_amount
from
    {{ ref('stg_sales_transactions') }} st
left join 
    {{ ref('stg_customers') }} c ON st.customer_id = c.customer_id
LEFT JOIN 
    {{ ref('stg_channels') }} ch ON st.channel_id = ch.channel_id
LEFT JOIN
    {{ ref('stg_products') }} p ON st.product_id = p.product_id 
    