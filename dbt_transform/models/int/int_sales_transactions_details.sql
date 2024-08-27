select
    st.transaction_id,
    st.transaction_date,
    ch.channel_id,
    ch.channel_name,
    c.customer_id,
    c.customer_name,
    c.customer_phone,
    c.age,
    p.product_id,
    p.product_name,
    p.price unit_price,
    st.quantity,
    (st.quantity * p.price) AS sales_amount,
    CASE
        WHEN LEFT(c.customer_phone, 2) = '62' THEN 'Indonesia'
        WHEN LEFT(c.customer_phone, 2) = '65' THEN 'Singapore'
        WHEN LEFT(c.customer_phone, 2) = '60' THEN 'Malaysia'
        WHEN LEFT(c.customer_phone, 2) = '66' THEN 'Thailand'
        WHEN LEFT(c.customer_phone, 2) = '63' THEN 'Philippines'
        WHEN LEFT(c.customer_phone, 2) = '84' THEN 'Vietnam'
        WHEN LEFT(c.customer_phone, 2) = '95' THEN 'Myanmar'
        WHEN LEFT(c.customer_phone, 3) = '856' THEN 'Laos'
        WHEN LEFT(c.customer_phone, 3) = '855' THEN 'Cambodia'
        WHEN LEFT(c.customer_phone, 3) = '673' THEN 'Brunei'
        ELSE 'Negara Tidak Diketahui'
    END AS country,
from
    {{ ref('stg_sales_transactions') }} st
left join 
    {{ ref('stg_customers') }} c 
    ON st.customer_id = c.customer_id
LEFT JOIN 
    {{ ref('stg_channels') }} ch 
    ON st.channel_id = ch.channel_id
LEFT JOIN
    {{ ref('stg_products') }} p 
    ON st.product_id = p.product_id
