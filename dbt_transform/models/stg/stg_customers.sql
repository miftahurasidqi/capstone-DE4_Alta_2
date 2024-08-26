select distinct
    cast(customer_id as INT64) as customer_id,
    customer_name,
    phone_number,
    {{ normalize_phone_number('raw_customers.phone_number') }} as normalized_phone,
    age
from {{source('ecommers_de4_team_2','raw_customers')}}
