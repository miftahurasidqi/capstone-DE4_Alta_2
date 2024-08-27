SELECT DISTINCT
    CAST(customer_id AS INT64) AS customer_id,
    customer_name,
    {{ normalize_phone_number('raw_customers.phone_number') }} AS customer_phone,
    age
FROM {{ source('ecommers_de4_team_2','raw_customers') }}
