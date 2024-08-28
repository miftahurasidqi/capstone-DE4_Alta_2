SELECT DISTINCT *
FROM {{ source('ecommers_de4_team_2','raw_products') }}