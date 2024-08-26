-- remove duplicates 

select distinct
    channel_id,
    channel_name,
    type_channel
from {{ source('ecommers_de4_team_2', 'raw_channels') }}
