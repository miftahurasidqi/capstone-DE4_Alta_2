SELECT DISTINCT
    channel_id,
    channel_name,
    type_channel
FROM {{ source('ecommers_de4_team_2', 'raw_channels') }}
