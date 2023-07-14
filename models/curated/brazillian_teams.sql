{{ config(materialized='table') }}

with teams_brazil as (

    SELECT team_id
        , team_code
        , team_name 
        , team_country
        , team_founded
        , team_logo
       , ingestion_date
       from {{ ref('teams_brazil') }}
)
select 
       tb.team_id::int as team_id
     , tb.team_code::STRING as team_code
     , tb.team_name::STRING as team_name 
     , tb.team_country::STRING as team_country
     , tb.team_founded::int as team_founded
     , tb.team_logo::STRING as team_logo
     , TO_TIMESTAMP(ingestion_date::STRING,'YYYY/MM/DD HH24:MI:SS') as ingestion_date
from teams_brazil tb 
