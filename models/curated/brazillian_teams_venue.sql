{{ config(materialized='table') }}

with staging_teams_brazil as (
    SELECT 
          team_id
        , venue_id
        , venue_name
        , venue_address
        , venue_city
        , venue_capacity
        , venue_surface
        , venue_image
        , ingestion_date
      from {{ ref('teams_brazil') }}

)
    
SELECT venue_id::numeric as venue_id 
     , team_id::number as team_id
     , venue_name::STRING as venue_name
     , venue_address::STRING as venue_address
     , venue_city::STRING as venue_city
     , venue_capacity::int as venue_capacity
     , venue_surface::STRING as venue_surface
     , venue_image::STRING as venue_image
     , TO_TIMESTAMP(ingestion_date::STRING,'YYYY/MM/DD HH24:MI:SS') as ingestion_date
 FROM staging_teams_brazil