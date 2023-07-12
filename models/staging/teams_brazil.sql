{{ config(materialized='table') }}

with source_teams_brazil as (

    select RAW_FILE
      from RAW.TEAMS_BRAZIL

)
select 
     r.value:team:id as team_id 
    ,r.value:team:code as team_code
    ,r.value:team:name as team_name
    ,r.value:team:country as team_country
    ,r.value:team:founded as team_founded
    ,r.value:team:logo as team_logo
    ,r.value:venue:id as venue_id 
    ,r.value:venue:name as venue_name
    ,r.value:venue:address as venue_address
    ,r.value:venue:city as venue_city
    ,r.value:venue:capacity as venue_capacity
    ,r.value:venue:surface as venue_surface
    ,r.value:venue:image as venue_image
    ,stb.RAW_FILE:ingestion_date as ingestion_date
from source_teams_brazil stb
,lateral flatten(input => stb.RAW_FILE, path => 'response') r
where r.value:team:national = false