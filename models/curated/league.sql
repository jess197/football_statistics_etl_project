{{ config(materialized='table') }}

with staging_standings_league as (

SELECT league_id
     , league_country
     , league_country_flag
     , league_logo
     , league_name
  FROM {{ ref('teams_standings') }}

)

SELECT league_id::int as league_id
     , league_country::VARCHAR(30) as league_country
     , league_country_flag::VARCHAR(100) as league_country_flag
     , league_logo::VARCHAR(100) as league_logo
     , league_name::VARCHAR(20) as league_name
  FROM staging_standings_league
  limit 1