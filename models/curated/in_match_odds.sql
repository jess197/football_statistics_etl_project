{{ config(materialized='table') }}

with in_match_odds as (
    select fixture_id
         , match_time_minutes
         , match_time
         , odd_name
         , odd
         , team_to_win
         , ingestion_date
      from {{ ref('odds_in_match') }}
)
select fixture_id::int as fixture_id
         , match_time_minutes::int as match_time_minutes
         , match_time::VARCHAR(30) as match_time
         , odd_name::VARCHAR(30) as odd_name
         , odd::VARCHAR(30) as odd
         , team_to_win::VARCHAR(30) as team_to_win
         , TO_TIMESTAMP(ingestion_date::STRING,'YYYY/MM/DD HH24:MI:SS') as ingestion_date
  from in_match_odds
 where odd_name like '%Fulltime Result%'