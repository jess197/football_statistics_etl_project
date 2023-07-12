{{ config(materialized='table') }}

with source_teams_fixtures as (

    select RAW_FILE
      from RAW.TEAMS_FIXTURES
)
select r.value:fixture:id as fixture_id 
     , r.value:fixture:date as fixture_date 
     , r.value:fixture:referee as fixture_referee 
     , r.value:fixture:venue:id as fixture_venue_id 
     , r.value:fixture:status:long as fixture_status_long
     , r.value:fixture:status:short as fixture_status_short
     , r.value:fixture:status:elapsed as fixture_status_elapsed
     , r.value:league:id as league_id 
     , r.value:league:season as league_season 
     , r.value:league:round as league_round 
     , r.value:teams:home:id as team_home_id 
     , r.value:teams:away:id as team_away_id 
     , r.value:teams:home:winner as team_home_winner 
     , r.value:teams:away:winner as team_away_winner 
     , r.value:goals:home as team_goals_home 
     , r.value:goals:away as team_goals_away
     , stf.raw_file:ingestion_date as ingestion_date
  from source_teams_fixtures stf
    ,lateral flatten(input => stf.RAW_FILE, path => 'response') r