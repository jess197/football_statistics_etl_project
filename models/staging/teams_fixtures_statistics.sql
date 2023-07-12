{{ config(materialized='table') }}

with source_teams_fixtures_stat as (

    select RAW_FILE
      from RAW.TEAMS_FIXTURES_STATISTICS
)
select sts.raw_file:fixture_id as fixture_id 
      , r.value:team:id as team_id 
      , r.value:team:name as team_name 
      , s.value:type as statistic_type
      , s.value:value as statistic_value
      , sts.raw_file:ingestion_date as ingestion_date
  from source_teams_fixtures_stat sts
    ,lateral flatten(input => sts.RAW_FILE, path => 'response') r
    ,lateral flatten(input => r.value, path => 'statistics') s