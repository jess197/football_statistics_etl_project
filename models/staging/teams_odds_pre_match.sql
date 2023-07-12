{{ config(materialized='table') }}

with source_teams_odds_pre_match as (

    select RAW_FILE
      from RAW.TEAMS_ODDS_PRE_MATCH

)
select btv.value:value
     , btv.value:odd
     , rf.value:fixture:id as fixture_id
     , rf.value:fixture:date as fixture_date 
     , rf.value:league:id as league_id
     , rf.value:league:season as league_season 
     , bm.value:id as bookmakers_id 
     , bm.value:name as bookmakers_name
     , bt.value:name as bet_name 
     , bt.value:id as bet_id 
     , rf.value:update as file_last_update 
     , RAW_FILE:ingestion_date as ingestion_date
  from source_teams_odds_pre_match  sto
,lateral flatten(input => sto.RAW_FILE) rf
,lateral flatten(input => rf.value, path => 'bookmakers') bm
,lateral flatten(input => bm.value, path => 'bets') bt
,lateral flatten(input => bt.value, path => 'values') btv

UNION ALL

select btv.value:value
     , btv.value:odd
     , rf.value:fixture:id as fixture_id
     , rf.value:fixture:date as fixture_date 
     , rf.value:league:id as league_id
     , rf.value:league:season as league_season 
     , bm.value:id as bookmakers_id 
     , bm.value:name as bookmakers_name
     , bt.value:name as bet_name 
     , bt.value:id as bet_id 
     , rf.value:update as file_last_update 
     , RAW_FILE:ingestion_date as ingestion_date
  from source_teams_odds_pre_match  sto
,lateral flatten(input => sto.RAW_FILE, path => 'response') rf
,lateral flatten(input => rf.value, path => 'bookmakers') bm
,lateral flatten(input => bm.value, path => 'bets') bt
,lateral flatten(input => bt.value, path => 'values') btv