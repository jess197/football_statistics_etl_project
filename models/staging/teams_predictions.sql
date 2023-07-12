{{ config(materialized='table') }}

with source_teams_fixtures_predictions as (

    select RAW_FILE
      from RAW.TEAMS_FIXTURES_PREDICTIONS

)
select $1:fixture_id as fixture_id
     , $1:winner:id as pred_winner_id 
     , $1:winner:name as pred_winner_name 
     , $1:winner:comment as pred_comment
     , $1:advice as pred_advice 
     , $1:goals:away as pred_goals_away 
     , $1:goals:home as pred_goals_home 
     , $1:percent:home as pred_percent_home 
     , $1:percent:away as pred_percent_away 
     , $1:percent:draw as pred_percent_draw
     , $1:win_or_draw as pred_win_or_draw 
     , $1:under_over as pred_under_over
     , $1:ingestion_date as ingestion_date
  from source_teams_fixtures_predictions stp