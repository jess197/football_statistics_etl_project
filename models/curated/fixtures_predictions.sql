{{ config(materialized='table') }}

with teams_fixtures_predictions as (

    SELECT 
          fixture_id
         ,pred_winner_id
         ,pred_winner_name
         ,pred_comment
         ,pred_advice
         ,pred_goals_away
         ,pred_goals_home
         ,pred_percent_home
         ,pred_percent_away
         ,pred_percent_draw
         ,pred_win_or_draw
         ,pred_under_over
         ,ingestion_date
     FROM {{ ref('teams_fixtures_predictions') }}
)
select   
      tp.fixture_id::int as fixture_id
     ,tp.pred_winner_id::int as pred_winner_id
     ,tp.pred_winner_name::VARCHAR(30) as pred_winner_name
     ,tp.pred_comment::VARCHAR(100) as pred_comment
     ,tp.pred_advice::VARCHAR(100) as pred_advice
     ,tp.pred_goals_away::int as pred_goals_away
     ,tp.pred_goals_home::int as pred_goals_home
     ,tp.pred_percent_home::VARCHAR(5) as pred_percent_home
     ,tp.pred_percent_away::VARCHAR(5) as pred_percent_away
     ,tp.pred_percent_draw::VARCHAR(5) as pred_percent_draw
     ,tp.pred_win_or_draw::BOOLEAN as pred_win_or_draw
     ,tp.pred_under_over::VARCHAR(10) as pred_under_over
     ,TO_TIMESTAMP(tp.ingestion_date::STRING,'YYYY/MM/DD HH24:MI:SS') as ingestion_date
from teams_fixtures_predictions tp 
