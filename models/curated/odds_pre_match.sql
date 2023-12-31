{{ config(materialized='table') }}

with staging_teams_odds_pm as (
    select odd_value
         , odd 
         , fixture_id
         , bookmakers_id
         , bet_id
         , file_last_update
         , ingestion_date
      from {{ ref('teams_odds_pre_match') }}
)
SELECT {{ dbt_utils.generate_surrogate_key(['op.fixture_id','op.ingestion_date']) }} as odd_pm_id
     , op.fixture_id::int as fixture_id
     , op.bookmakers_id::int as bookmakers_id
     , op.bet_id::int as bet_id 
     , MAX(CASE WHEN op.odd_value::VARCHAR(20) = 'Home' THEN op.odd::VARCHAR(20) ELSE NULL END) AS odd_value_home
     , MAX(CASE WHEN op.odd_value::VARCHAR(20) = 'Draw' THEN op.odd::VARCHAR(20) ELSE NULL END) AS odd_value_draw
     , MAX(CASE WHEN op.odd_value::VARCHAR(20) = 'Away' THEN op.odd::VARCHAR(20) ELSE NULL END) AS odd_value_away
     , TO_TIMESTAMP_NTZ(file_last_update::STRING) as file_last_update
     , TO_TIMESTAMP(op.ingestion_date::STRING,'YYYY/MM/DD HH24:MI:SS') as ingestion_date
  FROM staging_teams_odds_pm op 
  GROUP BY ALL 