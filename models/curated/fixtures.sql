{{ config(materialized='table') }}

with teams_fixtures as (

    SELECT 
          fixture_id
         ,fixture_date
         ,fixture_referee
         ,fixture_venue_id
         ,fixture_status_long
         ,fixture_status_short
         ,fixture_status_elapsed
         ,league_id
         ,league_season
         ,league_round
         ,team_home_id
         ,team_away_id
         ,team_home_winner
         ,team_away_winner
         ,team_goals_home
         ,team_goals_away
         ,ingestion_date
       from {{ ref('teams_fixtures') }}
)
select 
          tf.fixture_id::INT as fixture_id
         ,TO_CHAR(TO_TIMESTAMP_NTZ(tf.fixture_date::STRING), 'YYYY/MM/DD HH24:MI:SS')  AS fixture_date
         ,tf.fixture_referee::VARCHAR(100) as fixture_referee
         ,tf.fixture_venue_id::INT as fixture_venue_id
         ,tf.fixture_status_long::VARCHAR(25) as fixture_status_long
         ,tf.fixture_status_short::VARCHAR(5) as fixture_status_short
         ,tf.fixture_status_elapsed::INT as fixture_status_elapsed
         ,tf.league_id::INT as league_id
         ,tf.league_season::INT as league_season
         ,tf.league_round::VARCHAR(25) as league_round
         ,tf.team_home_id::INT as team_home_id
         ,tf.team_away_id::INT as team_away_id
         ,tf.team_home_winner::BOOLEAN as team_home_winner
         ,tf.team_away_winner::BOOLEAN as team_away_winner
         ,tf.team_goals_home::INT as team_goals_home
         ,tf.team_goals_away::INT as team_goals_away
         ,TO_TIMESTAMP(tf.ingestion_date::STRING,'YYYY/MM/DD HH24:MI:SS') as ingestion_date
from teams_fixtures tf 
