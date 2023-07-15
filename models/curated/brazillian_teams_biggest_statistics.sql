{{ config(materialized='table') }}

with staging_season_biggest_statistics as (
    select team_id
         , league_id
         , league_season
         , biggest_goals_against_away
         , biggest_goals_against_home
         , biggest_goals_for_away
         , biggest_goals_for_home
         , biggest_loses_away
         , biggest_loses_home
         , biggest_wins_away
         , biggest_wins_home
         , biggest_streak_wins
         , biggest_streak_loses
         , biggest_streak_draws
         , ingestion_date
      from {{ ref('teams_statistics') }}
)

SELECT {{ dbt_utils.generate_surrogate_key(['sbs.team_id','sbs.ingestion_date']) }} as season_biggest_stat_id
     , sbs.team_id::int as team_id 
     , league_id::int as league_id
     , sbs.league_season::VARCHAR(20) as league_season  
     , sbs.biggest_goals_against_away::int as biggest_goals_against_away
     , sbs.biggest_goals_against_home::int as biggest_goals_against_home
     , sbs.biggest_goals_for_away::int as biggest_goals_for_away
     , sbs.biggest_goals_for_home::int as biggest_goals_for_home
     , sbs.biggest_loses_away::VARCHAR(5) as biggest_loses_away
     , sbs.biggest_loses_home::VARCHAR(5) as biggest_loses_home
     , sbs.biggest_wins_away::VARCHAR(5) as biggest_wins_away
     , sbs.biggest_wins_home::VARCHAR(5) as biggest_wins_home
     , sbs.biggest_streak_wins::int as biggest_streak_wins
     , sbs.biggest_streak_loses::int as biggest_streak_loses
     , sbs.biggest_streak_draws::int as biggest_streak_draws
     , TO_TIMESTAMP(sbs.ingestion_date::STRING,'YYYY/MM/DD HH24:MI:SS') as ingestion_date
  FROM staging_season_biggest_statistics sbs