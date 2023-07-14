{{ config(materialized='table') }}

with staging_season_statistics_fixtures as (
    select team_id
         , league_id
         , league_season
         , fixtures_played_home
         , fixtures_played_away
         , fixtures_played_total
         , fixtures_wins_home
         , fixtures_wins_away
         , fixtures_wins_total
         , fixtures_draws_home
         , fixtures_draws_away
         , fixtures_draws_total
         , fixtures_loses_home
         , fixtures_loses_away
         , fixtures_loses_total
         , ingestion_date
      from {{ ref('teams_statistics') }}
)

SELECT ssf.team_id::int as team_id 
     , league_id::int as league_id
     , ssf.league_season::VARCHAR(20) as league_season
     , {{ dbt_utils.generate_surrogate_key(['ssf.team_id','ssf.ingestion_date']) }} as season_statistics_fixt_id  
     , ssf.fixtures_played_home::int as fixtures_played_home
     , ssf.fixtures_played_away::int as fixtures_played_away
     , ssf.fixtures_played_total::int as fixtures_played_total
     , ssf.fixtures_wins_home::int as fixtures_wins_home
     , ssf.fixtures_wins_away::int as fixtures_wins_away
     , ssf.fixtures_wins_total::int as fixtures_wins_total
     , ssf.fixtures_draws_home::int as fixtures_draws_home
     , ssf.fixtures_draws_away::int as fixtures_draws_away
     , ssf.fixtures_draws_total::int as fixtures_draws_total
     , ssf.fixtures_loses_home::int as fixtures_loses_home
     , ssf.fixtures_loses_away::int as fixtures_loses_away
     , ssf.fixtures_loses_total::int as fixtures_loses_total
     , TO_TIMESTAMP(ssf.ingestion_date::STRING,'YYYY/MM/DD HH24:MI:SS') as ingestion_date
  FROM staging_season_statistics_fixtures ssf