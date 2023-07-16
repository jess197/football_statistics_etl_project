{{ config(materialized='view') }}

WITH curated_fixtures AS (
    SELECT team_home_id,
           team_away_id,
           team_home_winner,
           team_away_winner,
           fixture_id
    FROM {{ ref('fixtures') }}
),
brazil_teams AS (
  SELECT *
  FROM {{ ref('brazillian_teams') }}
), head_to_head as (
  SELECT
    bt.team_id as team_id1,
    bt.team_name as team_name1,
    bt2.team_id as team_id2,
    bt2.team_name as team_name2,
    cf.team_away_id, 
    cf.team_home_id,
    SUM(CASE WHEN cf.team_home_winner = True THEN 1 ELSE 0 END) head_to_head_home_wins,
    SUM(CASE WHEN cf.team_away_winner = True THEN 1 ELSE 0 END) head_to_head_away_wins,
    SUM(CASE WHEN cf.team_home_winner IS NULL AND cf.team_away_winner IS NULL THEN 1 ELSE 0 END) head_to_head_draws,
    COUNT(cf.fixture_id) as total_games
  FROM curated_fixtures cf
  JOIN brazil_teams bt ON cf.team_home_id = bt.team_id
  JOIN brazil_teams bt2 ON cf.team_away_id = bt2.team_id
  GROUP BY ALL

  UNION

  SELECT
    bt.team_id as team_id1 ,
    bt.team_name as team_name1,
    bt2.team_id as team_id2 ,
    bt2.team_name as team_name2,
    cf.team_away_id, 
    cf.team_home_id,
    SUM(CASE WHEN cf.team_home_winner = True THEN 1 ELSE 0 END) head_to_head_home_wins,
    SUM(CASE WHEN cf.team_away_winner = True THEN 1 ELSE 0 END) head_to_head_away_wins,
    SUM(CASE WHEN cf.team_home_winner IS NULL AND cf.team_away_winner IS NULL THEN 1 ELSE 0 END) head_to_head_draws,  
    COUNT(cf.fixture_id) as total_games
  FROM curated_fixtures cf
  JOIN brazil_teams bt ON cf.team_home_id = bt.team_id
  JOIN brazil_teams bt2 ON cf.team_away_id = bt2.team_id
  GROUP BY ALL
)
    SELECT hh.team_id1
        , hh.team_name1
        , hh.team_id2
        , hh.team_name2
        , hh.head_to_head_home_wins
        , hh.head_to_head_away_wins
        , hh.head_to_head_draws
        , hh.team_away_id
        ,hh.team_home_id
        , SUM(hh.total_games) as total_games
    FROM head_to_head hh
    GROUP BY ALL 

