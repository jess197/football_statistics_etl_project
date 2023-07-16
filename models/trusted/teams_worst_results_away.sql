{{ config(materialized='view') }}

WITH curated_fixtures_alltime_statistics AS (
  SELECT *
  FROM {{ ref('brazillian_teams_all_fixtures_statistics') }}
),
br_teams AS (
  SELECT *
  FROM {{ ref('brazillian_teams') }}
),
ranked_teams AS (
  SELECT team_name
       , league_season
       , SUM(fs.fixtures_played_total) AS fixt_played_season
       , SUM(fs.fixtures_played_away) AS fixt_played_away_season
       , SUM(fs.fixtures_loses_away) AS fixt_loses_away_season
       , ROW_NUMBER() OVER (PARTITION BY league_season ORDER BY SUM(fs.fixtures_loses_away) DESC) AS row_num
  FROM curated_fixtures_alltime_statistics fs
  JOIN br_teams bt ON fs.team_id = bt.team_id
  GROUP BY team_name, league_season
)
SELECT team_name, league_season, fixt_played_season, fixt_played_away_season, fixt_loses_away_season
FROM ranked_teams
WHERE row_num <= 3
ORDER BY league_season, fixt_loses_away_season DESC