{{ config(materialized='table') }}

with staging_teams_fixtures_stat as (
    select team_id
         , team_name
         , fixture_id
         , statistic_type
         , statistic_value
         , ingestion_date
      from {{ ref('teams_fixtures_statistics') }}
)
SELECT
 team_id,
 team_name,
  fixture_id,
  MAX(CASE WHEN statistic_type = 'Shots on Goal' THEN statistic_value ELSE NULL END) AS shots_on_goal,
  MAX(CASE WHEN statistic_type = 'Shots off Goal' THEN statistic_value ELSE NULL END) AS shots_off_goal,
  MAX(CASE WHEN statistic_type = 'Total Shots' THEN statistic_value ELSE NULL END) AS total_shots,
  MAX(CASE WHEN statistic_type = 'Blocked Shots' THEN statistic_value ELSE NULL END) AS blocked_shots,
  MAX(CASE WHEN statistic_type = 'Shots insidebox' THEN statistic_value ELSE NULL END) AS shots_insidebox,
  MAX(CASE WHEN statistic_type = 'Shots outsidebox' THEN statistic_value ELSE NULL END) AS shots_outsidebox,
  MAX(CASE WHEN statistic_type = 'Fouls' THEN statistic_value ELSE NULL END) AS fouls,
  MAX(CASE WHEN statistic_type = 'Corner Kicks' THEN statistic_value ELSE NULL END) AS corner_kicks,
  MAX(CASE WHEN statistic_type = 'Offsides' THEN statistic_value ELSE NULL END) AS offsides,
  MAX(CASE WHEN statistic_type = 'Ball Possession' THEN statistic_value ELSE NULL END) AS ball_possession,
  MAX(CASE WHEN statistic_type = 'Yellow Cards' THEN statistic_value ELSE NULL END) AS yellow_cards,
  MAX(CASE WHEN statistic_type = 'Red Cards' THEN statistic_value ELSE NULL END) AS red_cards,
  MAX(CASE WHEN statistic_type = 'Goalkeeper Saves' THEN statistic_value ELSE NULL END) AS goalkeeper_saves,
  MAX(CASE WHEN statistic_type = 'Total passes' THEN statistic_value ELSE NULL END) AS total_passes,
  MAX(CASE WHEN statistic_type = 'Passes accurate' THEN statistic_value ELSE NULL END) AS passes_accurate,
  MAX(CASE WHEN statistic_type = 'Passes %' THEN statistic_value ELSE NULL END) AS passes_percentage
FROM staging_teams_fixtures_stat
GROUP BY ALL 