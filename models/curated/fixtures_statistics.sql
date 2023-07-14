{{ config(materialized='table') }}

with staging_teams_fixtures_stat as (
    select team_id
         , fixture_id
         , statistic_type
         , statistic_value
         , ingestion_date
      from {{ ref('teams_fixtures_statistics') }}
)
SELECT
       tfs.team_id::int as team_id
     , tfs.fixture_id::int as fixture_id
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Shots on Goal' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS shots_on_goal
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Shots off Goal' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS shots_off_goal
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Total Shots' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS total_shots
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Blocked Shots' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS blocked_shots
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Shots insidebox' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS shots_insidebox
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Shots outsidebox' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS shots_outsidebox
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Fouls' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS fouls
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Corner Kicks' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS corner_kicks
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Offsides' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS offsides
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Ball Possession' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS ball_possession
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Yellow Cards' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS yellow_cards
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Red Cards' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS red_cards
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Goalkeeper Saves' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS goalkeeper_saves
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Total passes' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS total_passes
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Passes accurate' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS passes_accurate
     , MAX(CASE WHEN tfs.statistic_type::VARCHAR(20) = 'Passes %' THEN tfs.statistic_value::VARCHAR(20) ELSE NULL END) AS passes_percentage
     ,TO_TIMESTAMP(tfs.ingestion_date::STRING,'YYYY/MM/DD HH24:MI:SS') as ingestion_date
FROM staging_teams_fixtures_stat tfs
GROUP BY ALL 