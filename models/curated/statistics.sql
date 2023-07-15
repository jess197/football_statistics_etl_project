{{ config(materialized='table') }}

with staging_statistics as (
    SELECT league_id
         , league_season
         , team_id
         , statistic_form
         , clean_sheet_home
         , clean_sheet_away
         , clean_sheet_total
         , failed_to_score_home
         , failed_to_score_away
         , failed_to_score_total
         , penalty_scored_total
         , penalty_scored_percentage
         , penalty_missed_total
         , penalty_missed_percentage
         , penalty_total
         , top_1_lineup
         , times_played_lineup_1
         , top_2_lineup
         , times_played_lineup_2
         , top_3_lineup
         , times_played_lineup_3
         , ingestion_date
      FROM {{ ref('teams_statistics') }}
)
   SELECT {{ dbt_utils.generate_surrogate_key(['sst.team_id','sst.ingestion_date']) }} as statistics_id 
        , sst.league_id::INT as league_id
        , sst.league_season::INT as league_season
        , sst.team_id::INT team_id
        , sst.statistic_form::VARCHAR(50) as last_matches_results
        , sst.clean_sheet_home::int as clean_sheet_home
        , sst.clean_sheet_away::int as clean_sheet_away
        , sst.clean_sheet_total::int as clean_sheet_total
        , sst.failed_to_score_home::int as failed_to_score_home
        , sst.failed_to_score_away::int as failed_to_score_away
        , sst.failed_to_score_total::int as failed_to_score_total
        , sst.penalty_scored_total::int as penalty_scored_total
        , sst.penalty_scored_percentage::VARCHAR(20) as penalty_scored_percentage
        , sst.penalty_missed_total::INT as penalty_missed_total
        , sst.penalty_missed_percentage::VARCHAR(20) as penalty_missed_percentage
        , sst.penalty_total::INT as penalty_total
        , sst.top_1_lineup::VARCHAR(10) as top_1_lineup
        , sst.times_played_lineup_1::int as times_played_lineup_1
        , sst.top_2_lineup::VARCHAR(10) as top_2_lineup
        , sst.times_played_lineup_2::int as times_played_lineup_2
        , sst.top_3_lineup::VARCHAR(10) as top_3_lineup
        , sst.times_played_lineup_3::int as times_played_lineup_3
        , TO_TIMESTAMP(sst.ingestion_date::STRING,'YYYY/MM/DD HH24:MI:SS') as ingestion_date
     FROM staging_statistics sst