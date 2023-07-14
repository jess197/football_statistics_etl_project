  {{ config(materialized='table') }}

with staging_standings as (
    SELECT league_id
         , league_season
         , rank
         , team_id
         , team_points
         , team_goals_diff
         , serie
         , team_form
         , status
         , description
         , matches_played
         , matches_win
         , matches_loss
         , matches_draw
         , goals_for
         , goals_against
         , matches_played_home
         , matches_win_home
         , matches_loss_home
         , matches_draw_home
         , goals_for_home
         , goals_against_home
         , matches_played_away
         , matches_win_away
         , matches_loss_away
         , matches_draw_away
         , goals_for_away
         , goals_against_away
         , file_last_update
         , ingestion_date
      FROM {{ ref('teams_standings') }}
)

SELECT st.league_id::int as league_id
     , st.league_season::int as season
     , st.rank::int as rank
     , st.team_id::int as team_id 
     , st.team_points::int as team_points
     , st.team_goals_diff::int as team_goals_diff
     , st.serie::VARCHAR(10) as serie
     , st.team_form::VARCHAR(10) as last_results
     , st.status::VARCHAR(10) as status
     , st.description::VARCHAR(50) as description
     , st.matches_played::int as matches_played
     , st.matches_win::int as matches_win
     , st.matches_loss::int as matches_loss
     , st.matches_draw::int as matches_draw
     , st.goals_for::int as goals_for
     , st.goals_against::int as goals_against 
     , st.matches_played_home::int as matches_played_home
     , st.matches_win_home::int as matches_win_home
     , st.matches_loss_home::int as matches_loss_home
     , st.matches_draw_home::int as matches_draw_home
     , st.goals_for_home::int as goals_for_home
     , st.goals_against_home::int as goals_against_home
     , st.matches_played_away::int as matches_played_away
     , st.matches_win_away::int as matches_win_away
     , st.matches_loss_away::int as matches_loss_away
     , st.matches_draw_away::int as matches_draw_away
     , st.goals_for_away::int as goals_for_away
     , st.goals_against_away::int as goals_against_away
     , TO_TIMESTAMP_NTZ(st.file_last_update::STRING) as file_last_update
     , TO_TIMESTAMP(st.ingestion_date::STRING,'YYYY/MM/DD HH24:MI:SS') as ingestion_date
  FROM staging_standings st
  WHERE season >= 2019