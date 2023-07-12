{{ config(materialized='table') }}

with source_teams_standings as (

    select RAW_FILE
      from RAW.TEAMS_STANDINGS

)
select
     r.value:league:id as league_id
     , r.value:league:country as league_country
     , r.value:league:flag as league_country_flag 
     , r.value:league:logo as league_logo
     , r.value:league:name as league_name
     , r.value:league:season as league_season
     , l.value:rank as rank
     , l.value:team:id as team_id 
     , l.value:team:name as team_name 
     , l.value:points as team_points 
     , l.value:goalsDiff as team_goals_diff 
     , l.value['group'] as serie
     , l.value:form as team_form 
     , l.value:status as status 
     , l.value:description as description
     , l.value:all:played as matches_played
     , l.value:all:win as matches_win 
     , l.value:all:lose as matches_loss 
     , l.value:all:draw as matches_draw
     , l.value:all:goals:for as goals_for 
     , l.value:all:goals:against as goals_against 
     , l.value:home:played as matches_played_home
     , l.value:home:win as matches_win_home
     , l.value:home:lose as matches_loss_home
     , l.value:home:draw as matches_draw_home
     , l.value:home:goals:for as goals_for_home
     , l.value:home:goals:against as goals_against_home 
     , l.value:away:played as matches_played_away
     , l.value:away:win as matches_win_away
     , l.value:away:lose as matches_loss_away
     , l.value:away:draw as matches_draw_away
     , l.value:away:goals:for as goals_for_away
     , l.value:away:goals:against as goals_against_away 
     , l.value:update as file_last_update
     , sts.RAW_FILE:ingestion_date as ingestion_date
  from source_teams_standings sts
  ,lateral flatten(input => sts.RAW_FILE, path => 'response') r
  ,lateral flatten(input => r.value, path => 'league:standings[0]') l