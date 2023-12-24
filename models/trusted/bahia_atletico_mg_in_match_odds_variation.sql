select DISTINCT
        imo.match_time_minutes,
        imo.match_time,
        imo.odd,
        CASE WHEN imo.team_to_win = 'Home' then bth.team_name
             WHEN imo.team_to_win = 'Away' then bta.team_name
             ELSE imo.team_to_win END as team_to_win
  from football_curated.in_match_odds imo
  join football_curated.fixtures f
  on   imo.fixture_id = f.fixture_id
  join football_curated.brazillian_teams bth
  on   f.team_home_id = bth.team_id
  join football_curated.brazillian_teams bta
  on   f.team_away_id = bta.team_id
  where f.fixture_id = 1006027 
  and match_time_minutes < 90
  order by match_time_minutes asc