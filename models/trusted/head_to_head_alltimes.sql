WITH head_to_head_alltimes AS (
    SELECT bt.team_id as team_id1
        , bt.team_name as team_name1
        , bt2.team_id as team_id2
        , bt2.team_name as team_name2 
        , hha.head_to_head_home_wins
        , hha.head_to_head_away_wins
        , hha.head_to_head_draws
        , hha.total_games
        , hha.team_away_id
        , hha.team_home_id
    FROM {{ ref('head_to_head_analysis') }} hha
    JOIN {{ ref('brazillian_teams') }} bt
     on hha.team_id1 = bt.team_id
    JOIN {{ ref('brazillian_teams') }} bt2
     on hha.team_id2 = bt2.team_id
    UNION 
    SELECT bt2.team_id as team_id1 
        , bt2.team_name as team_name1
        , bt.team_id as team_id2
        , bt.team_name as team_name2 
        , hha.head_to_head_home_wins
        , hha.head_to_head_away_wins
        , hha.head_to_head_draws
        , hha.total_games
        , hha.team_away_id
        , hha.team_home_id
    FROM {{ ref('head_to_head_analysis') }} hha
    JOIN {{ ref('brazillian_teams') }} bt
     on hha.team_id1 = bt.team_id
    JOIN {{ ref('brazillian_teams') }} bt2
     on hha.team_id2 = bt2.team_id
)
SELECT a.team_id1
     , a.team_name1 
     , a.team_id2
     , a.team_name2 
     , SUM(CASE WHEN team_away_id = team_id1 THEN head_to_head_AWAY_wins
                WHEN team_home_id = team_id1 THEN head_to_head_home_wins  END
        ) as total_wins_team_1
     , SUM(CASE WHEN team_away_id = team_id2 THEN head_to_head_AWAY_wins 
                WHEN team_home_id = team_id2 THEN head_to_head_home_wins END
     ) as total_wins_team_2 
     , SUM(head_to_head_draws) as total_draws 
     , SUM(total_games) AS total_games 
  FROM head_to_head_alltimes a 
group by all 