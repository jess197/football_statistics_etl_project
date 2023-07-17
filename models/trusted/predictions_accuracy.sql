{{ config(materialized='view') }}

WITH predictions as (
    SELECT fixture_id
         , pred_winner_id
         , pred_comment
         , pred_winner_name
      FROM {{ ref('fixtures_predictions')}}
), fixtures as (
    SELECT fixture_id
         , fixture_date
         , league_season
         , team_away_id
         , team_home_id
         , CASE WHEN team_home_winner = 'True' THEN team_home_id 
                WHEN team_away_winner = 'True' THEN team_away_id 
            ELSE 0
            END as team_winner_id
      FROM {{ref('fixtures')}}
), brazil_teams as (
    SELECT * 
      FROM {{ref('brazillian_teams')}}
)

-- 1005672.0 - santos seria o ganhador e foi o ganhador - previsao certa 
-- 37282.0 - vitoria seria o ganhador contra o internacional - previsao errada, internacional ganhou 
-- 37294.0 - athletico pr seria o ganhador contra o fluminense, nao foi -- previsao errada -- fluminense ganhou 
-- 1005738.0 - disse que o corinthians seria ganhador contra o AMERICA? 125 e previsao errada - 125 AMERICA ganhou

SELECT f.league_season 
     , bt.team_name||' X '||bt2.team_name as match
     , p.pred_winner_name
     , p.pred_comment
     , CASE WHEN p.pred_winner_id = f.team_winner_id OR f.team_winner_id = bt.team_id THEN bt.team_name
            WHEN p.pred_winner_id = f.team_winner_id OR f.team_winner_id = bt2.team_id THEN bt2.team_name
        ELSE 'Draw'
       END as winner_team 
     , CASE WHEN p.pred_winner_id = f.team_winner_id OR (f.team_winner_id = 0 AND p.pred_comment LIKE '%draw%') THEN 'Correct Prediction'
            ELSE 'Wrong Prediction'
         END as prediction_accuracy
  FROM predictions p 
  JOIN fixtures f
    ON p.fixture_id = f.fixture_id 
  JOIN brazil_teams bt
    ON f.team_home_id = bt.team_id
  JOIN brazil_teams bt2
    ON f.team_away_id = bt2.team_id
    AND f.fixture_date <= to_CHAR(CURRENT_DATE,'YYYY/MM/DD')
  GROUP BY ALL 
