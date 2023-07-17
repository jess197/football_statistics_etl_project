{{ config(materialized='view') }}
WITH period_w_most_yellow_cards AS (
    SELECT
        team_id,
        league_season,
        CASE
            WHEN yellow_cards_fifteen_min >= yellow_cards_thirty_min AND yellow_cards_fifteen_min >= yellow_cards_forty_five_min 
             AND yellow_cards_fifteen_min >= yellow_cards_sixty_min AND yellow_cards_fifteen_min >= yellow_cards_seventy_five_min 
             AND yellow_cards_fifteen_min >= yellow_cards_ninety_min AND yellow_cards_fifteen_min >= NVL(yellow_cards_hundred_five_min,0) THEN '0min - 15min'
            WHEN yellow_cards_thirty_min >= yellow_cards_fifteen_min AND yellow_cards_thirty_min >= yellow_cards_forty_five_min 
             AND yellow_cards_thirty_min >= yellow_cards_sixty_min AND yellow_cards_thirty_min >= yellow_cards_seventy_five_min 
             AND yellow_cards_thirty_min >= yellow_cards_ninety_min AND yellow_cards_thirty_min >= NVL(yellow_cards_hundred_five_min,0) THEN '16min - 30min'
            WHEN yellow_cards_forty_five_min >= yellow_cards_fifteen_min AND yellow_cards_forty_five_min >= yellow_cards_thirty_min 
             AND yellow_cards_forty_five_min >= yellow_cards_sixty_min AND yellow_cards_forty_five_min >= yellow_cards_seventy_five_min 
             AND yellow_cards_forty_five_min >= yellow_cards_ninety_min AND yellow_cards_forty_five_min >= NVL(yellow_cards_hundred_five_min,0) THEN '31min - 45min'
            WHEN yellow_cards_sixty_min >= yellow_cards_fifteen_min AND yellow_cards_sixty_min >= yellow_cards_thirty_min 
             AND yellow_cards_sixty_min >= yellow_cards_forty_five_min AND yellow_cards_sixty_min >= yellow_cards_seventy_five_min 
             AND yellow_cards_sixty_min >= yellow_cards_ninety_min AND yellow_cards_sixty_min >= NVL(yellow_cards_hundred_five_min,0) THEN '46min - 60min'
            WHEN yellow_cards_seventy_five_min >= yellow_cards_fifteen_min AND yellow_cards_seventy_five_min >= yellow_cards_thirty_min 
             AND yellow_cards_seventy_five_min >= yellow_cards_forty_five_min AND yellow_cards_seventy_five_min >= yellow_cards_sixty_min 
             AND yellow_cards_seventy_five_min >= yellow_cards_ninety_min AND yellow_cards_seventy_five_min >= NVL(yellow_cards_hundred_five_min,0) THEN '61min - 75min'
            WHEN yellow_cards_ninety_min >= yellow_cards_fifteen_min AND yellow_cards_ninety_min >= yellow_cards_thirty_min 
             AND yellow_cards_ninety_min >= yellow_cards_forty_five_min AND yellow_cards_ninety_min >= yellow_cards_sixty_min 
             AND yellow_cards_ninety_min >= yellow_cards_seventy_five_min AND yellow_cards_ninety_min >= NVL(NVL(yellow_cards_hundred_five_min,0),0) THEN '76min - 90min'
            WHEN NVL(yellow_cards_hundred_five_min,0) >= yellow_cards_fifteen_min AND NVL(yellow_cards_hundred_five_min,0) >= yellow_cards_thirty_min 
             AND NVL(yellow_cards_hundred_five_min,0) >= yellow_cards_forty_five_min AND NVL(yellow_cards_hundred_five_min,0) >= yellow_cards_sixty_min 
             AND NVL(yellow_cards_hundred_five_min,0) >= yellow_cards_seventy_five_min AND NVL(yellow_cards_hundred_five_min,0) >= yellow_cards_ninety_min THEN '91min - 105min'
            ELSE 'Unknown'
        END AS gratest_time_w_yellow
    FROM {{ ref('brazillian_teams_yellow_cards_statistics') }}
)
SELECT
    pyc.team_id,
    pyc.league_season,
    pyc.gratest_time_w_yellow,
    CASE
        WHEN pyc.gratest_time_w_yellow = '0min - 15min' THEN byc.yellow_cards_fifteen_min_percentage
        WHEN pyc.gratest_time_w_yellow = '16min - 30min' THEN byc.yellow_cards_thirty_min_percentage
        WHEN pyc.gratest_time_w_yellow = '31min - 45min' THEN byc.yellow_cards_forty_five_min_percentage
        WHEN pyc.gratest_time_w_yellow = '46min - 60min' THEN byc.yellow_cards_sixty_min_percentage
        WHEN pyc.gratest_time_w_yellow = '61min - 75min' THEN byc.yellow_cards_seventy_five_min_percentage
        WHEN pyc.gratest_time_w_yellow = '76min - 90min' THEN byc.yellow_cards_ninety_min_percentage
        WHEN pyc.gratest_time_w_yellow = '91min - 105min' THEN byc.yellow_cards_hundred_five_min_percentage
    END AS percentage_yellow_cards
FROM period_w_most_yellow_cards pyc
JOIN {{ ref('brazillian_teams_yellow_cards_statistics') }} byc ON pyc.team_id = byc.team_id AND pyc.league_season = byc.league_season
