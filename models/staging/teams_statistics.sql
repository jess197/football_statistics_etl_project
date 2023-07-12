{{ config(materialized='table') }}

with source_teams_statistics as (

    select RAW_FILE
      from RAW.TEAMS_STATISTICS

)
select 
      $1:league:id as league_id 
     ,$1:league:name as league_name
     ,$1:league:season as league_season
     ,$1:league:logo as league_logo
     ,$1:league:flag as league_country_flag
     ,$1:league:country as league_country
     ,$1:team:id as team_id 
     ,$1:team:name as team_name 
     ,$1:form as statistic_form 
     ,$1:fixtures:played:home as fixtures_played_home 
     ,$1:fixtures:played:away as fixtures_played_away
     ,$1:fixtures:played:total as fixtures_played_total
     ,$1:fixtures:wins:home as fixtures_wins_home 
     ,$1:fixtures:wins:away as fixtures_wins_away
     ,$1:fixtures:wins:total as fixtures_wins_total
     ,$1:fixtures:draws:home as fixtures_draws_home 
     ,$1:fixtures:draws:away as fixtures_draws_away
     ,$1:fixtures:draws:total as fixtures_draws_total
     ,$1:fixtures:loses:home as fixtures_loses_home 
     ,$1:fixtures:loses:away as fixtures_loses_away
     ,$1:fixtures:loses:total as fixtures_loses_total
     ,$1:biggest:goals:against:away as biggest_goals_against_away
     ,$1:biggest:goals:against:home as biggest_goals_against_home
     ,$1:biggest:goals:for:away as biggest_goals_for_away
     ,$1:biggest:goals:for:home as biggest_goals_for_home
     ,$1:biggest:loses:away as biggest_loses_away
     ,$1:biggest:loses:home as biggest_loses_home
     ,$1:biggest:wins:away as biggest_wins_away
     ,$1:biggest:wins:home as biggest_wins_home
     ,$1:biggest:streak:wins as biggest_streak_wins
     ,$1:biggest:streak:loses as biggest_streak_loses
     ,$1:biggest:streak:draws as biggest_streak_draws
     ,$1:goals:for:total:home as goals_for_total_home 
     ,$1:goals:for:total:home as goals_for_total_away 
     ,$1:goals:for:total:home as goals_for_total_total
     ,$1:goals:for:average:home as goals_for_average_home 
     ,$1:goals:for:average:home as goals_for_average_away 
     ,$1:goals:for:average:home as goals_for_average_total
     ,$1:goals:for:minute['0-15']:total as goals_for_fifteen_min
     ,$1:goals:for:minute['16-30']:total as goals_for_thirty_min 
     ,$1:goals:for:minute['31-45']:total as goals_for_forty_five_min
     ,$1:goals:for:minute['46-60']:total as goals_for_sixty_min
     ,$1:goals:for:minute['61-75']:total as goals_for_seventy_five_min
     ,$1:goals:for:minute['76-90']:total as goals_for_ninety_min
     ,$1:goals:for:minute['91-105']:total as goals_for_hundred_five_min
     ,$1:goals:for:minute['106-120']:total as goals_for_hundred_twenty_min
     ,$1:goals:for:minute['0-15']:percentage as goals_for_fifteen_min_percentage 
     ,$1:goals:for:minute['16-30']:percentage as goals_for_thirty_min_percentage 
     ,$1:goals:for:minute['31-45']:percentage as goals_for_forty_five_min_percentage 
     ,$1:goals:for:minute['46-60']:percentage as goals_for_sixty_min_percentage 
     ,$1:goals:for:minute['61-75']:percentage as goals_for_seventy_five_min_percentage 
     ,$1:goals:for:minute['76-90']:percentage as goals_for_ninety_min_percentage 
     ,$1:goals:for:minute['91-105']:percentage as goals_for_hundred_five_min_percentage 
     ,$1:goals:for:minute['106-120']:percentage as goals_for_hundred_twenty_min_percentage 
     ,$1:goals:against:total:home as goals_against_total_home 
     ,$1:goals:against:total:away as goals_against_total_away 
     ,$1:goals:against:total:total as goals_against_total
     ,$1:goals:against:average:home as goals_against_average_home 
     ,$1:goals:against:average:away as goals_against_average_away 
     ,$1:goals:against:average:total as goals_against_average_total
     ,$1:goals:against:minute['0-15']:total as goals_against_fifteen_min
     ,$1:goals:against:minute['16-30']:total as goals_against_thirty_min 
     ,$1:goals:against:minute['31-45']:total as goals_against_forty_five_min
     ,$1:goals:against:minute['46-60']:total as goals_against_sixty_min
     ,$1:goals:against:minute['61-75']:total as goals_against_seventy_five_min
     ,$1:goals:against:minute['76-90']:total as goals_against_ninety_min
     ,$1:goals:against:minute['91-105']:total as goals_against_hundred_five_min
     ,$1:goals:against:minute['106-120']:total as goals_against_hundred_twenty_min
     ,$1:goals:against:minute['0-15']:percentage as goals_against_fifteen_min_percentage 
     ,$1:goals:against:minute['16-30']:percentage as goals_against_thirty_min_percentage 
     ,$1:goals:against:minute['31-45']:percentage as goals_against_forty_five_min_percentage 
     ,$1:goals:against:minute['46-60']:percentage as goals_against_sixty_min_percentage 
     ,$1:goals:against:minute['61-75']:percentage as goals_against_seventy_five_min_percentage 
     ,$1:goals:against:minute['76-90']:percentage as goals_against_ninety_min_percentage 
     ,$1:goals:against:minute['91-105']:percentage as goals_against_hundred_five_min_percentage 
     ,$1:goals:against:minute['106-120']:percentage as goals_against_hundred_twenty_min_percentage 
     ,$1:clean_sheet:home as clean_sheet_home
     ,$1:clean_sheet:away as clean_sheet_away
     ,$1:clean_sheet:total as clean_sheet_total
     ,$1:failed_to_score:home as failed_to_score_home
     ,$1:failed_to_score:away as failed_to_score_away
     ,$1:failed_to_score:total as failed_to_score_total
     ,$1:penalty:scored:total as penalty_scored_total
     ,$1:penalty:scored:percentage as penalty_scored_percentage
     ,$1:penalty:missed:total as penalty_missed_total
     ,$1:penalty:missed:percentage as penalty_missed_percentage
     ,$1:penalty:total as penalty_total
     ,$1:cards:yellow['0-15']:total as yellow_cards_fifteen_min 
     ,$1:cards:yellow['16-30']:total as yellow_cards_thirty_min 
     ,$1:cards:yellow['31-45']:total as yellow_cards_forty_five_min 
     ,$1:cards:yellow['46-60']:total as yellow_cards_sixty_min 
     ,$1:cards:yellow['61-75']:total as yellow_cards_seventy_five_min 
     ,$1:cards:yellow['76-90']:total as yellow_cards_ninety_min 
     ,$1:cards:yellow['91-105']:total as yellow_cards_hundred_five_min 
     ,$1:cards:yellow['106-120']:total as yellow_cards_hundred_twenty_min 
     ,$1:cards:yellow['0-15']:percentage as yellow_cards_fifteen_min_percentage 
     ,$1:cards:yellow['16-30']:percentage as yellow_cards_thirty_min_percentage 
     ,$1:cards:yellow['31-45']:percentage as yellow_cards_forty_five_min_percentage 
     ,$1:cards:yellow['46-60']:percentage as yellow_cards_sixty_min_percentage 
     ,$1:cards:yellow['61-75']:percentage as yellow_cards_seventy_five_min_percentage 
     ,$1:cards:yellow['76-90']:percentage as yellow_cards_ninety_min_percentage 
     ,$1:cards:yellow['91-105']:percentage as yellow_cards_hundred_five_min_percentage 
     ,$1:cards:yellow['106-120']:percentage as yellow_cards_hundred_twenty_min_percentage 
     ,$1:lineups[0]:formation as top_1_lineup
     ,$1:lineups[0]:played as times_played_lineup_1
     ,$1:lineups[1]:formation as top_2_lineup
     ,$1:lineups[1]:played as times_played_lineup_2
     ,$1:lineups[2]:formation as top_3_lineup
     ,$1:lineups[2]:played as times_played_lineup_3
     ,$1:ingestion_date as ingestion_date
from source_teams_statistics stb
