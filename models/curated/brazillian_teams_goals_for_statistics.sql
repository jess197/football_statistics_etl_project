{{ config(materialized='table') }} 


with statistics_goals_for as (
    select team_id
         , league_id
         , league_season
         , goals_for_total_home
         , goals_for_total_away
         , goals_for_total_total
         , goals_for_average_home
         , goals_for_average_away
         , goals_for_average_total
         , goals_for_fifteen_min
         , goals_for_thirty_min
         , goals_for_forty_five_min
         , goals_for_sixty_min
         , goals_for_seventy_five_min
         , goals_for_ninety_min
         , goals_for_hundred_five_min
         , goals_for_hundred_twenty_min
         , goals_for_fifteen_min_percentage
         , goals_for_thirty_min_percentage
         , goals_for_forty_five_min_percentage
         , goals_for_sixty_min_percentage
         , goals_for_seventy_five_min_percentage
         , goals_for_ninety_min_percentage
         , goals_for_hundred_five_min_percentage
         , goals_for_hundred_twenty_min_percentage
         , ingestion_date
      from {{ ref('teams_statistics') }}
)
   SELECT {{ dbt_utils.generate_surrogate_key(['sgf.team_id','sgf.ingestion_date']) }} as statistics_goals_for_id 
        , sgf.team_id::int as team_id
        , sgf.league_id::int as league_id
        , sgf.league_season::VARCHAR(20) as league_season
        , sgf.goals_for_total_home::int as goals_for_total_home
        , sgf.goals_for_total_away::int as goals_for_total_away
        , sgf.goals_for_total_total::int as goals_for_total
        , sgf.goals_for_average_home::decimal(2,1) as goals_for_average_home
        , sgf.goals_for_average_away::decimal(2,1) as goals_for_average_away
        , sgf.goals_for_average_total::decimal(2,1) as goals_for_average
        , sgf.goals_for_fifteen_min::int as goals_for_fifteen_min
        , sgf.goals_for_thirty_min::int as goals_for_thirty_min
        , sgf.goals_for_forty_five_min::int as goals_for_forty_five_min
        , sgf.goals_for_sixty_min::int as goals_for_sixty_min
        , sgf.goals_for_seventy_five_min::int as goals_for_seventy_five_min
        , sgf.goals_for_ninety_min::int as goals_for_ninety_min
        , sgf.goals_for_hundred_five_min::int as goals_for_hundred_five_min
        , sgf.goals_for_hundred_twenty_min::int as goals_for_hundred_twenty_min
        , sgf.goals_for_fifteen_min_percentage::VARCHAR(10) as goals_for_fifteen_min_percentage
        , sgf.goals_for_thirty_min_percentage::VARCHAR(10) as goals_for_thirty_min_percentage
        , sgf.goals_for_forty_five_min_percentage::VARCHAR(10) as goals_for_forty_five_min_percentage
        , sgf.goals_for_sixty_min_percentage::VARCHAR(10) as goals_for_sixty_min_percentage
        , sgf.goals_for_seventy_five_min_percentage::VARCHAR(10) as goals_for_seventy_five_min_percentage
        , sgf.goals_for_ninety_min_percentage::VARCHAR(10) as goals_for_ninety_min_percentage
        , sgf.goals_for_hundred_five_min_percentage::VARCHAR(10) as goals_for_hundred_five_min_percentage
        , sgf.goals_for_hundred_twenty_min_percentage::VARCHAR(10) as goals_for_hundred_twenty_min_percentage
        , TO_TIMESTAMP(sgf.ingestion_date::STRING,'YYYY/MM/DD HH24:MI:SS') as ingestion_date
  FROM statistics_goals_for sgf
 