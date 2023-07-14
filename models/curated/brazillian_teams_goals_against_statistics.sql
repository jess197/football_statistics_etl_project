{{ config(materialized='table') }} 

with statistics_goals_against as (
    select team_id
         , league_id
         , league_season
         , goals_against_total_home
         , goals_against_total_away
         , goals_against_total
         , goals_against_average_home
         , goals_against_average_away
         , goals_against_average_total
         , goals_against_fifteen_min
         , goals_against_thirty_min
         , goals_against_forty_five_min
         , goals_against_sixty_min
         , goals_against_seventy_five_min
         , goals_against_ninety_min
         , goals_against_hundred_five_min
         , goals_against_hundred_twenty_min
         , goals_against_fifteen_min_percentage
         , goals_against_thirty_min_percentage
         , goals_against_forty_five_min_percentage
         , goals_against_sixty_min_percentage
         , goals_against_seventy_five_min_percentage
         , goals_against_ninety_min_percentage
         , goals_against_hundred_five_min_percentage
         , goals_against_hundred_twenty_min_percentage
         , ingestion_date
      from {{ ref('teams_statistics') }}
)
    SELECT {{ dbt_utils.generate_surrogate_key(['sga.team_id','sga.ingestion_date']) }} as statistics_goals_against_id 
         , sga.team_id::int as team_id
         , sga.league_id::int as league_id
         , sga.league_season::VARCHAR(20) as league_season
         , sga.goals_against_total_home::int as goals_against_total_home
         , sga.goals_against_total_away::int as goals_against_total_away
         , sga.goals_against_total::int as goals_against_total
         , sga.goals_against_average_home::decimal(2,1) as goals_against_average_home
         , sga.goals_against_average_away::decimal(2,1) as goals_against_average_away
         , sga.goals_against_average_total::decimal(2,1) as goals_against_average
         , sga.goals_against_fifteen_min::int as goals_against_fifteen_min 
         , sga.goals_against_thirty_min::int as goals_against_thirty_min
         , sga.goals_against_forty_five_min::int as goals_against_forty_five_min
         , sga.goals_against_sixty_min::int as goals_against_sixty_min
         , sga.goals_against_seventy_five_min::int as goals_against_seventy_five_min
         , sga.goals_against_ninety_min::int as goals_against_ninety_min
         , sga.goals_against_hundred_five_min::int as goals_against_hundred_five_min
         , sga.goals_against_hundred_twenty_min::int as goals_against_hundred_twenty_min
         , sga.goals_against_fifteen_min_percentage::VARCHAR(10) as goals_against_fifteen_min_percentage
         , sga.goals_against_thirty_min_percentage::VARCHAR(10) as goals_against_thirty_min_percentage
         , sga.goals_against_forty_five_min_percentage::VARCHAR(10) as goals_against_forty_five_min_percentage
         , sga.goals_against_sixty_min_percentage::VARCHAR(10) as goals_against_sixty_min_percentage
         , sga.goals_against_seventy_five_min_percentage::VARCHAR(10) as goals_against_seventy_five_min_percentage
         , sga.goals_against_ninety_min_percentage::VARCHAR(10) as goals_against_ninety_min_percentage
         , sga.goals_against_hundred_five_min_percentage::VARCHAR(10) as goals_against_hundred_five_min_percentage
         , sga.goals_against_hundred_twenty_min_percentage::VARCHAR(10) as goals_against_hundred_twenty_min_percentage
         , TO_TIMESTAMP(sga.ingestion_date::STRING,'YYYY/MM/DD HH24:MI:SS') as ingestion_date
      FROM statistics_goals_against sga