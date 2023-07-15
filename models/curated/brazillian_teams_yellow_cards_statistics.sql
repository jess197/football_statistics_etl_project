{{ config(materialized='table') }} 

with statistics_yellow_cards as (
    select team_id
         , league_id
         , league_season
         , yellow_cards_fifteen_min
         , yellow_cards_thirty_min
         , yellow_cards_forty_five_min
         , yellow_cards_sixty_min
         , yellow_cards_seventy_five_min
         , yellow_cards_ninety_min
         , yellow_cards_hundred_five_min
         , yellow_cards_hundred_twenty_min
         , yellow_cards_fifteen_min_percentage
         , yellow_cards_thirty_min_percentage
         , yellow_cards_forty_five_min_percentage
         , yellow_cards_sixty_min_percentage
         , yellow_cards_seventy_five_min_percentage
         , yellow_cards_ninety_min_percentage
         , yellow_cards_hundred_five_min_percentage
         , yellow_cards_hundred_twenty_min_percentage
         , ingestion_date
      from {{ ref('teams_statistics') }}
)         
    SELECT {{ dbt_utils.generate_surrogate_key(['syc.team_id','syc.ingestion_date']) }} as statistics_yellow_cards_id 
         , syc.team_id::int as team_id
         , syc.league_id::int as league_id
         , syc.league_season::VARCHAR(20) as league_season
         , syc.yellow_cards_fifteen_min::int as yellow_cards_fifteen_min
         , syc.yellow_cards_thirty_min::int as yellow_cards_thirty_min
         , syc.yellow_cards_forty_five_min::int as yellow_cards_forty_five_min
         , syc.yellow_cards_sixty_min::int as yellow_cards_sixty_min
         , syc.yellow_cards_seventy_five_min::int as yellow_cards_seventy_five_min
         , syc.yellow_cards_ninety_min::int as yellow_cards_ninety_min
         , syc.yellow_cards_hundred_five_min::int as yellow_cards_hundred_five_min
         , syc.yellow_cards_hundred_twenty_min::int as yellow_cards_hundred_twenty_min
         , syc.yellow_cards_fifteen_min_percentage::VARCHAR(10) as yellow_cards_fifteen_min_percentage
         , syc.yellow_cards_thirty_min_percentage::VARCHAR(10) as yellow_cards_thirty_min_percentage
         , syc.yellow_cards_forty_five_min_percentage::VARCHAR(10) as yellow_cards_forty_five_min_percentage
         , syc.yellow_cards_sixty_min_percentage::VARCHAR(10) as yellow_cards_sixty_min_percentage
         , syc.yellow_cards_seventy_five_min_percentage::VARCHAR(10) as yellow_cards_seventy_five_min_percentage
         , syc.yellow_cards_ninety_min_percentage::VARCHAR(10) as yellow_cards_ninety_min_percentage
         , syc.yellow_cards_hundred_five_min_percentage::VARCHAR(10) as yellow_cards_hundred_five_min_percentage
         , syc.yellow_cards_hundred_twenty_min_percentage::VARCHAR(10) as yellow_cards_hundred_twenty_min_percentage
         , TO_TIMESTAMP(syc.ingestion_date::STRING,'YYYY/MM/DD HH24:MI:SS') as ingestion_date
      FROM statistics_yellow_cards syc
         
