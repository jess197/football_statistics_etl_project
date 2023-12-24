{{ config(materialized='table') }}

select 
    fom.record_content:fixture:id as fixture_id
    ,fom.record_content:fixture:status:elapsed as match_time_minutes
    ,fom.record_content:fixture:status:long as match_time
    ,fom.record_content:odd:name as odd_name
    ,oddval.value:odd as odd
    ,oddval.value:value as team_to_win
    ,fom.record_content:ingestion_date as ingestion_date
from FOOTBALL_DATA.FOOTBALL_STREAMING.FOOTBALL_ODDS_IN_MATCH_1377302342 fom
,lateral flatten(input => fom.record_content:odd, path => 'values') oddval
where fom.record_content:fixture:id is not null