  select sum(fs.yellow_cards)+sum(ifnull(fs.red_cards,0)) as total_cards
     ,ifnull(dp.referee_name_para, fi.fixture_referee) as referee
  from {{ ref('fixtures_statistics') }} fs
  inner join {{ ref('fixtures') }} fi
  on fs.fixture_id = fi.fixture_id
    and fi.league_season = 2023
  left join  {{ ref('de_para_match_referee_names') }} dp 
    on fi.fixture_referee = dp.referee_name_de
    group by all
    having total_cards is not null
    order by total_cards desc
  limit 7