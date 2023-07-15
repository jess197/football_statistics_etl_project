WITH curated_standings AS (
    SELECT season, rank, team_id
    FROM {{ ref('standings') }}
),
curated_teams_brazil AS (
    SELECT team_code, team_name, team_id
    FROM {{ ref('brazillian_teams') }}
)
    SELECT team_code, team_name, count(cs.team_id) as total_appearances 
    FROM curated_standings cs
    JOIN curated_teams_brazil ct ON cs.team_id = ct.team_id
    where cs.rank <= 6
    GROUP BY all
    order by total_appearances desc
    limit 6
    


