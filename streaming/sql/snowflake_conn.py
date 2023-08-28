from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
import pandas as pd
import datetime

registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')

class SnowflakeConn:


    def __init__(self) -> None:
        self.__engine = create_engine(
            'snowflake://{user}:{password}@{account}/{db}/{schema}?warehouse={warehouse}'.format(
                user='******',
                password='*******',
                account='*******', 
                warehouse='*******',
                db='*******',
                schema='*******'
            )
        )
        
    
    def get_fixtures_ongoing(self):
        now = datetime.datetime.utcnow()
        query = f"SELECT DISTINCT fixture_id FROM FOOTBALL_DATA.FOOTBALL_STAGING.TEAMS_FIXTURES WHERE league_season = 2023 and TO_TIMESTAMP('{now}') between TO_TIMESTAMP(fixture_date) and DATEADD(minute, 15, TO_TIMESTAMP(fixture_date))"
        df = pd.read_sql_query(query, self.__engine)
        return df['fixture_id']