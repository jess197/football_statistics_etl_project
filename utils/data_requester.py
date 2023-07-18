
from .football_api import FootballAPI


class DataRequester:

    def __init__(self):
        self.api = FootballAPI()

    def request_teams(self, country):
        response = self.api.get(f'teams?country={country}')
        return response
    
    def request_teams_statistics(self, team_id, league_id, season_year):
        response = self.api.get(f'teams/statistics?team={team_id}&league={league_id}&season={season_year}')
        return response
    
    def request_standings(self,league_id,season_year):
        response = self.api.get(f'standings?league={league_id}&season={season_year}')
        return response
    
    def request_league(self,league_id):
        response = self.api.get(f'leagues?id={league_id}')
        return response
    
    def request_fixtures(self,league_id,season_year,team_id): 
        response = self.api.get(f'fixtures?league={league_id}&season={season_year}&team={team_id}')
        return response
    
    def request_fixtures_statistics(self,fixture_id):
        response = self.api.get(f'fixtures/statistics?fixture={fixture_id}')
        return response
    
    def request_fixtures_predictions(self,fixture_id):
        response = self.api.get(f'predictions?fixture={fixture_id}')
        return response
    
    def request_fixtures_odds_pre_match(self,fixture_id,bet_id,bookmaker_id):
        response = self.api.get(f'odds?fixture={fixture_id}&bet={bet_id}&bookmaker={bookmaker_id}')
        return response