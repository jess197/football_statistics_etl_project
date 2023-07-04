from data_uploader import DataUploader
from data_requester import DataRequester
import json
import time 
import re 
from datetime import timedelta,datetime

requester = DataRequester()
uploader = DataUploader()

def process_teams():
    response = requester.request_teams('Brazil')
    json_data = json.dumps(response['response'])
    uploader.upload_file_to_S3('teams_brazil/teams.json',json_data)

def process_teams_statistics():
    response_league = requester.request_league(71)
    for season in response_league['response'][0]['seasons']:
        year = season['year']
        if int(year) >= 2022:
            response_standings= requester.request_standings(71,year)
            championship = response_standings['response'][0]['league']['name']
            championship = championship.lower().replace(' ', '_')
            season = response_standings['response'][0]['league']['season']
            for team in response_standings['response'][0]['league']['standings'][0]:
                team_id = team['team']['id']
                team_name = team['team']['name']
                team_name = team_name.lower().replace(' ', '-')
                time.sleep(5)
                response_team = requester.request_teams_statistics(team_id,71,season)
                json_data = json.dumps(response_team['response'])
                uploader.upload_file_to_S3(f'teams_statistics/{championship}/{season}/{team_name}/statistics.json',json_data)
        else:
            continue

def process_standings():
    response = requester.request_league(71)
    for season in response['response'][0]['seasons']:
        year = season['year']
        if int(year) >= 2018:
            response_standings = requester.request_standings(71,year)
            json_data = json.dumps(response_standings['response'])
            uploader.upload_file_to_S3(f'teams_standings/{year}/standings.json',json_data)
        else:
            continue


def process_fixtures():
    response = requester.request_league(71)
    for season in response['response'][0]['seasons']:
        year = season['year']
        if int(year) >= 2018:
            response_standings= requester.request_standings(71,year)
            championship = response_standings['response'][0]['league']['name']
            championship = championship.lower().replace(' ', '_')
            season = response_standings['response'][0]['league']['season']
            for team in response_standings['response'][0]['league']['standings'][0]:
                team_id = team['team']['id']
                team_name = team['team']['name']
                team_name = team_name.lower().replace(' ', '-')
                response_fixtures = requester.request_fixtures(71,year,team_id)
                json_data = json.dumps(response_fixtures['response'])
                uploader.upload_file_to_S3(f'teams_fixtures/{championship}/{season}/{team_name}/fixtures.json',json_data)
        else:
            continue

# TODO Fix this to add fixture_id on the json files  
def process_fixtures_statistics(): 
    response = requester.request_league(71)
    for season in response['response'][0]['seasons']:
        year = season['year']
        if int(year) >= 2018:
            response_standings= requester.request_standings(71,year)
            championship = response_standings['response'][0]['league']['name']
            championship = championship.lower().replace(' ', '_')
            season = response_standings['response'][0]['league']['season']
            for team in response_standings['response'][0]['league']['standings'][0]:
                team_id = team['team']['id']
                response_fixtures = requester.request_fixtures(71,year,team_id)
                for fixture in response_fixtures['response']:
                    fixture_id = fixture['fixture']['id']
                    team_home = fixture['teams']['home']['name']
                    team_home = team_home.lower().replace(' ', '-')
                    team_away = fixture['teams']['away']['name']
                    team_away = team_away.lower().replace(' ', '-')
                    round = fixture['league']['round']
                    round = re.sub(r'\s*-\s*', '_', round.lower())
                    round = round.replace(' ', '_')
                    file_name = f'teams_fixtures_statistics/{championship}/{season}/{round}/{team_home}X{team_away}.json'
                    if not uploader.blob_exists(file_name):
                        response_fixt_statistics = requester.request_fixtures_statistics(fixture_id)
                        json_data = json.dumps(response_fixt_statistics['response'])
                        uploader.upload_file_to_S3(file_name,json_data)
                    else:
                        print(f'File already found: {file_name}')
                        continue
        else:
            continue

def process_fixtures_predictions():
    response = requester.request_league(71)
    for season in response['response'][0]['seasons']:
        year = season['year']
        if int(year) >= 2018:
            response_standings= requester.request_standings(71,year)
            championship = response_standings['response'][0]['league']['name']
            championship = championship.lower().replace(' ', '_')
            season = response_standings['response'][0]['league']['season']
            for team in response_standings['response'][0]['league']['standings'][0]:
                team_id = team['team']['id']
                response_fixtures = requester.request_fixtures(71,year,team_id)
                for fixture in response_fixtures['response']:
                    fixture_id = fixture['fixture']['id']
                    team_home = fixture['teams']['home']['name']
                    team_home = team_home.lower().replace(' ', '-')
                    team_away = fixture['teams']['away']['name']
                    team_away = team_away.lower().replace(' ', '-')
                    round = fixture['league']['round']
                    round = re.sub(r'\s*-\s*', '_', round.lower())
                    round = round.replace(' ', '_')
                    file_name = f'teams_fixtures_predictions/{championship}/{season}/{round}/{team_home}X{team_away}_predictions.json'
                    if not uploader.blob_exists(file_name):
                        response_fixt_predictions = requester.request_fixtures_predictions(fixture_id)
                        predict = response_fixt_predictions['response'][0]['predictions']
                        predict['fixture_id'] = fixture_id
                        json_data = json.dumps(predict)
                        uploader.upload_file_to_S3(file_name,json_data)
                    else:
                        print(f'File already found: {file_name}')
                        continue
        else:
            continue

def process_fixtures_odds(): 
    response_standings= requester.request_standings(71,2023)
    championship = response_standings['response'][0]['league']['name']
    championship = championship.lower().replace(' ', '_')
    season = response_standings['response'][0]['league']['season']
    for team in response_standings['response'][0]['league']['standings'][0]:
        team_id = team['team']['id']
        response_fixtures = requester.request_fixtures(71,2023,team_id)
        for fixture in response_fixtures['response']:
            fixture_id = fixture['fixture']['id']
            team_home = fixture['teams']['home']['name']
            team_home = team_home.lower().replace(' ', '-')
            team_away = fixture['teams']['away']['name']
            team_away = team_away.lower().replace(' ', '-')
            round = fixture['league']['round']
            round = re.sub(r'\s*-\s*', '_', round.lower())
            round = round.replace(' ', '_')
            file_name = f'teams_fixtures_odds_pre/{championship}/{season}/{round}/{team_home}X{team_away}_odds_pre.json'
            date = fixture['fixture']['date']
            fixture_date = datetime.fromisoformat(date).date()
            dia_atual = datetime.today().date()
            # historico de odds pre match 7 dias 
            # existem odds pre match até 14 dias antes de uma partida
            periodo_inicio = dia_atual - timedelta(days=7)
            periodo_fim = dia_atual + timedelta(days=14)
            if periodo_inicio <= fixture_date <= periodo_fim:
                if not uploader.blob_exists(file_name):
                    #bet_id - 1. quem vai ganhar? 
                    #bookmaker - bet365
                    response_fixt_odds_pre = requester.request_fixtures_odds_pre_match(fixture_id,1,8)
                    json_data = json.dumps(response_fixt_odds_pre['response'])
                    uploader.upload_file_to_S3(file_name,json_data)
                else:
                    print(f'File already found: {file_name}')
                    continue

# TODO CREATE DAGS IN AIRFLOW FOR EACH BATCH OR STREAMING PROCESS  
process_teams()
process_teams_statistics()
process_standings()
process_fixtures()
process_fixtures_statistics()
process_fixtures_predictions()
process_fixtures_odds()

