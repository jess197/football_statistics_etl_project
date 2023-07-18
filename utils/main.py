from .data_uploader import DataUploader
from .data_requester import DataRequester
import json
import re 
from datetime import timedelta, datetime


requester = DataRequester()
uploader = DataUploader()

def get_date():
    date_hour_now = datetime.now()
    return date_hour_now

def get_date_format_json(date):
    date_hour_formated = date.strftime("%Y/%m/%d %H:%M:%S")
    return date_hour_formated

def get_date_format_file(date):
    date_hour_formated = date.strftime("%Y%m%d_%H%M%S")
    return date_hour_formated


def process_teams():
    response = requester.request_teams('Brazil')
    date = get_date()
    ingestion_date = get_date_format_json(date=date)
    response['ingestion_date'] = ingestion_date
    json_data = json.dumps(response)    
    file_name = f'teams_brazil/teams.json'
    uploader.upload_file_to_S3(file_name,json_data)


def process_teams_statistics():
    response_league = requester.request_league(71)
    for season in response_league['response'][0]['seasons']:
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
                response_team = requester.request_teams_statistics(team_id,71,season)
                date = get_date()
                ingestion_date = get_date_format_json(date=date)
                response_team['response']['ingestion_date'] = ingestion_date
                json_data = json.dumps(response_team['response'])
                file_name = f'teams_statistics/{championship}/{season}/{team_name}/statistics_{team_name}.json'
                uploader.upload_file_to_S3(file_name,json_data)
        else:
            continue

def process_standings():
    response = requester.request_league(71)
    for season in response['response'][0]['seasons']:
        year = season['year']
        if int(year) >= 2018:
            response_standings = requester.request_standings(71,year)
            date = get_date()
            ingestion_date = get_date_format_json(date=date)
            standings = {
                            "response": response_standings['response'],
                            "ingestion_date": ingestion_date
                        }
            json_data = json.dumps(standings)
            file_name = f'teams_standings/{year}/standings.json'
            uploader.upload_file_to_S3(file_name,json_data)
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
                date = get_date()
                ingestion_date = get_date_format_json(date=date)
                fixtures = {
                            "response": response_fixtures['response'],
                            "ingestion_date": ingestion_date
                        }
                json_data = json.dumps(fixtures)
                file_name = f'teams_fixtures/{championship}/{season}/{team_name}/fixtures.json'
                uploader.upload_file_to_S3(file_name,json_data)
        else:
            continue

def process_fixtures_statistics(): 
    response = requester.request_league(71)
    for season in response['response'][0]['seasons']:
        year = season['year']
        if int(year) >= 2018:
            response_standings = requester.request_standings(71, year)
            championship = response_standings['response'][0]['league']['name']
            championship = championship.lower().replace(' ', '_')
            season = response_standings['response'][0]['league']['season']
            for team in response_standings['response'][0]['league']['standings'][0]:
                team_id = team['team']['id']
                response_fixtures = requester.request_fixtures(71, year, team_id)
                
                for fixture in response_fixtures['response']:
                    fixture_id = fixture['fixture']['id']
                    team_home = fixture['teams']['home']['name']
                    team_home = team_home.lower().replace(' ', '-')
                    team_away = fixture['teams']['away']['name']
                    team_away = team_away.lower().replace(' ', '-')
                    round = fixture['league']['round']
                    round = re.sub(r'\s*-\s*', '_', round.lower())
                    round = round.replace(' ', '_')
                    date = get_date()
                    ingestion_date = get_date_format_json(date=date)
                    file_name = f'teams_fixtures_statistics/{championship}/{season}/{round}/{team_home}X{team_away}.json'

                    if not uploader.blob_exists(file_name):
                        response_fixt_statistics = requester.request_fixtures_statistics(fixture_id)
                        fixture_statistics = {
                            "fixture_id": fixture_id,
                            "response": response_fixt_statistics['response'],
                            "ingestion_date": ingestion_date
                        }
                        if len(response_fixt_statistics['response']) > 0:
                            json_data = json.dumps(fixture_statistics)
                            uploader.upload_file_to_S3(file_name, json_data)
                        else:
                            continue
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
                    date = get_date()
                    ingestion_date = get_date_format_json(date=date)
                    file_name = f'teams_fixtures_predictions/{championship}/{season}/{round}/{team_home}X{team_away}_predictions.json'
                    if not uploader.blob_exists(file_name):
                        response_fixt_predictions = requester.request_fixtures_predictions(fixture_id)
                        predict = response_fixt_predictions['response'][0]['predictions']
                        predict['fixture_id'] = fixture_id
                        predict['ingestion_date'] = ingestion_date
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
            date = get_date()
            ingestion_date = get_date_format_json(date=date)
            dt_file_name = get_date_format_file(date=date)           
            file_name = f'teams_fixtures_odds_pre/{championship}/{season}/{round}/{team_home}X{team_away}_odds_pre_{dt_file_name}.json'
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
                    fixt_odds_pre = {
                        "response": response_fixt_odds_pre['response'],
                        "ingestion_date": ingestion_date
                    }
                    json_data = json.dumps(fixt_odds_pre)
                    uploader.upload_file_to_S3(file_name,json_data)
                else:
                    print(f'File already found: {file_name}')
                    continue


