import sys
import os
# Get the parent directory path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)
from utils.data_requester import DataRequester
import json
import re 
from datetime import timedelta, datetime
import logging
from confluent_kafka import Producer


requester = DataRequester()

def get_date():
    date_hour_now = datetime.now()
    return date_hour_now

def get_date_format_json(date):
    date_hour_formated = date.strftime("%Y/%m/%d %H:%M:%S")
    return date_hour_formated

def get_date_format_file(date):
    date_hour_formated = date.strftime("%Y%m%d_%H%M%S")
    return date_hour_formated


def process_fixtures_odds_pre_match():
    p=Producer({'bootstrap.servers':'192.168.0.105:9092'})
    print('oi')

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
            date = fixture['fixture']['date']
            fixture_date = datetime.fromisoformat(date).date()
            dia_atual = datetime.today().date()
            # historico de odds pre match 7 dias 
            # existem odds pre match até 14 dias antes de uma partida
            periodo_inicio = dia_atual - timedelta(days=7)
            periodo_fim = dia_atual + timedelta(days=14)
            if periodo_inicio <= fixture_date <= periodo_fim:
                response_fixt_odds_pre = requester.request_fixtures_odds_pre_match(fixture_id,1,8)
                fixt_odds_pre = {
                    "response": response_fixt_odds_pre['response'],
                    "ingestion_date": ingestion_date
                }
                json_data = json.dumps(fixt_odds_pre)
                p.poll(1)
                p.produce('football-odds-pre-match', json_data.encode('utf-8'),callback=receipt)
                p.flush()
            else:
                continue





logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='streaming/producer.log',
    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)


process_fixtures_odds_pre_match()
        
     
