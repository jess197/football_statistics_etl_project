import sys
import os
# Get the parent directory path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)
from utils.data_requester import DataRequester
import json
from datetime import datetime
import logging
from confluent_kafka import Producer
from threading import Thread
import time
from .sql.snowflake_conn import SnowflakeConn


logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='streaming/producer.log',
    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)




def get_date():
    date_hour_now = datetime.now()
    return date_hour_now

def get_date_format_json(date):
    date_hour_formated = date.strftime("%Y/%m/%d %H:%M:%S")
    return date_hour_formated

def get_date_format_file(date):
    date_hour_formated = date.strftime("%Y%m%d_%H%M%S")
    return date_hour_formated


def process_fixtures_odds_in_match():
    threads_list = list()
    sf = SnowflakeConn()
    fixtures_ongoing = sf.get_fixtures_ongoing()

    for fixture in fixtures_ongoing:
        thread = Thread(target=lambda: process_fixture_ongoing(fixture=fixture))
        threads_list.append(thread)
        time.sleep(1)
        thread.start()

    for thread in threads_list:
        thread.join()


def process_fixture_ongoing(fixture):
    logger.info(f"Thread for fixture: {fixture} started")
    p = Producer({'bootstrap.servers':'192.168.0.105:9092'})

    retry_count = 0
    while 1:
        requester = DataRequester()
        response_odds = requester.request_fixtures_odds_in_match(fixture)

        logger.info(response_odds)
        logger.info(len(response_odds['response']))

        if len(response_odds['response']) < 1 and retry_count <= 10:
            retry_count += 1
            logger.info(f"Couldn`t get data from request. Retry count: {retry_count}")
            time.sleep(60)
            continue

        if response_odds['response'][0]['fixture']['status']['long'] == 'Match Finished' or retry_count > 10:
            logger.info(f"Thread for fixture: {fixture} exited")
            break

        retry_count = 0

        date = get_date()
        ingestion_date = get_date_format_json(date=date)

        for response in response_odds['response']:
            for odd in response['odds']:
                odd_response = {
                    'fixture': response['fixture'],
                    'league': response['league'],
                    'teams': response['teams'],
                    'status': response['status'],
                    'update': response['update'],
                    'ingestion_date': ingestion_date,
                    'odd': odd
                } 

                p.poll(1)
                p.produce('football-odds-in-match', json.dumps(odd_response).encode('utf-8'),callback=receipt)
                p.flush()

        time.sleep(60)



def receipt(err,msg):
    if err is not None:
        logger.error(err)
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)


#process_fixtures_odds_in_match()