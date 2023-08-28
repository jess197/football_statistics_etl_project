#######------------------#######
#######CONFIGURATIONS-API#######
#######------------------######
import os
import urllib3
import json

#in the dockerfile or in the venv configured API_KEY

class FootballAPI:

    def __init__(self):
        self.base_api_url = 'https://v3.football.api-sports.io/'
        self.api_key=os.getenv('API_KEY')
        self.default_headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "v3.football.api-sports.io"
        }


    def get(self, url):
        http = urllib3.PoolManager()
        response = http.request('GET',f'{self.base_api_url}{url}', headers=self.default_headers)
        http.clear()
        return json.loads(response.data)
