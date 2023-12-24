# Exploring the Emotions of Brasileirão: An Innovative Data Journey 
### ELT Football API Data Project

### INTRO
<p>
  I invite you to delve into the heart of the best Championship of the world, the Brazilian Championship, most known as Brasileirão to discover some stories about football.
  This data engineering project is a fascinating odyssey, bringing forth not just statistics but also insights that challenge the way we perceive Brazil's most beloved sport.
 </p>
 <p> The choosen data stack was ith a modern approach, we adopted the most advanced technology stack: Snowflake, dbt, Kafka, Airflow, Redash, Python, Docker, and AWS.  </p>
 <p>  I choosed to do this project because of my love about football, I enjoyed each single moment doing this about my favorite sport. Hope you enjoy it too :) </p>
</p>

### DATA ARCHITECTURE 
![Project Architecture](img/football_api_project_elt.png)

### SOLUTION  

The project adopts a Lambda architecture, seamlessly blending batch and streaming layers, with data sourced from the API Football (https://www.api-football.com/).

#### Data Extraction:
The data extraction process is automated using Apache Airflow pipelines. Python scripts within these pipelines adhere to best practices in software development and Object-Oriented Programming (OOP).
The process is facilitated by four scripts designed for the extraction:


#### API Connection
```football_api.py: ```

Contains a class, ```APIFootball```, simplifying the connection to the Football API. The class facilitates various API actions by providing the action name and query string. To use this class, set your Football API key as an environment variable named ```API_KEY```.

#### API Requests
```data_requester.py:```

The ```DataRequester``` class simplifies the retrieval of football events data from the Football API using the ```APIFootball``` class.


#### AWS Connection
```data_uploader.py:```

The ```DataUploader``` class handles the upload of data to an Amazon S3 (Simple Storage Service) bucket.
The script establishes seamless a connection with AWS using boto3 AWS SDK for python. 

#### Core Component
```main.py:```
The core component of the application orchestrates the process. It collects football events data from the Football API using the ```DataRequester``` class and uploads it to S3 using the ```DataUploader``` class.

#### Airflow Setup
The project leverages the Astro CLI for Apache Airflow orchestration, this aproach facilitates the management of Apache Airflow. The following commands initialize, start, and stop the project:
(https://docs.astronomer.io/astro/cli/overview)
```
astro dev init
astro dev start
astro dev stop
```
#### DAGS:
```
dag_process_teams.py
dag_process_teams_statistics.py
dag_process_standings.py
dag_process_odds_in_match.py
dag_process_fixtures.py
dag_process_fixtures_statistics.py
dag_process_fixtures_predictions.py
dag_process_fixtures_odds_pre.py
```
