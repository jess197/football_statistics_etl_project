USE DATABASE MANAGE_DB; 

CREATE OR REPLACE SCHEMA MANAGE_DB.PIPES; 

USE SCHEMA PIPES; 

CREATE OR REPLACE PIPE MANAGE_DB.PIPES.TEAMS_FIXTURES_STATISTICS_RAW
    AUTO_INGEST = TRUE
    COMMENT = 'Pipe to autoingest fixtures/matches statistics of brazil teams data between 2018 o 2023 from s3'
 AS 
  COPY INTO FOOTBALL_DATA.RAW.TEAMS_FIXTURES_STATISTICS
  FROM @MANAGE_DB.EXTERNAL_STAGES.TEAMS_FIXTURES_STATISTICS_STAGE_S3
  FILE_FORMAT = MANAGE_DB.FILE_FORMATS.JSON_FORMAT
  ON_ERROR = CONTINUE;

DESC PIPE TEAMS_FIXTURES_STATISTICS_RAW;

SELECT * FROM FOOTBALL_DATA.RAW.TEAMS_FIXTURES_STATISTICS;

SELECT SYSTEM$PIPE_STATUS('TEAMS_FIXTURES_STATISTICS_RAW');

SELECT * FROM TABLE(validate_pipe_load(
PIPE_NAME => 'MANAGE_DB.PIPES.TEAMS_FIXTURES_STATISTICS_RAW',
START_TIME => DATEADD(YEAR,-2,CURRENT_TIMESTAMP())
));

SELECT *
  FROM TABLE (INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'FOOTBALL_DATA.RAW.TEAMS_FIXTURES_STATISTICS',
  START_TIME => DATEADD(YEAR,-2,CURRENT_TIMESTAMP())
  ));  

SELECT * 
  FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
  DATE_RANGE_START=> DATEADD(DAYS,-2,CURRENT_TIMESTAMP()),
  PIPE_NAME => 'MANAGE_DB.PIPES.TEAMS_FIXTURES_STATISTICS_RAW'
  ));

ALTER PIPE MANAGE_DB.PIPES.TEAMS_FIXTURES_STATISTICS_RAW SET PIPE_EXECUTION_PAUSED = false;

ALTER PIPE MANAGE_DB.PIPES.TEAMS_FIXTURES_STATISTICS_RAW REFRESH; 

SELECT * FROM FOOTBALL_DATA.RAW.TEAMS_FIXTURES_STATISTICS LIMIT 5000;

SHOW PIPES; 

