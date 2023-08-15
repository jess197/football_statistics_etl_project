USE DATABASE MANAGE_DB; 

CREATE OR REPLACE SCHEMA MANAGE_DB.PIPES; 

USE SCHEMA PIPES; 

CREATE OR REPLACE  PIPE MANAGE_DB.PIPES.TEAMS_ODDS_PRE_MATCH_RAW
    AUTO_INGEST = TRUE
    COMMENT = 'Pipe to autoingest odds pre match of brazil teams data from s3'
 AS 
  COPY INTO FOOTBALL_DATA.RAW.TEAMS_ODDS_PRE_MATCH
  FROM @MANAGE_DB.EXTERNAL_STAGES.TEAMS_ODDS_PRE_MATCH_STAGE_S3
  FILE_FORMAT = MANAGE_DB.FILE_FORMATS.JSON_FORMAT
  ON_ERROR = CONTINUE;

DESC PIPE TEAMS_ODDS_PRE_MATCH_RAW;

SELECT * FROM FOOTBALL_DATA.RAW.TEAMS_ODDS_PRE_MATCH;

SELECT SYSTEM$PIPE_STATUS('TEAMS_ODDS_PRE_MATCH_RAW');

SELECT * FROM TABLE(validate_pipe_load(
PIPE_NAME => 'MANAGE_DB.PIPES.TEAMS_ODDS_PRE_MATCH_RAW',
START_TIME => DATEADD(YEAR,-2,CURRENT_TIMESTAMP())
));

SELECT *
  FROM TABLE (INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'FOOTBALL_DATA.RAW.TEAMS_ODDS_PRE_MATCH',
  START_TIME => DATEADD(YEAR,-2,CURRENT_TIMESTAMP())
  ));  

SELECT * 
  FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
  DATE_RANGE_START=> DATEADD(DAYS,-2,CURRENT_TIMESTAMP()),
  PIPE_NAME => 'MANAGE_DB.PIPES.TEAMS_ODDS_PRE_MATCH_RAW'
  ));

ALTER PIPE MANAGE_DB.PIPES.TEAMS_ODDS_PRE_MATCH_RAW SET PIPE_EXECUTION_PAUSED = false;

ALTER PIPE MANAGE_DB.PIPES.TEAMS_ODDS_PRE_MATCH_RAW REFRESH; 

SELECT * FROM FOOTBALL_DATA.RAW.TEAMS_ODDS_PRE_MATCH LIMIT 5000;

SHOW PIPES; 

