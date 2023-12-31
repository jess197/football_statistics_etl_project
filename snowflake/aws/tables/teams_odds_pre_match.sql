---- ## SERIE A TEAMS ODDS PRE MATCH ## ----
USE FOOTBALL_DATA;
USE SCHEMA FOOTBALL_DATA.RAW;

SELECT * FROM @MANAGE_DB.EXTERNAL_STAGES.TEAMS_ODDS_PRE_MATCH_STAGE_S3
(file_format => MANAGE_DB.FILE_FORMATS.JSON_FORMAT);

CREATE OR REPLACE TABLE FOOTBALL_DATA.RAW.TEAMS_ODDS_PRE_MATCH(
RAW_FILE VARIANT);
-- ALTAMENTE COMPRIMIDO, UTILIZADOS PARA SEMIESTRUTURADOS, OTIMIZA CONSULTAS NO SNOWFLAKE

-- I'M JUST PREPARING THE COPY BELLOW TO USE WITH SNOWPIPE
COPY INTO FOOTBALL_DATA.RAW.TEAMS_ODDS_PRE_MATCH
  FROM @MANAGE_DB.EXTERNAL_STAGES.TEAMS_ODDS_PRE_MATCH_STAGE_S3
  FILE_FORMAT = MANAGE_DB.FILE_FORMATS.JSON_FORMAT
  ON_ERROR = SKIP_FILE;

SELECT * 
  FROM FOOTBALL_DATA.RAW.TEAMS_ODDS_PRE_MATCH;