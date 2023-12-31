---- ## SERIE A BRAZIL TEAMS FIXTURES PREDICTIONS BETWWEN 2018 T0 2023 ## ----
USE FOOTBALL_DATA;
USE SCHEMA FOOTBALL_DATA.RAW;

SELECT * FROM @MANAGE_DB.EXTERNAL_STAGES.TEAMS_FIXTURES_PREDICTIONS_STAGE_S3
(file_format => MANAGE_DB.FILE_FORMATS.JSON_FORMAT);

CREATE OR REPLACE TABLE FOOTBALL_DATA.RAW.TEAMS_FIXTURES_PREDICTIONS(
RAW_FILE VARIANT);
-- ALTAMENTE COMPRIMIDO, UTILIZADOS PARA SEMIESTRUTURADOS, OTIMIZA CONSULTAS NO SNOWFLAKE

-- I'M JUST PREPARING THE COPY BELLOW TO USE WITH SNOWPIPE
COPY INTO FOOTBALL_DATA.RAW.TEAMS_FIXTURES_PREDICTIONS
  FROM @MANAGE_DB.EXTERNAL_STAGES.TEAMS_FIXTURES_PREDICTIONS_STAGE_S3
  FILE_FORMAT = MANAGE_DB.FILE_FORMATS.JSON_FORMAT
  ON_ERROR = SKIP_FILE;

SELECT * 
  FROM FOOTBALL_DATA.RAW.TEAMS_FIXTURES_PREDICTIONS;