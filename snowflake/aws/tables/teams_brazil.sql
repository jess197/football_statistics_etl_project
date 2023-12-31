CREATE DATABASE FOOTBALL_DATA;
USE FOOTBALL_DATA;
CREATE SCHEMA FOOTBALL_DATA.RAW;

---- ## BRAZIL TEAMS ## ----
CREATE OR REPLACE TABLE FOOTBALL_DATA.RAW.TEAMS_BRAZIL(
RAW_FILE VARIANT);
-- ALTAMENTE COMPRIMIDO, UTILIZADOS PARA SEMIESTRUTURADOS, OTIMIZA CONSULTAS NO SNOWFLAKE

-- I USED THIS COPY BELLOW BECAUSE IT'S A HISTORICAL DATA THAT WON'T CHANGE
COPY INTO FOOTBALL_DATA.RAW.TEAMS_BRAZIL
  FROM @MANAGE_DB.EXTERNAL_STAGES.TEAMS_BRAZIL_STAGE_S3
  FILE_FORMAT = MANAGE_DB.FILE_FORMATS.JSON_FORMAT
  ON_ERROR = SKIP_FILE;

SELECT * 
  FROM FOOTBALL_DATA.RAW.TEAMS_BRAZIL;