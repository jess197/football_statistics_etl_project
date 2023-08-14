---- ## SERIE A TEAMS ODDS PRE MATCH ## ----
USE SCHEMA EXTERNAL_STAGES;
CREATE OR REPLACE STAGE MANAGE_DB.EXTERNAL_STAGES.TEAMS_ODDS_PRE_MATCH_STAGE_S3
    URL = 's3://football-data-bucket-s3/teams_fixtures_odds_pre/'
    STORAGE_INTEGRATION = AWS_S3_INTEGRATION;

LIST @MANAGE_DB.EXTERNAL_STAGES.TEAMS_ODDS_PRE_MATCH_STAGE_S3;

DESC STAGE TEAMS_ODDS_PRE_MATCH_STAGE_S3;

SHOW STAGES; 