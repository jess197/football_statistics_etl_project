CREATE OR REPLACE TASK UPDATE_TEAMS_STATISTICS_TASK
WAREHOUSE = 'INGESTION_WAREHOUSE'
SCHEDULE = 'USING CRON */2 * * * * UTC'
AS
ALTER PIPE MANAGE_DB.PIPES.TEAMS_STATISTICS_RAW SET PIPE_EXECUTION_PAUSED = false;


CREATE OR REPLACE TASK UPDATE_TEAMS_STATISTICS_REFRESH_TASK
WAREHOUSE = 'INGESTION_WAREHOUSE'
SCHEDULE = 'USING CRON */3 * * * * UTC'
AS
ALTER PIPE MANAGE_DB.PIPES.TEAMS_STATISTICS_RAW REFRESH;

SHOW TASKS;

ALTER TASK UPDATE_TEAMS_STATISTICS_TASK RESUME;
ALTER TASK UPDATE_TEAMS_STATISTICS_REFRESH_TASK RESUME;

ALTER TASK UPDATE_TEAMS_STATISTICS_TASK SUSPEND;
ALTER TASK UPDATE_TEAMS_STATISTICS_REFRESH_TASK SUSPEND;

select current_datetime();

