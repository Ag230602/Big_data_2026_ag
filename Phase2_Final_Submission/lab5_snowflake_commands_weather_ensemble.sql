-- ============================================================
-- Lab 5 Snowflake Partial Integration (Weather Ensemble)
-- Database: CS5542_DB
-- Schema:   LAB5
-- Warehouse: CATFISH_WH 
-- Role: TRAINING_ROLE
-- ============================================================

-- 0) Session context
USE ROLE TRAINING_ROLE;
USE WAREHOUSE CATFISH_WH;
USE DATABASE CS5542_DB;
USE SCHEMA LAB5;

-- 1) Verify table exists and inspect structure
SHOW TABLES LIKE 'RAW_DATA' IN SCHEMA LAB5;
DESC TABLE RAW_DATA;

-- 2) Preview a few rows
SELECT * FROM RAW_DATA LIMIT 10;

-- NOTE:
-- RAW_DATA was auto-created with generic columns C1..C6 (all VARCHAR)
-- because the CSV header row was not used as table column names.

-- 3) Query 1: Retrieval / filtering (safe numeric conversion)
-- (Uses TRY_TO_NUMBER to avoid failures due to header row or bad values)
SELECT
  C1 AS T,
  C2 AS NODE_ID,
  C3 AS ENS,
  C4 AS WIND10M,
  C5 AS PRECIP,
  C6 AS MSLP
FROM RAW_DATA
WHERE TRY_TO_NUMBER(C5) >= 10
ORDER BY TRY_TO_NUMBER(C5) DESC
LIMIT 50;

-- 4) Create a clean typed view (removes header row and casts to numbers)
CREATE OR REPLACE VIEW V_WEATHER_CLEAN AS
SELECT
  TRY_TO_NUMBER(C1) AS T,
  C2                AS NODE_ID,
  TRY_TO_NUMBER(C3) AS ENS,
  TRY_TO_NUMBER(C4) AS WIND10M,
  TRY_TO_NUMBER(C5) AS PRECIP,
  TRY_TO_NUMBER(C6) AS MSLP
FROM RAW_DATA
WHERE TRY_TO_NUMBER(C5) IS NOT NULL;

-- Validate the clean view
SELECT * FROM V_WEATHER_CLEAN LIMIT 10;
SELECT COUNT(*) AS CLEAN_ROWS FROM V_WEATHER_CLEAN;

-- 5) Query 2: Group-by analytics (node-level summary)
SELECT
  NODE_ID,
  AVG(WIND10M) AS AVG_WIND10M,
  SUM(PRECIP)  AS TOTAL_PRECIP,
  AVG(MSLP)    AS AVG_MSLP,
  COUNT(*)     AS RECORDS
FROM V_WEATHER_CLEAN
GROUP BY NODE_ID
ORDER BY TOTAL_PRECIP DESC
LIMIT 50;

-- 6) Query 3: Join + group-by (requirement)
-- Create a dimension table for nodes
CREATE OR REPLACE TABLE DIM_NODES AS
SELECT DISTINCT
  NODE_ID,
  MD5(NODE_ID) AS NODE_KEY
FROM V_WEATHER_CLEAN;

-- Join + group-by by time step (T) and node
SELECT
  d.NODE_KEY,
  w.NODE_ID,
  w.T,
  AVG(w.WIND10M) AS AVG_WIND10M,
  SUM(w.PRECIP)  AS TOTAL_PRECIP,
  AVG(w.MSLP)    AS AVG_MSLP,
  COUNT(*)       AS RECORDS
FROM V_WEATHER_CLEAN w
JOIN DIM_NODES d
  ON w.NODE_ID = d.NODE_ID
GROUP BY d.NODE_KEY, w.NODE_ID, w.T
ORDER BY w.T, TOTAL_PRECIP DESC
LIMIT 100;

-- 7) Dashboard-ready views ("data agent" layer)
CREATE OR REPLACE VIEW V_NODE_SUMMARY AS
SELECT
  NODE_ID,
  AVG(WIND10M) AS AVG_WIND10M,
  SUM(PRECIP)  AS TOTAL_PRECIP,
  AVG(MSLP)    AS AVG_MSLP,
  COUNT(*)     AS RECORDS
FROM V_WEATHER_CLEAN
GROUP BY NODE_ID;

CREATE OR REPLACE VIEW V_TIME_SUMMARY AS
SELECT
  T,
  AVG(WIND10M) AS AVG_WIND10M,
  SUM(PRECIP)  AS TOTAL_PRECIP,
  AVG(MSLP)    AS AVG_MSLP,
  COUNT(*)     AS RECORDS
FROM V_WEATHER_CLEAN
GROUP BY T
ORDER BY T;

-- 8) Pipeline logging (export results as pipeline_logs.csv from Snowsight)
CREATE OR REPLACE TABLE PIPELINE_LOGS (
  LOG_TS TIMESTAMP_NTZ,
  STEP STRING,
  STATUS STRING,
  ROWS_RETURNED NUMBER,
  NOTES STRING
);

INSERT INTO PIPELINE_LOGS
SELECT CURRENT_TIMESTAMP(), 'LOAD_RAW_DATA', 'SUCCESS',
       (SELECT COUNT(*) FROM RAW_DATA),
       'Loaded weather_ensemble.csv into CS5542_DB.LAB5.RAW_DATA (auto-created columns C1..C6).';

INSERT INTO PIPELINE_LOGS
SELECT CURRENT_TIMESTAMP(), 'CLEAN_VIEW_CREATED', 'SUCCESS',
       (SELECT COUNT(*) FROM V_WEATHER_CLEAN),
       'Created V_WEATHER_CLEAN using TRY_TO_NUMBER and removed header/non-numeric rows.';

INSERT INTO PIPELINE_LOGS
SELECT CURRENT_TIMESTAMP(), 'DASHBOARD_VIEWS_CREATED', 'SUCCESS',
       (SELECT COUNT(*) FROM V_NODE_SUMMARY),
       'Created V_NODE_SUMMARY and V_TIME_SUMMARY for charts/dashboards.';

SELECT * FROM PIPELINE_LOGS ORDER BY LOG_TS DESC;

-- 9) Dashboard queries (run and click "Chart" in Snowsight)
-- Time trend chart
SELECT * FROM V_TIME_SUMMARY;

-- Top nodes by total precipitation
SELECT *
FROM V_NODE_SUMMARY
ORDER BY TOTAL_PRECIP DESC
LIMIT 20;

-- ============================================================
-- End of script
-- ============================================================