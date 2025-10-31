-- ==================================================
-- STEP 1: Database Setup
-- ==================================================
create database Traffic_volum_db;
show databases;
use Traffic_volum_db;

-- ==================================================
-- STEP 2: Create Tables
-- ==================================================
CREATE TABLE delay_log (
    delay_id VARCHAR(20) PRIMARY KEY,
    route_id VARCHAR(20),
    bus_id VARCHAR(20),
    intersection_id VARCHAR(20),
    stop_id VARCHAR(20),
    scheduled_arrival_ts DATETIME,
    actual_arrival_ts DATETIME,
    delay_minutes DECIMAL(5,2),
    delay_reason VARCHAR(50),
    date DATE
);


CREATE TABLE signal_config (
    config_id VARCHAR(20) PRIMARY KEY,
    intersection_id VARCHAR(20),
    effective_start_ts DATETIME,
    effective_end_ts DATETIME,
    signal_phase VARCHAR(20),
    duration_sec INT
);

-- Traffic Volume Log--

CREATE TABLE traffic_volume (
    record_id VARCHAR(30) PRIMARY KEY,
    intersection_id VARCHAR(20),
    intersection_name VARCHAR(100),
    zone VARCHAR(50),
    timestamp_utc DATETIME,
    date DATE,
    is_peak_hour BOOLEAN,
    vehicle_type VARCHAR(20),
    vehicle_count_total INT
);

DESC delay_log; 
DESC Signal_Timing_Config;
DESC Traffic_Volume_Log;
 ==================================================
-- STEP 3 + STEP 4: Load Data with Preprocessing
-- ==================================================

-- Public Transport Delay Log
LOAD DATA LOCAL INFILE "C:\DATA ANALYSIS\PROJECT-_284\Dataset\REAL DATASET\Public_Transport_Delay_Log.csv"
INTO TABLE delay_log
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(route_id, intersection_id, bus_id, stop_id, 
 scheduled_arrival_ts, actual_arrival_ts, 
 delay_minutes, delay_reason, date)
SET scheduled_arrival_ts = STR_TO_DATE(scheduled_arrival_ts, '%Y-%m-%d %H:%i:%s'),
    actual_arrival_ts    = STR_TO_DATE(actual_arrival_ts, '%Y-%m-%d %H:%i:%s'),
    date               = STR_TO_DATE(date, '%Y-%m-%d');

-- Signal Timing Config
LOAD DATA LOCAL INFILE "C:\DATA ANALYSIS\PROJECT-_284\Dataset\REAL DATASET\Signal_Timing_Config.csv"
INTO TABLE Signal_Config
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(intersection_id, signal_phase, duration_sec, 
 effective_start_ts, effective_end_ts)
SET effective_start_ts = STR_TO_DATE(effective_start_ts, '%Y-%m-%d %H:%i:%s'),
    effective_end_ts   = STR_TO_DATE(effective_end_ts, '%Y-%m-%d %H:%i:%s');

-- Traffic Volume Log
LOAD DATA INFILE 'C:\DATA ANALYSIS\PROJECT-_284\Dataset\REAL DATASET\Traffic_Volume_Log.csv'
INTO TABLE traffic_volume
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;
(intersection_id, zone_id, timestamp_raw, is_peak_hour, 
 vehicle_type, vehicle_count_total, day_dummy, hour_dummy)
SET timestamp_utc = STR_TO_DATE(timestamp_raw, '%Y-%m-%d %H:%i:%s'),
    day_of_week   = DAYNAME(STR_TO_DATE(timestamp_raw, '%Y-%m-%d %H:%i:%s')),
    hour_of_day   = HOUR(STR_TO_DATE(timestamp_raw, '%Y-%m-%d %H:%i:%s'));




-- ==================================================
-- STEP 5: SQL-BASED EDA & BUSINESS STATISTICS
-- ==================================================

-- ================================
-- 5.1 DATA QUALITY CHECKS
-- ================================
SELECT 'delay log' AS table_name, COUNT(*) AS row_count FROM delay_Log
UNION ALL
SELECT 'signal  config', COUNT(*) FROM Signal_Config
UNION ALL
SELECT 'traffic volume ', COUNT(*) FROM Traffic_Volume_Log;

-- Check missing values
SELECT 'Missing Values in delay log' AS check_name,
    SUM(CASE WHEN route_id IS NULL THEN 1 ELSE 0 END) AS missing_route_id,
    SUM(CASE WHEN intersection_id IS NULL THEN 1 ELSE 0 END) AS missing_intersection_id,
    SUM(CASE WHEN delay_minutes IS NULL THEN 1 ELSE 0 END) AS missing_delay,
    SUM(CASE WHEN delay_reason IS NULL THEN 1 ELSE 0 END) AS missing_reason
FROM delay_log;

-- ================================
-- 5.2 UNIVARIATE ANALYSIS
-- ================================

-- Delay: Mean, Median, Mode
-- Mean
SELECT ROUND(AVG(delay_minutes),2) AS mean_delay
FROM delay_log;

-- Median
-- Calculate median delay in MySQL
WITH ordered AS (
    SELECT d.delay_minutes,
           ROW_NUMBER() OVER (ORDER BY d.delay_minutes) AS row_num,
           COUNT(*) OVER () AS total_rows
    FROM delay_log d
)
SELECT 
    ROUND(AVG(delay_minutes),2) AS median_delay
FROM ordered
WHERE row_num IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2));



-- Mode
SELECT delay_minutes AS mode_delay
FROM delay_log
GROUP BY delay_minutes
ORDER BY COUNT(*) DESC
LIMIT 1;



-- Histogram-like bins for delay
SELECT 
    CONCAT(FLOOR(delay_minutes/5)*5, '-', FLOOR(delay_minutes/5)*5+4) AS delay_bin,
    COUNT(*) AS freq
FROM delay_log
GROUP BY delay_bin
ORDER BY MIN(delay_minutes);

-- Vehicle counts descriptive stats
SELECT 
    vehicle_type,
    MIN(vehicle_count_total) AS min_count,
    MAX(vehicle_count_total) AS max_count,
    ROUND(AVG(vehicle_count_total),2) AS mean_count,
    ROUND(STDDEV(vehicle_count_total),2) AS stddev_count
FROM traffic_volume
GROUP BY vehicle_type;

-- ================================
-- 5.3 BIVARIATE ANALYSIS
-- ================================

-- Delay by reason
SELECT delay_reason, ROUND(AVG(delay_minutes),2) AS avg_delay, COUNT(*) AS freq
FROM delay_log
GROUP BY delay_reason
ORDER BY avg_delay DESC;

-- Peak vs non-peak traffic
SELECT is_peak_hour, ROUND(AVG(vehicle_count_total),0) AS avg_vehicle_count, COUNT(*) AS samples
FROM traffic_volume
GROUP BY is_peak_hour;

-- Vehicle Type vs Peak Hour
SELECT vehicle_type,
    SUM(CASE WHEN is_peak_hour = 1 THEN vehicle_count_total ELSE 0 END) AS peak_total,
    SUM(CASE WHEN is_peak_hour = 0 THEN vehicle_count_total ELSE 0 END) AS offpeak_total
FROM traffic_volume
GROUP BY vehicle_type;

-- ================================
-- 5.4 MULTIVARIATE ANALYSIS
-- ================================

-- Intersection + Hour + Traffic
SELECT intersection_id, hour_of_day, ROUND(AVG(vehicle_count_total),0) AS avg_count
FROM traffic_volume
GROUP BY intersection_id, hour_of_day
ORDER BY avg_count DESC
LIMIT 10;h

-- Intersection + Vehicle Type + Delay
SELECT d.intersection_id, t.vehicle_type,
       ROUND(AVG(d.delay_minutes),2) AS avg_delay,
       ROUND(AVG(t.vehicle_count_total),0) AS avg_traffic
FROM delay_log d 
JOIN traffic_volume t ON d.intersection_id = t.intersection_id
GROUP BY d.intersection_id, t.vehicle_type
ORDER BY avg_delay DESC;

-- ================================
-- 5.5 CORRELATION
-- ================================

-- Overall correlation between delay and traffic
SELECT 
    (SUM((d.delay_minutes - stats.avg_delay) * (t.vehicle_count_total - stats.avg_traffic)) /
     (SQRT(SUM(POW(d.delay_minutes - stats.avg_delay, 2))) * 
      SQRT(SUM(POW(t.vehicle_count_total - stats.avg_traffic, 2))))) AS correlation_coefficient
FROM delay_log d
JOIN traffic_volume t 
    ON d.intersection_id = t.intersection_id
CROSS JOIN (
    SELECT 
        AVG(d.delay_minutes) AS avg_delay, 
        AVG(t.vehicle_count_total) AS avg_traffic
    FROM delay_log d
    JOIN traffic_volume t 
        ON d.intersection_id = t.intersection_id
) stats;




-- 5.2 Public Transport Delay Stats
SELECT 'Delay Summary' AS metric,
       MIN(delay_minutes) AS min_delay,
       MAX(delay_minutes) AS max_delay,
       ROUND(AVG(delay_minutes),2) AS avg_delay,
       ROUND(STDDEV(delay_minutes),2) AS std_delay
FROM delay_log;

SELECT delay_reason, COUNT(*) AS cnt
FROM delay_log
GROUP BY delay_reason
ORDER BY cnt DESC;

-- 5.3 Signal Timing Config Stats
SELECT signal_phase,
       MIN(duration_sec) AS min_dur,
       MAX(duration_sec) AS max_dur,
       ROUND(AVG(duration_sec),2) AS avg_dur,
       ROUND(STDDEV(duration_sec),2) AS stddev_dur
FROM signal_config
GROUP BY signal_phase;

-- 5.4 Traffic Volume Stats
SELECT vehicle_type,
       ROUND(AVG(vehicle_count_total),0) AS avg_count,
       MAX(vehicle_count_total) AS max_count,
       MIN(vehicle_count_total) AS min_count
FROM traffic_volume
GROUP BY vehicle_type
ORDER BY avg_count DESC;

-- 5.5 Temporal Analysis
SELECT date, ROUND(AVG(delay_minutes),2) AS avg_daily_delay
FROM delay_log
GROUP BY date
ORDER BY date;

SELECT hour_of_day, ROUND(AVG(vehicle_count_total),0) AS avg_count
FROM traffic_volume
GROUP BY hour_of_day
ORDER BY hour_of_day;

SELECT is_peak_hour, ROUND(AVG(vehicle_count_total),0) AS avg_count
FROM traffic_volume
GROUP BY is_peak_hour;

-- 5.6 Intersection-Level Insights
SELECT 'Busiest Intersection',
       CONCAT(intersection_id, ' (avg vehicles: ', ROUND(AVG(vehicle_count_total),0), ')') AS insight
FROM traffic_volume
GROUP BY intersection_id
ORDER BY AVG(vehicle_count_total) DESC
LIMIT 1;

SELECT 'Intersection with Max Bus Delay',
       CONCAT(intersection_id, ' (avg delay: ', ROUND(AVG(delay_minutes),2), ' mins)') AS insight
FROM delay_log
GROUP BY intersection_id
ORDER BY AVG(delay_minutes) DESC
LIMIT 1;

SELECT 'Most Delayed Route',
       CONCAT(route_id, ' (avg delay: ', ROUND(AVG(delay_minutes),2), ' mins)') AS insight
FROM delay_log
GROUP BY route_id
ORDER BY AVG(delay_minutes) DESC
LIMIT 1;

-- 5.7 Delay vs Traffic Correlation (per intersection)
SELECT d.intersection_id,
       ROUND(AVG(d.delay_minutes),2) AS avg_delay,
       ROUND(AVG(t.vehicle_count_total),0) AS avg_traffic
FROM delay_log d
JOIN traffic_volume t ON d.intersection_id = t.intersection_id
GROUP BY d.intersection_id
ORDER BY avg_delay DESC;
