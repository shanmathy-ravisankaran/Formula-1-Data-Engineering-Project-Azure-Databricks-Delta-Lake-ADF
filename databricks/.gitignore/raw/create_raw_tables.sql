-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits (
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
) USING csv
OPTIONS (
  path "/mnt/formula1dlsgen/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT *  FROM f1_raw.circuits;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races (
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
) USING csv
OPTIONS (
  path "/mnt/formula1dlsgen/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.Constructors;
CREATE TABLE IF NOT EXISTS f1_raw.Constructors (
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (
  path "/mnt/formula1dlsgen/raw/constructors.json"
)

-- COMMAND ----------

SELECT * FROM f1_raw.Constructors;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers (
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (
  path "/mnt/formula1dlsgen/raw/drivers.json"
)

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results (
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING
)
USING json    
OPTIONS (
  path "/mnt/formula1dlsgen/raw/results.json"
)

-- COMMAND ----------

SELECT  * FROM f1_raw.results

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;

CREATE TABLE f1_raw.pit_stops
USING PARQUET
OPTIONS ('path' '/mnt/formula1dlsgen/processed/pit_stops');


-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;


-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;

CREATE TABLE IF NOT EXISTS f1_raw.lap_times (
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING csv
OPTIONS (
  path "/mnt/formula1dlsgen/raw/lap_times"
);


-- COMMAND ----------

SELECT COUNT (1) FROM f1_raw.lap_times

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

CREATE TABLE IF NOT EXISTS f1_raw.qualifying (
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyId INT,
  raceId INT
)
USING JSON
OPTIONS (
  path "/mnt/formula1dlsgen/raw/qualifying", multiline true
);

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying;

-- COMMAND ----------

