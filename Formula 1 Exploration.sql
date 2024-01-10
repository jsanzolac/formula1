-- Databricks notebook source
DROP TABLE IF EXISTS circuits;
DROP TABLE IF EXISTS drivers;
DROP TABLE IF EXISTS results;
DROP TABLE IF EXISTS races;


CREATE TABLE circuits
USING csv
OPTIONS (
  path "/FileStore/tables/circuits.csv",
  header "true"
);

CREATE TABLE drivers
USING csv
OPTIONS (
  path "/FileStore/tables/drivers.csv",
  header "true"
);

CREATE TABLE results
USING csv
OPTIONS (
  path "/FileStore/tables/results.csv",
  header "true"
);

CREATE TABLE races
USING csv
OPTIONS (
  path "/FileStore/tables/races.csv",
  header "true"
);

-- COMMAND ----------

DESCRIBE results;

-- COMMAND ----------

CREATE TABLE XD
PARTITIONED BY (cons) AS
SELECT
  resultId,
  raceId,
  driverId,
  constructorId AS cons,
  number,
  grid
FROM
  results;

-- COMMAND ----------

SELECT * FROM results WHERE constructorId = 1;

-- COMMAND ----------

SELECT * FROM XD WHERE cons = 1;

-- COMMAND ----------

SELECT * FROM races WHERE circuitId = 6 AND year > 2015;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW partialResults AS
SELECT driverId AS did, *
FROM results;

-- COMMAND ----------

SELECT * FROM partialResults ORDER BY driverId DESC;

-- COMMAND ----------

SELECT
  driverId,
  ROUND( AVG(grid), 1) AS meanGrid,
  ROUND( AVG(position), 1) AS meanResult
FROM
  partialResults
WHERE 
  grid > 1
GROUP BY
  driverId
ORDER BY
  meanGrid ASC;

-- COMMAND ----------

SELECT COUNT( DISTINCT(driverId)) FROM partialResults;

-- COMMAND ----------

SELECT MAX(driverId) FROM results AS uniqueDrivers;

-- COMMAND ----------

SELECT 
  driverId, 
  COUNT(driverId) AS fq 
FROM results 
GROUP BY driverId 
ORDER BY fq DESC;

-- COMMAND ----------

DESCRIBE drivers;

-- COMMAND ----------

DESCRIBE drivers;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW t_drivers AS
SELECT 
  code, forename, surname, dob, nationality, url, driverId AS did_2, driverRef
FROM drivers;

-- COMMAND ----------

DESCRIBE partialResults;

-- COMMAND ----------

DROP TABLE IF EXISTS fullresults;

CREATE OR REPLACE TEMPORARY VIEW fullresults AS
SELECT
  t_drivers.*, partialResults.*
FROM
  t_drivers
JOIN partialResults ON (did=did_2);


-- COMMAND ----------

SELECT * FROM fullJoin;

-- COMMAND ----------

DESCRIBE partialResults;

-- COMMAND ----------

DESCRIBE drivers;

-- COMMAND ----------

SELECT
  drivers.* , partialResults.*
FROM
  drivers
JOIN partialResults ON (partialResults.driverId=drivers.driverId)

-- COMMAND ----------


