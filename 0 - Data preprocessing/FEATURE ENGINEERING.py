# Databricks notebook source
# MAGIC %md
# MAGIC # EXPLORATION

# COMMAND ----------

# MAGIC %md
# MAGIC     AVG(brakeTemp1, brakeTemp2, brakeTemp3, brakeTemp4) AS avgBrakeTemp,
# MAGIC     AVG(surfaceTyreTemp1, surfaceTyreTemp2, surfaceTyreTemp3, surfaceTyreTemp4) AS avgSurfaceTyreTemp,
# MAGIC     AVG(innerTyreTemp1, innerTyreTemp2, innerTyreTemp3, innerTyreTemp4) AS avgInnerTyreTemp,
# MAGIC     AVG(tyrePressure1, tyrePressure2, tyrePressure3, tyrePressure4) AS avgTyrePressure,

# COMMAND ----------

# MAGIC %md
# MAGIC ## CREATE BASE GOLD TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE goldData AS (
# MAGIC       SELECT
# MAGIC         sessionUID,
# MAGIC         CAST(currentLapNum AS INT),
# MAGIC         CAST(speed AS INT), 
# MAGIC         CAST(throttle AS FLOAT) * 100 AS throttle,
# MAGIC         CAST(steer AS FLOAT),
# MAGIC         CAST(brake AS FLOAT) * 100 AS brake,
# MAGIC         CAST(gear AS INT),
# MAGIC         CAST(rpm AS INT),
# MAGIC         CAST(drs AS INT),
# MAGIC         CAST(revLightPercentage AS INT),
# MAGIC         CAST(engineTemp AS INT),
# MAGIC         CAST(currentLapTimeInMs AS FLOAT) / 1000.0 AS currentLapTimeInSec,
# MAGIC         CAST(lapDistanceMeters AS FLOAT),
# MAGIC         (CAST(lapDistanceMeters AS FLOAT) / 5793) * 100 AS percentageCompleted,
# MAGIC
# MAGIC         date_format(concat(
# MAGIC           date_format(from_unixtime(currentLapTimeInSec), 'HH:mm:ss'),
# MAGIC           '.',
# MAGIC           LPAD(CAST((currentLapTimeInSec - FLOOR(currentLapTimeInSec)) * 1000 AS INT), 3, '0')
# MAGIC           ), 'HH:mm:ss.SSS') AS lap_time, 
# MAGIC
# MAGIC         MAX(CAST(currentLapTimeInMs AS FLOAT) / 1000.0) OVER (PARTITION BY sessionUID, currentLapNum) AS timeToPredict
# MAGIC     FROM
# MAGIC         lap_vs_telemetry
# MAGIC     ORDER BY sessionUID, currentLapNum, currentLapTimeInSec ASC 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from goldData where currentLapNum = 9

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXP 1: SESONALITY BASED ON SESSION TIME

# COMMAND ----------

# MAGIC %md
# MAGIC ### QUERY

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TAKES INTO ACCOUNT ONLY ONE SESSION ID = 11662394204853373771
# MAGIC CREATE OR REPLACE TEMPORARY VIEW EXP1 AS (
# MAGIC       SELECT
# MAGIC         sessionUID,
# MAGIC         CAST(currentLapNum AS INT),
# MAGIC         CAST(speed AS INT), 
# MAGIC         CAST(throttle AS FLOAT) * 100 AS throttle,
# MAGIC         CAST(steer AS FLOAT),
# MAGIC         CAST(brake AS FLOAT) * 100 AS brake,
# MAGIC         CAST(gear AS INT),
# MAGIC         CAST(rpm AS INT),
# MAGIC         CAST(drs AS INT),
# MAGIC         CAST(revLightPercentage AS INT),
# MAGIC         CAST(engineTemp AS INT),
# MAGIC         CAST(currentLapTimeInMs AS FLOAT) / 1000.0 AS currentLapTimeInSec,
# MAGIC         CAST(lapDistanceMeters AS FLOAT),
# MAGIC         (CAST(lapDistanceMeters AS FLOAT) / 5793) * 100 AS percentageCompleted,
# MAGIC
# MAGIC         date_format(concat(
# MAGIC           date_format(from_unixtime(currentLapTimeInSec), 'HH:mm:ss'),
# MAGIC           '.',
# MAGIC           LPAD(CAST((currentLapTimeInSec - FLOOR(currentLapTimeInSec)) * 1000 AS INT), 3, '0')
# MAGIC           ), 'HH:mm:ss.SSS') AS lap_time, 
# MAGIC
# MAGIC         date_format(concat(
# MAGIC           date_format(from_unixtime(sessionTimestamp), 'HH:mm:ss'),
# MAGIC           '.',
# MAGIC           LPAD(CAST((sessionTimestamp - FLOOR(sessionTimestamp)) * 1000 AS INT), 4, '0')
# MAGIC           ), 'HH:mm:ss.SSS') AS overallSessionTime,
# MAGIC
# MAGIC         MAX(CAST(currentLapTimeInMs AS FLOAT) / 1000.0) OVER (PARTITION BY sessionUID, currentLapNum) AS timeToPredict
# MAGIC
# MAGIC     FROM
# MAGIC         lap_vs_telemetry
# MAGIC     WHERE sessionUID = "11662394204853373771"
# MAGIC     ORDER BY sessionUID, currentLapNum, currentLapTimeInSec ASC 
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXP1 - EXPLORATION

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM EXP1 group ORDER BY overallSessionTime ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   currentLapNum,
# MAGIC   count(1) as ct,
# MAGIC   date_format(lap_time, 'HH:mm:ss') as secs
# MAGIC   
# MAGIC FROM EXP1 group by currentLapNum, secs ORDER BY currentLapNum, secs ASC

# COMMAND ----------


