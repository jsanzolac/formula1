# Databricks notebook source
aws_bucket_name = ""
mount_name = "bronze"

access_key = "" 
secret_key = ""
try:
    dbutils.fs.mount(f"s3n://{access_key}:{secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")
except:
    dbutils.fs.unmount(f"/mnt/{mount_name}")
    dbutils.fs.mount(f"s3n://{access_key}:{secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")

# COMMAND ----------

aws_bucket_name = "f1-datalake/silver"
mount_name = "silver"

access_key = "AKIA5U2XQEH7PGWG7SVN" 
secret_key = "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"
try:
    dbutils.fs.mount(f"s3n://{access_key}:{secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")
except:
    dbutils.fs.unmount(f"/mnt/{mount_name}")
    dbutils.fs.mount(f"s3n://{access_key}:{secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")

# COMMAND ----------

storageAccountName = ""
storageAccountAccessKey = ""
blobContainerName = "silver"
mountPoint = "/mnt/silver"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    try:
        dbutils.fs.mount(
            source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
            mount_point = mountPoint,
            extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
            #extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
        )
        print("mount succeeded!")
    except Exception as e:
        print("mount exception", e)

# COMMAND ----------

dbutils.fs.ls("/mnt")
#dbutils.fs.unmount("/mnt/silver/")

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession\
    .builder \
    .appName("F1 Decoding") \
    .getOrCreate()

# COMMAND ----------

raw_data = spark.read.json("dbfs:/mnt/bronze/*.json")

# COMMAND ----------

from pyspark.sql.types import MapType, StringType, DoubleType, BinaryType, ArrayType, StructType
from pyspark.sql.functions import udf
import base64
import struct

# COMMAND ----------

def decode_headers(byte_string):
    buffer_data = base64.b64decode(byte_string)
    return {
        "pFormat": str(struct.unpack('<H', buffer_data[:2])[0]),
        "gameYear": str(struct.unpack('<B', buffer_data[2:3])[0]),
        "gameMVersion": str(struct.unpack('<B', buffer_data[3:4])[0]),
        "gameMinorVersion": str(struct.unpack('<B', buffer_data[4:5])[0]),
        "packetVersion": str(struct.unpack('<B', buffer_data[5:6])[0]),
        "packetId": str(struct.unpack('<B', buffer_data[6:7])[0]),
        "sessionUID": str(struct.unpack('<Q', buffer_data[7:15])[0]),
        "sessionTimestamp": str(struct.unpack('<f', buffer_data[15:19])[0]),
        "frameID": str(struct.unpack('<L', buffer_data[19:23])[0]),
        "overallFrameID": str(struct.unpack('<L', buffer_data[23:27])[0]),
        "playerCarIdenx": str(struct.unpack('<B', buffer_data[27:28])[0]),
        "secondaryPlayerIndex": str(struct.unpack('<B', buffer_data[28:29])[0]),
    }

# COMMAND ----------

unpack_udf = udf(decode_headers, MapType(StringType(), StringType()))

# COMMAND ----------

unpacked_data = raw_data.withColumn("unpacked", unpack_udf("string")).cache()
count_records = unpacked_data.count()

# COMMAND ----------

unpacked_data.createOrReplaceTempView("sample_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW data_headers AS
# MAGIC SELECT
# MAGIC   string,
# MAGIC   unpacked['pFormat'] AS pFormat,
# MAGIC   unpacked['gameYear'] AS gameYear,
# MAGIC   unpacked['gameMVersion'] AS gameMVersion,
# MAGIC   unpacked['gameMinorVersion'] AS gameMinorVersion,
# MAGIC   unpacked['packetVersion'] AS packetVersion,
# MAGIC   unpacked['packetId'] AS packetId,
# MAGIC   unpacked['sessionUID'] AS sessionUID,
# MAGIC   unpacked['sessionTimestamp'] AS sessionTimestamp,
# MAGIC   unpacked['frameID'] AS frameID,
# MAGIC   unpacked['overallFrameID'] AS overallFrameID,
# MAGIC   unpacked['playerCarIdenx'] AS playerCarIdenx,
# MAGIC   unpacked['secondaryPlayerIndex'] AS secondaryPlayerIndex
# MAGIC FROM
# MAGIC   sample_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(packetId AS INT) packet, COUNT(1) AS FQ FROM data_headers GROUP BY packet ORDER BY packet ASC;

# COMMAND ----------

telemetry = spark.sql("SELECT * FROM data_headers WHERE packetId = '6' ").cache()

# COMMAND ----------

def decode_telemetry_data(byte_string):
    buffer_data = base64.b64decode(byte_string)
    return {
        "pFormat": str(struct.unpack('<H', buffer_data[:2])[0]),
        "gameYear": str(struct.unpack('<B', buffer_data[2:3])[0]),
        "gameMVersion": str(struct.unpack('<B', buffer_data[3:4])[0]),
        "gameMinorVersion": str(struct.unpack('<B', buffer_data[4:5])[0]),
        "packetVersion": str(struct.unpack('<B', buffer_data[5:6])[0]),
        "packetId": str(struct.unpack('<B', buffer_data[6:7])[0]),
        "sessionUID": str(struct.unpack('<Q', buffer_data[7:15])[0]),
        "sessionTimestamp": str(struct.unpack('<f', buffer_data[15:19])[0]),
        "frameID": str(struct.unpack('<L', buffer_data[19:23])[0]),
        "overallFrameID": str(struct.unpack('<L', buffer_data[23:27])[0]),
        "playerCarIdenx": str(struct.unpack('<B', buffer_data[27:28])[0]),
        "secondaryPlayerIndex": str(struct.unpack('<B', buffer_data[28:29])[0]),
    

        # TELEMETRY
        "speed": str(struct.unpack('<H', buffer_data[29:31])[0]),
        "throttle": str(struct.unpack('<f', buffer_data[31:35])[0]),
        "steer": str(struct.unpack('<f', buffer_data[35:39])[0]),
        "brake": str(struct.unpack('<f', buffer_data[39:43])[0]),
        "clutch": str(struct.unpack('<B', buffer_data[43:44])[0]),
        "gear": str(struct.unpack('<b', buffer_data[44:45])[0]),
        "rpm": str(struct.unpack('<H', buffer_data[45:47])[0]),
        "drs": str(struct.unpack('<B', buffer_data[47:48])[0]),
        "revLightPercentage": str(struct.unpack('<B', buffer_data[48:49])[0]),
        "revLightBitValue": str(struct.unpack('<H', buffer_data[49:51])[0]),
        "brakeTemp1": str(struct.unpack('<H', buffer_data[51:53])[0]),
        "brakeTemp2": str(struct.unpack('<H', buffer_data[53:55])[0]),
        "brakeTemp3": str(struct.unpack('<H', buffer_data[55:57])[0]),
        "brakeTemp4": str(struct.unpack('<H', buffer_data[57:59])[0]),
        "surfaceTyreTemp1": str(struct.unpack('<B', buffer_data[59:60])[0]),
        "surfaceTyreTemp2": str(struct.unpack('<B', buffer_data[60:61])[0]),
        "surfaceTyreTemp3": str(struct.unpack('<B', buffer_data[61:62])[0]),
        "surfaceTyreTemp4": str(struct.unpack('<B', buffer_data[63:64])[0]),
        "innerTyreTemp1": str(struct.unpack('<B', buffer_data[64:65])[0]),
        "innerTyreTemp2": str(struct.unpack('<B', buffer_data[65:66])[0]),
        "innerTyreTemp3": str(struct.unpack('<B', buffer_data[66:67])[0]),
        "innerTyreTemp4": str(struct.unpack('<B', buffer_data[67:68])[0]),
        "engineTemp": str(struct.unpack('<H', buffer_data[68:70])[0]),
        "tyrePressure1": str(struct.unpack('<f', buffer_data[70:74])[0]),
        "tyrePressure2": str(struct.unpack('<f', buffer_data[74:78])[0]),
        "tyrePressure3": str(struct.unpack('<f', buffer_data[78:82])[0]),
        "tyrePressure4": str(struct.unpack('<f', buffer_data[82:86])[0]),
        "surfaceWheel1": str(struct.unpack('<B', buffer_data[86:87])[0]),
        "surfaceWheel2": str(struct.unpack('<B', buffer_data[87:88])[0]),
        "surfaceWheel3": str(struct.unpack('<B', buffer_data[88:89])[0]),
        "surfaceWheel4": str(struct.unpack('<B', buffer_data[89:90])[0]),
    }

# COMMAND ----------

telemetry_udf = udf(decode_telemetry_data, MapType(StringType(), StringType()))
telemetry_unpacked = telemetry.withColumn("unpacked", telemetry_udf("string"))
telemetry_unpacked .createOrReplaceTempView("telemetry_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM telemetry_data TABLESAMPLE (5 ROWS);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW f1_telemetry_data AS
# MAGIC SELECT
# MAGIC   unpacked['pFormat'] AS pFormat,
# MAGIC   unpacked['gameYear'] AS gameYear,
# MAGIC   unpacked['gameMVersion'] AS gameMVersion,
# MAGIC   unpacked['gameMinorVersion'] AS gameMinorVersion,
# MAGIC   unpacked['packetVersion'] AS packetVersion,
# MAGIC   unpacked['packetId'] AS packetId,
# MAGIC   unpacked['sessionUID'] AS sessionUID,
# MAGIC   unpacked['sessionTimestamp'] AS sessionTimestamp,
# MAGIC   unpacked['frameID'] AS frameID,
# MAGIC   unpacked['overallFrameID'] AS overallFrameID,
# MAGIC   unpacked['playerCarIdenx'] AS playerCarIdenx,
# MAGIC   unpacked['secondaryPlayerIndex'] AS secondaryPlayerIndex,
# MAGIC   unpacked['speed'] AS speed,
# MAGIC   unpacked['throttle'] AS throttle,
# MAGIC   unpacked['steer'] AS steer,
# MAGIC   unpacked['brake'] AS brake,
# MAGIC   unpacked['clutch'] AS clutch,
# MAGIC   unpacked['gear'] AS gear,
# MAGIC   unpacked['rpm'] AS rpm,
# MAGIC   unpacked['drs'] AS drs,
# MAGIC   unpacked['revLightPercentage'] AS revLightPercentage,
# MAGIC   unpacked['revLightBitValue'] AS revLightBitValue,
# MAGIC   unpacked['brakeTemp1'] AS brakeTemp1,
# MAGIC   unpacked['brakeTemp2'] AS brakeTemp2,
# MAGIC   unpacked['brakeTemp3'] AS brakeTemp3,
# MAGIC   unpacked['brakeTemp4'] AS brakeTemp4,
# MAGIC   unpacked['surfaceTyreTemp1'] AS surfaceTyreTemp1,
# MAGIC   unpacked['surfaceTyreTemp2'] AS surfaceTyreTemp2,
# MAGIC   unpacked['surfaceTyreTemp3'] AS surfaceTyreTemp3,
# MAGIC   unpacked['surfaceTyreTemp4'] AS surfaceTyreTemp4,
# MAGIC   unpacked['innerTyreTemp1'] AS innerTyreTemp1,
# MAGIC   unpacked['innerTyreTemp2'] AS innerTyreTemp2,
# MAGIC   unpacked['innerTyreTemp3'] AS innerTyreTemp3,
# MAGIC   unpacked['innerTyreTemp4'] AS innerTyreTemp4,
# MAGIC   unpacked['engineTemp'] AS engineTemp,
# MAGIC   unpacked['tyrePressure1'] AS tyrePressure1,
# MAGIC   unpacked['tyrePressure2'] AS tyrePressure2,
# MAGIC   unpacked['tyrePressure3'] AS tyrePressure3,
# MAGIC   unpacked['tyrePressure4'] AS tyrePressure4,
# MAGIC   unpacked['surfaceWheel1'] AS surfaceWheel1,
# MAGIC   unpacked['surfaceWheel2'] AS surfaceWheel2,
# MAGIC   unpacked['surfaceWheel3'] AS surfaceWheel3,
# MAGIC   unpacked['surfaceWheel4'] AS surfaceWheel4
# MAGIC FROM
# MAGIC   telemetry_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(sessionUID) AS sesID, COUNT(1) FROM f1_telemetry_data GROUP BY sesID;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unpack lap data

# COMMAND ----------

_laps = spark.sql("SELECT * FROM data_headers WHERE packetId = '2' ").cache()

# COMMAND ----------

def decode_lap_data(byte_string):
    buffer_data = base64.b64decode(byte_string)
    return {
        "pFormat": str(struct.unpack('<H', buffer_data[:2])[0]),
        "gameYear": str(struct.unpack('<B', buffer_data[2:3])[0]),
        "gameMVersion": str(struct.unpack('<B', buffer_data[3:4])[0]),
        "gameMinorVersion": str(struct.unpack('<B', buffer_data[4:5])[0]),
        "packetVersion": str(struct.unpack('<B', buffer_data[5:6])[0]),
        "packetId": str(struct.unpack('<B', buffer_data[6:7])[0]),
        "sessionUID": str(struct.unpack('<Q', buffer_data[7:15])[0]),
        "sessionTimestamp": str(struct.unpack('<f', buffer_data[15:19])[0]),
        "frameID": str(struct.unpack('<L', buffer_data[19:23])[0]),
        "overallFrameID": str(struct.unpack('<L', buffer_data[23:27])[0]),
        "playerCarIdenx": str(struct.unpack('<B', buffer_data[27:28])[0]),
        "secondaryPlayerIndex": str(struct.unpack('<B', buffer_data[28:29])[0]),
    

        # LAP DATA
        "lastLapTime": str(struct.unpack('<L', buffer_data[29:33])[0]), 
        "currentLapTimeInMs": str(struct.unpack('<L', buffer_data[33:37])[0]),
        "sectorOneTimeMs": str(struct.unpack('<H', buffer_data[37:39])[0]),
        "sectorOneTimeSc": str(struct.unpack('<B', buffer_data[39:40])[0]),
        "sectorTwoTimeMs": str(struct.unpack('<H', buffer_data[40:42])[0]),
        "sectorTwoTimeSc": str(struct.unpack('<B', buffer_data[42:43])[0]),
        "deltaToFrontCarInMs": str(struct.unpack('<H', buffer_data[43:45])[0]),
        "deltaToRaceLeaderMs": str(struct.unpack('<H', buffer_data[45:47])[0]),
        "lapDistanceMeters": str(struct.unpack('<f', buffer_data[47:51])[0]),
        "totalDistance": str(struct.unpack('<f', buffer_data[51:55])[0]),
        "safetyCarDelta": str(struct.unpack('<f', buffer_data[55:59])[0]),
        "carPosition": str(struct.unpack('<B', buffer_data[59:60])[0]),
        "currentLapNum": str(struct.unpack('<B', buffer_data[60:61])[0]),
        "pitStatus": str(struct.unpack('<B', buffer_data[61:62])[0]),
        "numPitStops": str(struct.unpack('<B', buffer_data[62:63])[0]),
        "sector": str(struct.unpack('<B', buffer_data[63:64])[0]),
        "invalidLap": str(struct.unpack('<B', buffer_data[64:65])[0]),
        "penalties": str(struct.unpack('<B', buffer_data[65:66])[0]),
        "totalWarnings": str(struct.unpack('<B', buffer_data[66:67])[0]),
        "cornerCuttingWarnings": str(struct.unpack('<B', buffer_data[67:68])[0]),
        "numDriveThrough": str(struct.unpack('<B', buffer_data[68:69])[0]),
        "stopGoPens": str(struct.unpack('<B', buffer_data[69:70])[0]),
        "gridPosition": str(struct.unpack('<B', buffer_data[70:71])[0]),
        "driverStatus": str(struct.unpack('<B', buffer_data[71:72])[0]),
        "resultsStatus": str(struct.unpack('<B', buffer_data[72:73])[0]),
        "pitLaneTimerActive": str(struct.unpack('<B', buffer_data[73:74])[0]),
        "pitLaneTimeInMs": str(struct.unpack('<H', buffer_data[74:76])[0]),
        "pitLaneStopInMs": str(struct.unpack('<H', buffer_data[76:78])[0]),
        "pitStoShoulServePenalty":  str(struct.unpack('<B', buffer_data[78:79])[0])
    }

# COMMAND ----------

lap_udf = udf(decode_lap_data, MapType(StringType(), StringType()))
lap_unpacked = _laps.withColumn("unpacked", lap_udf("string"))
lap_unpacked.createOrReplaceTempView("lap_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW f1_laps_data AS
# MAGIC SELECT
# MAGIC   unpacked['pFormat'] AS pFormat,
# MAGIC   unpacked['gameYear'] AS gameYear,
# MAGIC   unpacked['gameMVersion'] AS gameMVersion,
# MAGIC   unpacked['gameMinorVersion'] AS gameMinorVersion,
# MAGIC   unpacked['packetVersion'] AS packetVersion,
# MAGIC   unpacked['packetId'] AS packetId,
# MAGIC   unpacked['sessionUID'] AS sessionUID,
# MAGIC   unpacked['sessionTimestamp'] AS sessionTimestamp,
# MAGIC   unpacked['frameID'] AS frameID,
# MAGIC   unpacked['overallFrameID'] AS overallFrameID,
# MAGIC   unpacked['playerCarIdenx'] AS playerCarIdenx,
# MAGIC   unpacked['secondaryPlayerIndex'] AS secondaryPlayerIndex,
# MAGIC   unpacked['lastLapTime'] AS lastLapTime,
# MAGIC   unpacked['currentLapTimeInMs'] AS currentLapTimeInMs,
# MAGIC   unpacked['sectorOneTimeMs'] AS sectorOneTimeMs,
# MAGIC   unpacked['sectorOneTimeSc'] AS sectorOneTimeSc,
# MAGIC   unpacked['sectorTwoTimeMs'] AS sectorTwoTimeMs,
# MAGIC   unpacked['sectorTwoTimeSc'] AS sectorTwoTimeSc,
# MAGIC   unpacked['deltaToFrontCarInMs'] AS deltaToFrontCarInMs,
# MAGIC   unpacked['deltaToRaceLeaderMs'] AS deltaToRaceLeaderMs,
# MAGIC   unpacked['lapDistanceMeters'] AS lapDistanceMeters,
# MAGIC   unpacked['totalDistance'] AS totalDistance,
# MAGIC   unpacked['safetyCarDelta'] AS safetyCarDelta,
# MAGIC   unpacked['carPosition'] AS carPosition,
# MAGIC   unpacked['currentLapNum'] AS currentLapNum,
# MAGIC   unpacked['pitStatus'] AS pitStatus,
# MAGIC   unpacked['numPitStops'] AS numPitStops,
# MAGIC   unpacked['sector'] AS sector,
# MAGIC   unpacked['invalidLap'] AS invalidLap,
# MAGIC   unpacked['penalties'] AS penalties,
# MAGIC   unpacked['totalWarnings'] AS totalWarnings,
# MAGIC   unpacked['cornerCuttingWarnings'] AS cornerCuttingWarnings,
# MAGIC   unpacked['numDriveThrough'] AS numDriveThrough,
# MAGIC   unpacked['stopGoPens'] AS stopGoPens,
# MAGIC   unpacked['gridPosition'] AS gridPosition,
# MAGIC   unpacked['driverStatus'] AS driverStatus,
# MAGIC   unpacked['resultsStatus'] AS resultsStatus,
# MAGIC   unpacked['pitLaneTimerActive'] AS pitLaneTimerActive,
# MAGIC   unpacked['pitLaneTimeInMs'] AS pitLaneTimeInMs,
# MAGIC   unpacked['pitLaneStopInMs'] AS pitLaneStopInMs,
# MAGIC   unpacked['pitStoShoulServePenalty'] AS pitStoShoulServePenalty
# MAGIC FROM lap_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(sessionUID) FROM lap_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DISTINCT(f1_telemetry_data.sessionUID)
# MAGIC FROM f1_laps_data
# MAGIC INNER JOIN f1_telemetry_data
# MAGIC ON f1_laps_data.sessionUID = f1_telemetry_data.sessionUID AND f1_laps_data.frameID = f1_telemetry_data.frameID
# MAGIC WHERE CAST(f1_laps_data.lapDistanceMeters AS FLOAT) >= 0
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS LAP_VS_TELEMETRY;
# MAGIC
# MAGIC CREATE TABLE LAP_VS_TELEMETRY
# MAGIC USING delta
# MAGIC LOCATION '/mnt/silver/LAP_VS_TELEMETRY/' AS (
# MAGIC
# MAGIC   SELECT
# MAGIC     f1_telemetry_data.*,
# MAGIC     FROM_UNIXTIME(CAST(f1_telemetry_data.sessionTimestamp AS FLOAT), 'HH:mm:ss:SSS') AS sessionTime,
# MAGIC     f1_laps_data.lastLapTime,
# MAGIC     f1_laps_data.currentLapTimeInMs,
# MAGIC     f1_laps_data.sectorOneTimeMs,
# MAGIC     f1_laps_data.sectorOneTimeSc,
# MAGIC     f1_laps_data.sectorTwoTimeMs,
# MAGIC     f1_laps_data.sectorTwoTimeSc,
# MAGIC     f1_laps_data.lapDistanceMeters,
# MAGIC     f1_laps_data.totalDistance,
# MAGIC     f1_laps_data.currentLapNum,
# MAGIC     f1_laps_data.sector
# MAGIC   FROM f1_laps_data
# MAGIC   INNER JOIN f1_telemetry_data
# MAGIC   ON f1_laps_data.sessionUID = f1_telemetry_data.sessionUID AND f1_laps_data.frameID = f1_telemetry_data.frameID
# MAGIC   WHERE CAST(f1_laps_data.lapDistanceMeters AS FLOAT) >= 0
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sessionUID, count(DISTINCT(currentLapNum)) as count FROM lap_vs_telemetry GROUP BY sessionUID;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW featureEngineering AS
# MAGIC (
# MAGIC   SELECT
# MAGIC     sessionUID,
# MAGIC     CAST(speed AS INT), 
# MAGIC     (CAST(brake AS FLOAT) * 100) AS brake,
# MAGIC     (CAST(throttle AS FLOAT) * 100) AS throttle,
# MAGIC     FROM_UNIXTIME(CAST(sessionTimestamp AS FLOAT), 'HH:mm:ss:SSS') AS sessionTime,
# MAGIC     CAST(currentLapNum AS INT) AS lapNumber,
# MAGIC     CAST(currentLapTimeInMs AS FLOAT) / 1000.0 AS lapTimeSec,
# MAGIC     (CAST(lapDistanceMeters AS FLOAT) / 5793) * 100 AS percentageCompleted
# MAGIC   FROM LAP_VS_TELEMETRY WHERE CAST(lapDistanceMeters AS FLOAT) >= 0
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC   --date_format(from_unixtime(lapTimeSec), 'HH:mm:ss.SSSS') AS lapTime,
# MAGIC   --LPAD(CAST((lapTimeSec - FLOOR(lapTimeSec)) * 1000 AS INT), 3, '0') AS lapMS,

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC
# MAGIC   sessionUID,
# MAGIC   lapNumber,
# MAGIC
# MAGIC   date_format(concat(
# MAGIC     date_format(from_unixtime(lapTimeSec), 'HH:mm:ss'),
# MAGIC     '.',
# MAGIC     LPAD(CAST((lapTimeSec - FLOOR(lapTimeSec)) * 1000 AS INT), 3, '0')
# MAGIC     ), 'HH:mm:ss.SSS') AS lap_time, 
# MAGIC
# MAGIC   lapDis
# MAGIC
# MAGIC FROM featureEngineering GROUP BY date_format(lap_time, 'HH:mm:ss')
# MAGIC ORDER BY sessionUID, lapNumber, lap_time

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from goldData

# COMMAND ----------


