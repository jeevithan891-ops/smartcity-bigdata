"""Batch layer: historical analytics on the HDFS data lake (Spark SQL; Hive-style queries)."""
from pyspark.sql import SparkSession
HDFS = "hdfs://namenode:9000"
spark = SparkSession.builder.appName("BatchAnalytics").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
for n in ["env", "traffic", "health"]:
    spark.read.parquet(f"{HDFS}/lake/{n}").createOrReplaceTempView(n)

print("\n=== 1. Pollution alert readings per zone (PM2.5 > 60) ===")
spark.sql("SELECT zone, COUNT(*) alert_readings, ROUND(MAX(pm25),1) max_pm25 FROM env WHERE pm25>60 GROUP BY zone ORDER BY alert_readings DESC").show()

print("=== 2. Congested readings per road segment (speed < 20) ===")
spark.sql("SELECT segment, zone, COUNT(*) congested_readings FROM traffic WHERE speed<20 GROUP BY segment, zone ORDER BY congested_readings DESC").show()

print("=== 3. Abnormal vitals per zone ===")
spark.sql("SELECT zone, COUNT(*) abnormal_readings FROM health WHERE heart_rate>120 OR spo2<92 GROUP BY zone ORDER BY abnormal_readings DESC").show()

spark.sql("""
CREATE OR REPLACE TEMP VIEW joined AS
WITH e AS (SELECT zone, window(ts,'1 minute') w, AVG(pm25) pm25 FROM env GROUP BY zone, window(ts,'1 minute')),
     h AS (SELECT zone, window(ts,'1 minute') w, SUM(CASE WHEN heart_rate>120 OR spo2<92 THEN 1 ELSE 0 END) abnormal FROM health GROUP BY zone, window(ts,'1 minute'))
SELECT e.zone, e.w.start minute, ROUND(e.pm25,1) pm25, h.abnormal FROM e JOIN h ON e.zone=h.zone AND e.w=h.w""")

print("=== 4. CROSS-DOMAIN: PM2.5 vs abnormal health readings (per zone-minute) ===")
spark.sql("SELECT * FROM joined ORDER BY minute, zone").show(40, False)
spark.sql("SELECT ROUND(corr(pm25, abnormal),3) AS correlation_pm25_vs_abnormal_vitals FROM joined").show()
spark.sql("SELECT CASE WHEN pm25>60 THEN 'High PM2.5 (>60)' ELSE 'Normal' END AS air_quality, ROUND(AVG(abnormal),2) avg_abnormal_per_minute, COUNT(*) samples FROM joined GROUP BY 1").show()
