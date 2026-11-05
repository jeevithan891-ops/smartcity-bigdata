# Report skeleton (fill in screenshots + your numbers)
1. Introduction & problem statement — smart city + healthcare + environment; why combine them.
2. Why Big Data / Hadoop — 3Vs: 20+ msgs/s per simulated stream scales to millions of sensor events.
3. Architecture — insert diagram: Producer → Kafka → Spark Streaming → HDFS / Kafka → Dashboard; Spark SQL batch layer.
   Note: HDFS+MapReduce are batch; "real-time" comes from Kafka + Spark Streaming (speed layer).
4. Tool roles — Kafka (ingestion), HDFS (storage), Spark (stream+batch), YARN (resource mgr; here Spark runs local mode, on a cluster use --master yarn),
   Hive (same SQL as batch_analytics.py via external table on /lake), ZooKeeper (Kafka coordination in older versions; KRaft here), Streamlit (visualisation).
5. Implementation — producer.py, stream_job.py (windowed aggregation, watermark, alert rules), batch_analytics.py.
6. Results — screenshots: dashboard, HDFS /lake browser, Spark UI, batch output tables, correlation value.
7. Cross-domain insight — in pollution episodes, PM2.5 > 90, traffic speed < 20 and abnormal vitals rise in the same zone.
8. Challenges — latency vs. watermark, data quality, privacy (DPDP Act / HIPAA: anonymise patient IDs), scalability.
9. Conclusion & future scope — edge computing, ML (Spark MLlib forecasting), cloud migration.
