# Smart City Health & Environment Monitor — Real-Time Big Data Analytics on Hadoop

Lambda architecture:  Producer (IoT simulator) → **Kafka** → **Spark Structured Streaming** (speed layer)
→ live metrics/alerts topic → **Streamlit dashboard**, and raw events → **HDFS** (Parquet data lake)
→ **Spark SQL** batch analytics (batch layer, Hive-style SQL).  YARN/ZooKeeper/Hive are discussed in the report (see REPORT.md).

## Requirements
Docker Desktop / Docker Engine with Compose v2, 8 GB RAM free, internet (first run downloads images + Spark Kafka jar).

## Run
```bash
docker compose up -d --build
docker compose logs -f spark-stream      # wait for "Streaming query made progress"
```
- Dashboard: http://localhost:8501   (data appears after ~1–2 min)
- HDFS UI:   http://localhost:9870   (Utilities → Browse → /lake)
- Spark UI:  http://localhost:4040

## Batch analytics (run after ~5 min of streaming)
```bash
docker compose --profile batch run --rm spark-batch
```
Prints alert counts per zone, congested segments, abnormal vitals, and the **PM2.5 vs abnormal-vitals correlation**.

## Stop
```bash
docker compose down -v
```

## Troubleshooting
- spark-stream restarts a few times at first while HDFS leaves safe mode — normal (restart: on-failure).
- Dashboard empty: `docker compose logs producer spark-stream`.
- Out of memory: Docker Desktop → Resources → 8 GB.
