import json, os, time
import pandas as pd, streamlit as st
from kafka import KafkaConsumer

BOOT = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
st.set_page_config(page_title="Smart City Health & Environment Monitor", layout="wide")
st.title("🏙️ Smart City Health & Environment Monitor — live")

@st.cache_resource
def get_consumer():
    while True:
        try:
            return KafkaConsumer("city_metrics", bootstrap_servers=BOOT, auto_offset_reset="latest",
                                 value_deserializer=lambda b: json.loads(b))
        except Exception:
            time.sleep(3)

c = get_consumer()
rows = st.session_state.setdefault("rows", [])
for _, recs in c.poll(timeout_ms=1500, max_records=500).items():
    rows.extend(r.value for r in recs)
del rows[:-4000]

if not rows:
    st.info("Waiting for data… (Spark needs ~1-2 minutes after startup)"); time.sleep(2); st.rerun()

df = pd.DataFrame(rows); df["window_end"] = pd.to_datetime(df["window_end"])
latest = df.sort_values("window_end").groupby(["domain", "key"]).tail(1)
alerts = latest[latest["alert"]]

cols = st.columns(3)
titles = {"traffic": ("🚦 Avg speed (km/h) — alert < 20", "Congested segments"),
          "environment": ("🌫️ PM2.5 (µg/m³) — alert > 60", "Polluted zones"),
          "health": ("❤️ Abnormal vitals / 30s — alert ≥ 3", "Zones with alerts")}
for col, dom in zip(cols, ["traffic", "environment", "health"]):
    d = df[df.domain == dom]
    col.subheader(titles[dom][0])
    col.metric(titles[dom][1], int(alerts[alerts.domain == dom].shape[0]))
    if not d.empty:
        col.line_chart(d.pivot_table(index="window_end", columns="key", values="value", aggfunc="last").tail(60))

st.subheader("🚨 Active alerts")
st.dataframe(alerts[["domain", "key", "value", "window_end"]].sort_values("domain"), use_container_width=True)
time.sleep(1); st.rerun()
