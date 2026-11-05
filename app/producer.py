"""Simulates IoT streams: traffic, health (wearables) and environment (AQI) sensors.
Every ~2 min a 'pollution episode' hits one zone: PM2.5 and congestion rise there,
and abnormal health readings increase -> the cross-domain insight for the report."""
import json, os, random, time
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOT = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
ZONES = ["Zone-A", "Zone-B", "Zone-C", "Zone-D"]
SEGMENTS = {f"SEG-{i:02d}": ZONES[i % 4] for i in range(8)}
PATIENTS = {f"P{i:03d}": random.choice(ZONES) for i in range(40)}

while True:
    try:
        prod = KafkaProducer(bootstrap_servers=BOOT,
                             value_serializer=lambda v: json.dumps(v).encode())
        break
    except Exception as e:
        print("waiting for kafka...", e); time.sleep(3)

def now(): return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

episode_zone, episode_end, next_episode = None, 0, time.time() + 45
print("producer started")
while True:
    t = time.time()
    if episode_zone and t > episode_end:
        episode_zone = None; next_episode = t + 60
    if not episode_zone and t > next_episode:
        episode_zone = random.choice(ZONES); episode_end = t + 60
        print("EPISODE in", episode_zone)

    for z in ZONES:
        bad = z == episode_zone
        pm = random.uniform(90, 140) if bad else random.uniform(20, 50)
        prod.send("env", {"zone": z, "pm25": round(pm, 1), "ts": now()})

    for seg, z in SEGMENTS.items():
        bad = z == episode_zone
        speed = random.uniform(8, 18) if bad else random.uniform(30, 55)
        prod.send("traffic", {"segment": seg, "zone": z, "speed": round(speed, 1),
                              "vehicles": random.randint(40, 200), "ts": now()})

    for p in random.sample(list(PATIENTS), 10):
        z = PATIENTS[p]
        abnormal = random.random() < (0.35 if z == episode_zone else 0.03)
        hr = random.randint(125, 160) if abnormal else random.randint(60, 100)
        spo2 = random.randint(85, 91) if abnormal else random.randint(95, 99)
        prod.send("health", {"patient_id": p, "zone": z, "heart_rate": hr,
                             "spo2": spo2, "ts": now()})
    prod.flush()
    time.sleep(1)
