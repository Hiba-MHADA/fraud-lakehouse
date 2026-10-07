import argparse
import json
import time
from datetime import datetime, timezone

import pandas as pd
import xgboost as xgb
from confluent_kafka import Consumer, Producer

ap = argparse.ArgumentParser()
ap.add_argument("--threshold", type=float, default=0.5)
args = ap.parse_args()

FEATURES = [f"V{i}" for i in range(1, 29)] + ["Amount"]
model = xgb.XGBClassifier()
model.load_model("models/xgb_real.json")

run_id = int(time.time())
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": f"fraud-scorer-real-{run_id}",
    "auto.offset.reset": "latest",
})
consumer.subscribe(["transactions_real"])
producer = Producer({"bootstrap.servers": "localhost:9092", "linger.ms": 20})

processed = tp = fp = fn = tn = 0
win_lat, win_n = 0.0, 0
last_stats = time.time()
print(f"Scorer réel prêt (seuil {args.threshold}), en attente de transactions...")

try:
    while True:
        msgs = consumer.consume(num_messages=500, timeout=1.0)
        txs = [json.loads(m.value()) for m in msgs if not m.error()]
        if txs:
            X = pd.DataFrame(txs, columns=FEATURES)
            scores = model.predict_proba(X)[:, 1]
            now = datetime.now(timezone.utc)
            for tx, sc in zip(txs, scores):
                flagged = sc >= args.threshold
                lat = (now - datetime.fromisoformat(tx["event_time"])).total_seconds() * 1000
                processed += 1
                win_lat += lat
                win_n += 1
                if flagged and tx["is_fraud"]:
                    tp += 1
                elif flagged:
                    fp += 1
                elif tx["is_fraud"]:
                    fn += 1
                else:
                    tn += 1
                if flagged:
                    producer.produce("fraud_alerts_real", key=str(tx["row_id"]), value=json.dumps({
                        "type": "alert", "run_id": run_id, "row_id": tx["row_id"],
                        "Amount": tx["Amount"], "score": round(float(sc), 4),
                        "is_fraud": tx["is_fraud"], "latency_ms": round(lat, 1),
                    }))
            producer.poll(0)

        if win_n and time.time() - last_stats >= 2:
            producer.produce("fraud_alerts_real", key="stats", value=json.dumps({
                "type": "stats", "run_id": run_id, "ts": time.time(), "processed": processed,
                "tp": tp, "fp": fp, "fn": fn, "tn": tn, "lat_win": round(win_lat / win_n, 1),
            }))
            producer.poll(0)
            win_lat, win_n, last_stats = 0.0, 0, time.time()
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    consumer.close()