import json
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import pandas as pd
import xgboost as xgb
from confluent_kafka import Consumer, Producer

MODEL_PATH = "models/xgb_fraud.json"
MERCHANTS = ["atm", "electronics", "fuel", "grocery", "online_shop", "restaurant", "travel"]
FEATURES = ["amount", "amount_ratio", "tx_1h", "is_foreign"] + [
    f"merchant_category_{m}" for m in MERCHANTS
]
THRESHOLD = 0.5

# État par carte : nombre de transactions, somme des montants, horodatages de la dernière heure
state = defaultdict(lambda: {"n": 0, "sum": 0.0, "ts": deque()})


def build_features(tx):
    s = state[tx["card_id"]]
    ts = int(datetime.fromisoformat(tx["event_time"]).timestamp())

    while s["ts"] and s["ts"][0] < ts - 3600:
        s["ts"].popleft()
    tx_1h = sum(1 for t in s["ts"] if t <= ts - 1)
    amount_ratio = 1.0 if s["n"] == 0 else tx["amount"] / (s["sum"] / s["n"])

    row = {
        "amount": tx["amount"],
        "amount_ratio": amount_ratio,
        "tx_1h": tx_1h,
        "is_foreign": int(tx["country"] != tx["home_country"]),
    }
    for m in MERCHANTS:
        row[f"merchant_category_{m}"] = int(tx["merchant_category"] == m)

    # Mise à jour de l'état APRÈS le calcul : les features n'utilisent que le passé
    s["n"] += 1
    s["sum"] += tx["amount"]
    s["ts"].append(ts)
    return row


model = xgb.XGBClassifier()
model.load_model(MODEL_PATH)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "fraud-scorer",
    "auto.offset.reset": "latest",
})
consumer.subscribe(["transactions"])
producer = Producer({"bootstrap.servers": "localhost:9092", "linger.ms": 20})

total = tp = fp = fn = 0
lat_sum = 0.0
last_print = time.time()
print("Scorer prêt, en attente de transactions...")

try:
    while True:
        msgs = consumer.consume(num_messages=500, timeout=1.0)
        txs, rows = [], []
        for m in msgs:
            if m.error():
                continue
            tx = json.loads(m.value())
            if "event_time" not in tx or "country" not in tx:
                continue
            txs.append(tx)
            rows.append(build_features(tx))

        if rows:
            scores = model.predict_proba(pd.DataFrame(rows, columns=FEATURES))[:, 1]
            now = datetime.now(timezone.utc)
            for tx, sc in zip(txs, scores):
                flagged = sc >= THRESHOLD
                latency_ms = (now - datetime.fromisoformat(tx["event_time"])).total_seconds() * 1000
                lat_sum += latency_ms
                total += 1
                if flagged and tx["is_fraud"]:
                    tp += 1
                elif flagged:
                    fp += 1
                elif tx["is_fraud"]:
                    fn += 1
                if flagged:
                    alert = {
                        "transaction_id": tx["transaction_id"],
                        "card_id": tx["card_id"],
                        "amount": tx["amount"],
                        "merchant_category": tx["merchant_category"],
                        "country": tx["country"],
                        "home_country": tx["home_country"],
                        "score": round(float(sc), 4),
                        "event_time": tx["event_time"],
                        "latency_ms": round(latency_ms, 1),
                    }
                    producer.produce("fraud_alerts", key=tx["card_id"], value=json.dumps(alert))
            producer.poll(0)

        if time.time() - last_print >= 5 and total:
            print(
                f"traitées={total} alertes={tp + fp} vraies_fraudes_détectées={tp} "
                f"fausses_alertes={fp} fraudes_manquées={fn} "
                f"latence_moy={lat_sum / total:.0f}ms"
            )
            last_print = time.time()
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    consumer.close()