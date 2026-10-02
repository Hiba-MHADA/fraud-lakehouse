import argparse
import json
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Producer

parser = argparse.ArgumentParser()
parser.add_argument("--rate", type=int, default=200, help="événements par seconde")
parser.add_argument("--start", type=int, default=0, help="première ligne à rejouer")
parser.add_argument("--limit", type=int, default=0, help="nombre de lignes (0 = tout)")
parser.add_argument("--topic", default="transactions_real")
args = parser.parse_args()

df = pd.read_csv("data/raw/creditcard.csv").iloc[args.start:]
if args.limit:
    df = df.iloc[: args.limit]
records = df.to_dict("records")

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "linger.ms": 20,
    "compression.type": "lz4",
})

sent, t0 = 0, time.time()
for i in range(0, len(records), args.rate):
    loop = time.time()
    batch = records[i : i + args.rate]
    for j, r in enumerate(batch):
        row_id = args.start + i + j
        msg = {"row_id": row_id}
        msg.update({k: float(v) for k, v in r.items() if k != "Class"})
        msg["is_fraud"] = int(r["Class"])
        msg["event_time"] = datetime.now(timezone.utc).isoformat()
        producer.produce(args.topic, key=str(row_id), value=json.dumps(msg))
        producer.poll(0)
    sent += len(batch)
    print(f"{sent} lignes envoyées ({sent / (time.time() - t0):.0f}/s)")
    time.sleep(max(0, 1 - (time.time() - loop)))

producer.flush()
print(f"Terminé : {sent} lignes en {time.time() - t0:.1f}s")