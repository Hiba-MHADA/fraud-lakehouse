import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

COUNTRIES = ["MA", "FR", "ES", "DE", "US", "GB", "IT", "AE"]
MERCHANTS = ["grocery", "fuel", "restaurant", "electronics", "travel", "online_shop", "atm"]
N_CARDS = 5000
parser = argparse.ArgumentParser()
parser.add_argument("--rate", type=int, default=100, help="événements par seconde")
parser.add_argument("--duration", type=int, default=10, help="durée en secondes (0 = infini)")
parser.add_argument("--fraud-rate", type=float, default=0.01, help="proportion de fraude")
args = parser.parse_args()

RATE = args.rate
DURATION = args.duration
FRAUD_RATE = args.fraud_rate

# Chaque carte a un pays d'origine et un montant habituel
cards = {
    f"card_{i}": {
        "home": random.choice(COUNTRIES),
        "avg": random.lognormvariate(3.5, 0.6),
    }
    for i in range(N_CARDS)
}
card_ids = list(cards)


def make_transaction():
    card_id = random.choice(card_ids)
    profile = cards[card_id]
    is_fraud = random.random() < FRAUD_RATE

    if is_fraud:
        # Comportement anormal : gros montant, pays étranger, commerçant à risque
        amount = profile["avg"] * random.uniform(5, 30)
        country = random.choice([c for c in COUNTRIES if c != profile["home"]])
        merchant = random.choice(["electronics", "online_shop", "travel", "atm"])
    else:
        # Comportement normal, avec 5 % de voyages à l'étranger
        amount = random.lognormvariate(0, 0.5) * profile["avg"]
        country = profile["home"] if random.random() < 0.95 else random.choice(COUNTRIES)
        merchant = random.choice(MERCHANTS)

    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card_id,
        "amount": round(amount, 2),
        "merchant_category": merchant,
        "country": country,
        "home_country": profile["home"],
        "event_time": datetime.now(timezone.utc).isoformat(),
        "is_fraud": int(is_fraud),
    }


producer = Producer({"bootstrap.servers": "localhost:9092"})

sent = 0
start = time.time()
while DURATION == 0 or time.time() - start < DURATION:
    loop_start = time.time()
    for _ in range(RATE):
        tx = make_transaction()
        producer.produce("transactions", key=tx["card_id"], value=json.dumps(tx))
        producer.poll(0)
    sent += RATE
    print(f"{sent} événements envoyés ({sent / (time.time() - start):.0f}/s)")
    time.sleep(max(0, 1 - (time.time() - loop_start)))

producer.flush()
elapsed = time.time() - start
print(f"Terminé : {sent} événements en {elapsed:.1f}s ({sent / elapsed:.0f}/s)")