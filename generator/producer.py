import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

COUNTRIES = ["MA", "FR", "ES", "DE", "US", "GB", "IT", "AE"]
MERCHANTS = ["grocery", "fuel", "restaurant", "electronics", "travel", "online_shop", "atm"]
N_CARDS = 5000
RATE = 100       # événements par seconde
DURATION = 10    # secondes

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
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card_id,
        "amount": round(random.lognormvariate(0, 0.5) * profile["avg"], 2),
        "merchant_category": random.choice(MERCHANTS),
        "country": profile["home"],
        "home_country": profile["home"],
        "event_time": datetime.now(timezone.utc).isoformat(),
        "is_fraud": 0,
    }


producer = Producer({"bootstrap.servers": "localhost:9092"})

sent = 0
start = time.time()
while time.time() - start < DURATION:
    loop_start = time.time()
    for _ in range(RATE):
        tx = make_transaction()
        producer.produce("transactions", key=tx["card_id"], value=json.dumps(tx))
        producer.poll(0)
    sent += RATE
    print(f"{sent} événements envoyés")
    time.sleep(max(0, 1 - (time.time() - loop_start)))

producer.flush()
print("Terminé")