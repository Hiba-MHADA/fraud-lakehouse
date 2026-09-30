# Fraud Lakehouse

Plateforme de détection de fraude en temps réel (big data).

## Stack
Kafka, MinIO, Spark Structured Streaming, Delta Lake, MLflow, Docker

## Démarrage
docker compose up -d

## Avancement
- [x] Environnement, Git, Docker
- [x] Kafka
- [x] Générateur de transactions
- [ ] Stockage (à choisir, MinIO n'est plus disponible)
- [ ] Streaming Spark + Delta Lake
- [ ] Modèle ML
- [ ] Dashboard Streamlit
## Benchmarks (phase 1)

Machine : PC portable, 8 Go de RAM, Docker limité à 4 Go, 2 processeurs.

| Débit demandé | Débit réel |
|---|---|
| 500/s | 500/s |
| 5 000/s | 4 946/s |
| 20 000/s | 4 341/s |

Le générateur Python mono-processus plafonne à environ 4 300-5 000 événements/s.
Le goulot d'étranglement est le générateur, pas Kafka.

## Choix techniques
- 3 partitions pour le topic `transactions`, avec `card_id` comme clé de message :
  toutes les transactions d'une même carte restent dans l'ordre.
- 1 % de fraude injectée, avec 5 % de voyages légitimes à l'étranger,
  pour que le pays seul ne suffise pas à détecter la fraude.
