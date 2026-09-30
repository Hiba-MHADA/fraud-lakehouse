# Fraud Lakehouse

Plateforme de détection de fraude en temps réel (big data).

## Stack
Kafka (KRaft), Spark Structured Streaming, Delta Lake, Docker, Python 3.12

## Architecture
Générateur Python -> Kafka (topic `transactions`) -> Spark -> Delta bronze -> Delta silver

## Démarrage
docker compose up -d

## Avancement
- [x] Environnement, Git, Docker
- [x] Kafka
- [x] Générateur de transactions
- [x] Couche bronze (Kafka vers Delta Lake)
- [x] Couche silver (JSON décodé, typé, nettoyé)
- [x] Couche gold (features par carte)
- [ ] Modèle ML
- [ ] Dashboard Streamlit

## Benchmarks

Machine : PC portable, 8 Go de RAM, Docker limité à 4 Go, 2 processeurs.

| Test | Résultat |
|---|---|
| Générateur, 500/s demandés | 500/s |
| Générateur, 5 000/s demandés | 4 946/s |
| Générateur, 20 000/s demandés | 4 341/s |
| Bronze, rattrapage de l'arriéré | environ 1 270 lignes/s |

Le générateur Python mono-processus plafonne à environ 4 300-5 000 événements/s.
Le goulot d'étranglement est le générateur, pas Kafka.

## Vérifications de qualité
- Bronze : 229 501 lignes, somme des offsets des 3 partitions identique à Kafka
  (aucune perte, aucun doublon).
- Silver : 229 500 lignes (le message de test sans `is_fraud` est écarté),
  dont 2 272 fraudes (0,99 %).

## Choix techniques
- 3 partitions, avec `card_id` comme clé de message : les transactions d'une même
  carte restent dans l'ordre.
- 1 % de fraude injectée, avec 5 % de voyages légitimes à l'étranger, pour que le
  pays seul ne suffise pas à détecter la fraude.
- Bronze = données brutes inchangées (JSON en texte), ce qui permet de tout rejouer.
- Silver = schéma explicite, types corrects, lignes invalides filtrées.
- Silver en `availableNow` : le job traite tout ce qui est disponible puis
  s'arrête, ce qui économise la RAM.
- Stockage Delta Lake dans un dossier local : MinIO n'est plus distribué sur
  Docker Hub, et la RAM est limitée.
- Les features gold n'utilisent que l'historique passé de la carte (les fenêtres excluent la ligne courante), pour éviter toute fuite de données.
- Limite connue : `tx_1h` n'a pas de pouvoir discriminant avec le générateur actuel (pas de fraudes en rafale). Les fraudes générées sont aussi trop faciles à détecter (montant x15, toujours à l'étranger) : on les rendra plus ambiguës.