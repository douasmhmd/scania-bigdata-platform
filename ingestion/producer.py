"""
Producteur Kafka - couche d'ingestion SCANIA Component X.

Lit les releves operationnels (CSV), les trie selon une horloge de flotte
(time_step puis vehicle_id) et publie chaque releve comme un evenement JSON
dans Kafka, avec vehicle_id comme cle (ordre garanti pour chaque camion).
"""
import argparse
import json
import logging
import time
from datetime import datetime, timezone

import duckdb
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

SCHEMA_VERSION = 1  # version du contrat d'evenement


def parse_args():
    p = argparse.ArgumentParser(description="Rejoue les releves SCANIA dans Kafka")
    p.add_argument("--file", default="/data/train_operational_readouts.csv")
    p.add_argument("--topic", default="scania.readouts")
    p.add_argument("--bootstrap", default="kafka:29092")
    p.add_argument("--vehicles", type=int, default=0, help="nombre de camions (0 = tous)")
    p.add_argument("--limit", type=int, default=0, help="nombre max d'evenements (0 = tous)")
    p.add_argument("--rate", type=float, default=0, help="evenements par seconde (0 = maximum)")
    p.add_argument("--batch", type=int, default=5000, help="taille des lots lus par DuckDB")
    return p.parse_args()


def build_query(path, n_vehicles, limit):
    """Sous-ensemble de camions + tri selon l'horloge de flotte, sans fichier intermediaire."""
    src = f"read_csv_auto('{path}', header=true)"
    where = ""
    if n_vehicles > 0:
        where = (f"WHERE vehicle_id IN (SELECT DISTINCT vehicle_id FROM {src} "
                 f"ORDER BY vehicle_id LIMIT {n_vehicles})")
    lim = f"LIMIT {limit}" if limit > 0 else ""
    return f"SELECT * FROM {src} {where} ORDER BY time_step, vehicle_id {lim}"


def main():
    args = parse_args()

    # DuckDB trie hors memoire si besoin, dans /tmp du conteneur (jamais sur Windows)
    con = duckdb.connect()
    con.execute("SET memory_limit='1GB'")
    con.execute("SET temp_directory='/tmp/duckdb'")
    cursor = con.execute(build_query(args.file, args.vehicles, args.limit))
    feature_cols = [d[0] for d in cursor.description][2:]  # apres vehicle_id et time_step

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "client.id": "scania-replay-producer",
        "acks": "all",                    # accuse de reception complet du broker
        "enable.idempotence": True,       # aucun doublon en cas de renvoi
        "compression.type": "zstd",       # economise disque et reseau
        "linger.ms": 50,                  # regroupe les messages en lots
        "partitioner": "murmur2_random",  # meme hachage des cles que les clients Java
    })

    stats = {"sent": 0, "failed": 0}

    def on_delivery(err, msg):
        if err is not None:
            stats["failed"] += 1
            log.error("Echec de livraison : %s", err)

    start = time.time()
    while True:
        rows = cursor.fetchmany(args.batch)
        if not rows:
            break
        for row in rows:
            vehicle_id, time_step = int(row[0]), float(row[1])
            event = {
                "schema_version": SCHEMA_VERSION,
                "event_id": f"{vehicle_id}-{time_step}",
                "vehicle_id": vehicle_id,
                "time_step": time_step,  # temps relatif anonymise du dataset
                "produced_at": datetime.now(timezone.utc).isoformat(),
                "readings": {c: (None if v is None else float(v))
                             for c, v in zip(feature_cols, row[2:])},
            }
            payload = json.dumps(event).encode("utf-8")
            while True:
                try:
                    producer.produce(args.topic, key=str(vehicle_id).encode(),
                                     value=payload, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.5)  # file d'attente pleine : on laisse partir des messages
            producer.poll(0)
            stats["sent"] += 1
            if args.rate > 0:
                time.sleep(1.0 / args.rate)
        log.info("%d evenements publies", stats["sent"])

    producer.flush(60)
    elapsed = time.time() - start
    log.info("Termine : %d publies, %d echecs, %.1f s (%.0f ev/s)",
             stats["sent"], stats["failed"], elapsed, stats["sent"] / max(elapsed, 1e-9))


if __name__ == "__main__":
    main()