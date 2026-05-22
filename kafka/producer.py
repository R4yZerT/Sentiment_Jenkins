import csv
import json
import os
import signal
import sys
import time

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "heart-records")
CSV_PATH = os.environ.get("CSV_PATH", "/data/heart.csv")
DELAY_SECONDS = float(os.environ.get("DELAY_SECONDS", "1.0"))

running = True


def handle_signal(signum, frame):
    global running
    print(f"\n[producer] Señal {signum} recibida, cerrando...")
    running = False


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def main():
    print(f"[producer] Conectando a Kafka: {KAFKA_BOOTSTRAP}")
    print(f"[producer] Topic: {KAFKA_TOPIC}")
    print(f"[producer] CSV: {CSV_PATH}")
    print(f"[producer] Delay: {DELAY_SECONDS}s")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3,
        retry_backoff_ms=1000,
    )

    sent = 0
    try:
        with open(CSV_PATH, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not running:
                    break

                for key, val in row.items():
                    try:
                        row[key] = int(val)
                    except (ValueError, TypeError):
                        try:
                            row[key] = float(val)
                        except (ValueError, TypeError):
                            pass

                producer.send(KAFKA_TOPIC, value=row)
                sent += 1

                if sent % 50 == 0:
                    print(f"[producer] {sent} mensajes enviados...")

                time.sleep(DELAY_SECONDS)

    except FileNotFoundError:
        print(f"[producer] ERROR: No se encontró {CSV_PATH}")
        sys.exit(1)
    finally:
        print(f"[producer] Flush final...")
        producer.flush(timeout=10)
        producer.close(timeout=10)
        print(f"[producer] Total enviado: {sent} mensajes")


if __name__ == "__main__":
    main()