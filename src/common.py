"""Shared config and Kafka helpers for the Reddit capstone pipeline."""
from __future__ import annotations

import json
import os
import signal
import time
from pathlib import Path
from typing import Callable

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Host-side bootstrap (Redpanda external listener from docker-compose.yml).
BOOTSTRAP = "localhost:19092"

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
FIGURES_DIR = ROOT / "figures"
STATS_FILE = ROOT / "stats.json"
DONE_FILE = DATA_DIR / ".stats_done"

# Linear enrichment topics: each stage reads the previous one and writes the next.
T_RAW = "comments.raw"
T_LANG = "comments.lang"
T_SENTIMENT = "comments.sentiment"
T_KEYWORDS = "comments.keywords"
ALL_TOPICS = [T_RAW, T_LANG, T_SENTIMENT, T_KEYWORDS]

PARTITIONS = 3
REPLICATION = 1

# Stages self-stop after this many idle seconds so a run drains on its own.
# Override with the IDLE_EXIT_S env var.
IDLE_EXIT_S = float(os.environ.get("IDLE_EXIT_S", "25"))


def admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def recreate_topics(topics=ALL_TOPICS, partitions=PARTITIONS, replication=REPLICATION,
                    timeout: float = 30.0) -> None:
    """Delete the given topics if present, then create them fresh."""
    a = admin()
    existing = a.list_topics(timeout=10).topics
    present = [t for t in topics if t in existing]
    if present:
        for _, fut in a.delete_topics(present, operation_timeout=20).items():
            try:
                fut.result(timeout=timeout)
            except Exception:
                pass
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not any(t in a.list_topics(timeout=10).topics for t in present):
                break
            time.sleep(0.5)

    new = [NewTopic(t, num_partitions=partitions, replication_factor=replication) for t in topics]
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        for _, fut in a.create_topics(new).items():
            try:
                fut.result(timeout=timeout)
                last_err = None
            except Exception as e:  # may still be mid-delete; retry
                last_err = e
        md = a.list_topics(timeout=10).topics
        if all(t in md and len(md[t].partitions) == partitions for t in topics):
            return
        time.sleep(0.5)
    raise RuntimeError(f"could not create topics {topics}: {last_err}")


def encode(obj: dict) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def decode(b: bytes) -> dict:
    return json.loads(b.decode("utf-8"))


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 10, "acks": "all"})


def make_consumer(group: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })


def consume_loop(name: str, topic: str, on_message: Callable[[object, int], None],
                 group: str | None = None,
                 on_drain: Callable[[int], None] | None = None) -> int:
    """Generic consumer loop with graceful shutdown, shared by every service.

    Polls `topic` and calls `on_message(msg, n)` per record (n = 1-based count). The
    loop self-stops on SIGTERM/SIGINT or after IDLE_EXIT_S of no input, then calls
    `on_drain(n)` (if given) before closing. Returns the number of messages handled.
    """
    running = {"on": True}

    def stop(*_):
        running["on"] = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer = make_consumer(group or f"{name}-group")
    consumer.subscribe([topic])

    n = 0
    last = time.time()
    while running["on"]:
        msg = consumer.poll(1.0)
        if msg is None:
            if n > 0 and time.time() - last > IDLE_EXIT_S:
                break
            continue
        if msg.error():
            continue
        n += 1
        last = time.time()
        on_message(msg, n)

    if on_drain:
        on_drain(n)
    consumer.close()
    return n


def run_enricher(name: str, in_topic: str, out_topic: str,
                 enrich: Callable[[dict], dict], group: str | None = None) -> None:
    """Consume -> enrich -> produce: read `in_topic`, add fields, write `out_topic`.

    `enrich` takes a decoded message dict and returns it with extra fields added.
    """
    producer = make_producer()

    def on_message(msg, n: int) -> None:
        rec = enrich(decode(msg.value()))
        producer.produce(out_topic, key=msg.key(), value=encode(rec))
        producer.poll(0)
        if n % 50 == 0:
            producer.flush(5)
            print(f"[{name}] processed {n}", flush=True)

    def on_drain(n: int) -> None:
        producer.flush(10)
        print(f"[{name}] done, {n} messages", flush=True)

    print(f"[{name}] up: {in_topic} -> {out_topic}", flush=True)
    consume_loop(name, in_topic, on_message, group=group, on_drain=on_drain)
