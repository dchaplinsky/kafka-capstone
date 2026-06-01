"""Generator: reads data/comments.jsonl and publishes each comment to comments.raw."""
from __future__ import annotations

import argparse
import json
import time

from src.common import DATA_DIR, T_RAW, encode, make_producer, recreate_topics


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=str(DATA_DIR / "comments.jsonl"))
    ap.add_argument("--limit", type=int, default=0, help="max messages (0 = all)")
    ap.add_argument("--recreate", action="store_true", help="recreate pipeline topics first")
    args = ap.parse_args()

    if args.recreate:
        recreate_topics()

    producer = make_producer()
    n = 0
    with open(args.file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            rec["send_ts"] = time.time()
            key = str(rec.get("id", n)).encode("utf-8")
            producer.produce(T_RAW, key=key, value=encode(rec))
            producer.poll(0)
            n += 1
            if n % 200 == 0:
                producer.flush(5)
                print(f"[generator] sent {n}", flush=True)
            if args.limit and n >= args.limit:
                break

    producer.flush(15)
    print(f"[generator] done, sent {n} messages to {T_RAW}", flush=True)


if __name__ == "__main__":
    main()
