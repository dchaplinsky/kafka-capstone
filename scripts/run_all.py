"""Run the whole pipeline end to end: recreate topics, start the enrichment and stats
services, fire the generator, wait for the statistics service to finalize, then stop.

Services consume from earliest, so the generator can run immediately; messages buffer
in the durable topics and are processed as each model finishes loading.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

from src.common import DONE_FILE, ROOT, STATS_FILE, recreate_topics

PY = sys.executable
SERVICES = [
    "src.lang_service",
    "src.sentiment_service",
    "src.keyword_service",
    "src.stats_service",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="max comments (0 = all)")
    ap.add_argument("--timeout", type=int, default=3600, help="overall wait, seconds")
    args = ap.parse_args()

    env = dict(os.environ)
    env.setdefault("HF_HOME", str(ROOT / ".hf_cache"))
    env.setdefault("TOKENIZERS_PARALLELISM", "false")

    DONE_FILE.unlink(missing_ok=True)
    print("[run_all] recreating topics ...", flush=True)
    recreate_topics()

    procs = []
    for m in SERVICES:
        print(f"[run_all] starting {m}", flush=True)
        procs.append(subprocess.Popen([PY, "-m", m], cwd=str(ROOT), env=env))

    gen = [PY, "-m", "src.generator"]
    if args.limit:
        gen += ["--limit", str(args.limit)]
    print("[run_all] running generator ...", flush=True)
    subprocess.run(gen, cwd=str(ROOT), env=env, check=True)

    print("[run_all] waiting for the pipeline to drain ...", flush=True)
    deadline = time.time() + args.timeout
    while time.time() < deadline and not DONE_FILE.exists():
        time.sleep(2)

    for p in procs:
        if p.poll() is None:
            p.terminate()
    for p in procs:
        try:
            p.wait(timeout=20)
        except subprocess.TimeoutExpired:
            p.kill()

    if STATS_FILE.exists():
        print("\n[run_all] final stats:\n" + STATS_FILE.read_text(), flush=True)
    print("[run_all] complete.", flush=True)


if __name__ == "__main__":
    main()
