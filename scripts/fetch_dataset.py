"""Fetch a real public Reddit-comments dataset from the HuggingFace hub and write a
subset to data/comments.jsonl. The dataset is streamed, so we never download the whole
thing. The chosen source is recorded in data/dataset_source.txt for the report.

Primary source: HuggingFaceGECLM/REDDIT_comments - real Reddit comments, one split per
subreddit. We pull a balanced mix across several subreddits for topic variety.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from datasets import load_dataset

from src.common import DATA_DIR

PRIMARY = "HuggingFaceGECLM/REDDIT_comments"
SUBREDDITS = [
    "programming", "science", "history", "books", "personalfinance",
    "gaming", "travel", "philosophy", "technology", "Fitness",
    "askscience", "todayilearned", "explainlikeimfive", "DIY", "space",
]

# Parquet-based fallbacks (repo, config, text_field) if the primary is unavailable.
FALLBACKS = [
    ("SocialGrep/one-million-reddit-jokes", None, "selftext"),
    ("SocialGrep/one-million-reddit-questions", None, "selftext"),
]

SKIP = {"[deleted]", "[removed]", ""}


def clean(text: str | None) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def stream_primary(rows: int, min_len: int) -> list[dict]:
    per = rows // len(SUBREDDITS) + 1
    collected: list[dict] = []
    for sub in SUBREDDITS:
        ds = load_dataset(PRIMARY, split=sub, streaming=True)
        c = 0
        for ex in ds:
            text = clean(ex.get("body"))
            if text in SKIP or len(text) < min_len:
                continue
            collected.append({
                "id": str(ex.get("id") or f"{sub}-{c}"),
                "subreddit": sub,
                "text": text,
                "created_utc": ex.get("created_utc"),
            })
            c += 1
            if c >= per or len(collected) >= rows:
                break
        print(f"[fetch]   {sub}: +{c}  (total {len(collected)})", flush=True)
        if len(collected) >= rows:
            break
    return collected


def stream_fallback(repo: str, config: str | None, field: str, n: int, min_len: int) -> list[dict]:
    ds = (load_dataset(repo, config, split="train", streaming=True) if config
          else load_dataset(repo, split="train", streaming=True))
    rows: list[dict] = []
    for ex in ds:
        text = clean(ex.get(field))
        if text in SKIP or len(text) < min_len:
            continue
        rows.append({
            "id": str(ex.get("id") or len(rows)),
            "subreddit": ex.get("subreddit") or "",
            "text": text,
            "created_utc": ex.get("created_utc"),
        })
        if len(rows) >= n:
            break
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=3000)
    ap.add_argument("--min-len", type=int, default=20)
    ap.add_argument("--out", default=str(DATA_DIR / "comments.jsonl"))
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    used = None
    rows: list[dict] = []

    try:
        print(f"[fetch] primary: {PRIMARY} across {len(SUBREDDITS)} subreddits", flush=True)
        rows = stream_primary(args.rows, args.min_len)
        if rows:
            used = (PRIMARY, ",".join(SUBREDDITS), "body")
    except Exception as e:
        print(f"[fetch] primary failed: {e}", flush=True)

    if not rows:
        for repo, config, field in FALLBACKS:
            try:
                print(f"[fetch] fallback: {repo} (field={field}) ...", flush=True)
                rows = stream_fallback(repo, config, field, args.rows, args.min_len)
                if rows:
                    used = (repo, config or "", field)
                    break
            except Exception as e:
                print(f"[fetch] {repo} failed: {e}", flush=True)

    if not rows or used is None:
        raise SystemExit("could not fetch any candidate dataset")

    out = Path(args.out)
    with open(out, "w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    repo, config, field = used
    url = f"https://huggingface.co/datasets/{repo}"
    (DATA_DIR / "dataset_source.txt").write_text(
        f"repo={repo}\nconfig={config}\ntext_field={field}\nurl={url}\n")
    print(f"[fetch] wrote {len(rows)} comments to {out}")
    print(f"[fetch] source: {url}")


if __name__ == "__main__":
    main()
