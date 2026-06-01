"""Statistics service: consumes comments.keywords and reports the three required
aggregates live:
  (a) languages with number of messages
  (b) number of messages per sentiment class
  (c) top 10 keywords

Writes stats.json continuously and, once the stream drains, renders bar charts to
figures/ and drops a .stats_done marker so the orchestrator knows the run is over.
"""
from __future__ import annotations

import json
import time
from collections import Counter

from src.common import (DONE_FILE, FIGURES_DIR, STATS_FILE, T_KEYWORDS, consume_loop,
                        decode)


def snapshot(langs: Counter, sents: Counter, kws: Counter, processed: int, final: bool) -> dict:
    return {
        "processed": processed,
        "languages": dict(langs.most_common()),
        "sentiment": dict(sents.most_common()),
        "top_keywords": kws.most_common(10),
        "final": final,
    }


def write_stats(data: dict) -> None:
    STATS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def render_console(data: dict, title: str) -> str:
    bar = "=" * 52
    lines = [bar, f"  {title}  (processed {data['processed']})", bar]
    lines.append("  Languages:")
    for lang, n in list(data["languages"].items())[:10]:
        lines.append(f"    {lang:<10} {n:>7}")
    lines.append("  Sentiment:")
    for s, n in data["sentiment"].items():
        lines.append(f"    {s:<10} {n:>7}")
    lines.append("  Top 10 keywords:")
    for i, (k, n) in enumerate(data["top_keywords"], 1):
        lines.append(f"    {i:>2}. {k:<24} {n:>6}")
    lines.append(bar)
    return "\n".join(lines)


def make_figures(data: dict) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    # (a) languages
    langs = data["languages"]
    if langs:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar(list(langs.keys()), list(langs.values()), color="#4C72B0")
        ax.set_title("Messages per language")
        ax.set_ylabel("messages")
        fig.tight_layout()
        fig.savefig(FIGURES_DIR / "languages.png", dpi=130)
        plt.close(fig)

    # (b) sentiment
    sents = data["sentiment"]
    if sents:
        colors = {"Positive": "#55A868", "Negative": "#C44E52"}
        fig, ax = plt.subplots(figsize=(5, 4))
        ax.bar(list(sents.keys()), list(sents.values()),
               color=[colors.get(k, "#8172B3") for k in sents])
        ax.set_title("Messages per sentiment class")
        ax.set_ylabel("messages")
        fig.tight_layout()
        fig.savefig(FIGURES_DIR / "sentiment.png", dpi=130)
        plt.close(fig)

    # (c) top keywords
    kws = data["top_keywords"]
    if kws:
        labels = [k for k, _ in kws][::-1]
        values = [n for _, n in kws][::-1]
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.barh(labels, values, color="#937860")
        ax.set_title("Top 10 keywords")
        ax.set_xlabel("messages")
        fig.tight_layout()
        fig.savefig(FIGURES_DIR / "keywords.png", dpi=130)
        plt.close(fig)


def main() -> None:
    DONE_FILE.unlink(missing_ok=True)

    langs: Counter = Counter()
    sents: Counter = Counter()
    kws: Counter = Counter()
    last_write = 0.0

    def on_message(msg, processed: int) -> None:
        nonlocal last_write
        rec = decode(msg.value())
        langs[rec.get("lang", "unknown")] += 1
        sents[rec.get("sentiment", "unknown")] += 1
        for k in rec.get("keywords", []):
            kws[str(k).lower()] += 1
        if time.time() - last_write > 2.0:
            data = snapshot(langs, sents, kws, processed, final=False)
            write_stats(data)
            print(render_console(data, "LIVE STATS"), flush=True)
            last_write = time.time()

    def on_drain(processed: int) -> None:
        data = snapshot(langs, sents, kws, processed, final=True)
        write_stats(data)
        make_figures(data)
        print(render_console(data, "FINAL STATS"), flush=True)
        DONE_FILE.write_text(str(processed))
        print(f"[stats] done, {processed} messages; wrote {STATS_FILE.name} and figures/",
              flush=True)

    print("[stats] up: consuming", T_KEYWORDS, flush=True)
    consume_loop("stats", T_KEYWORDS, on_message, group="stats-group", on_drain=on_drain)


if __name__ == "__main__":
    main()
