"""Sentiment service: comments.lang -> comments.sentiment.

Uses distilbert-base-uncased-finetuned-sst-2-english, which emits exactly the two
classes the project asks for: Positive / Negative.
"""
from __future__ import annotations

from transformers import pipeline

from src.common import T_LANG, T_SENTIMENT, run_enricher
from src.lang_service import top1

MODEL = "distilbert-base-uncased-finetuned-sst-2-english"


def main() -> None:
    clf = pipeline("sentiment-analysis", model=MODEL, truncation=True, max_length=512)

    def enrich(rec: dict) -> dict:
        text = (rec.get("text") or "").strip()
        if text:
            out = top1(clf(text))
            rec["sentiment"] = out["label"].capitalize()   # Positive / Negative
            rec["sentiment_score"] = round(float(out["score"]), 4)
        else:
            rec["sentiment"] = "unknown"
            rec["sentiment_score"] = 0.0
        return rec

    run_enricher("sentiment", T_LANG, T_SENTIMENT, enrich)


if __name__ == "__main__":
    main()
