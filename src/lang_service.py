"""Language detection service: comments.raw -> comments.lang.

Uses papluca/xlm-roberta-base-language-detection (20 languages).
"""
from __future__ import annotations

from transformers import pipeline

from src.common import T_LANG, T_RAW, run_enricher

MODEL = "papluca/xlm-roberta-base-language-detection"


def top1(result):
    """Normalise a text-classification pipeline result to a single {label, score}."""
    while isinstance(result, list):
        result = result[0]
    return result


def main() -> None:
    clf = pipeline("text-classification", model=MODEL, top_k=1,
                   truncation=True, max_length=512)

    def enrich(rec: dict) -> dict:
        text = (rec.get("text") or "").strip()
        if text:
            out = top1(clf(text))
            rec["lang"] = out["label"]
            rec["lang_score"] = round(float(out["score"]), 4)
        else:
            rec["lang"] = "unknown"
            rec["lang_score"] = 0.0
        return rec

    run_enricher("lang", T_RAW, T_LANG, enrich)


if __name__ == "__main__":
    main()
