"""Keyword service: comments.sentiment -> comments.keywords.

Uses KeyBERT (all-MiniLM-L6-v2 embeddings) to pull the top keyphrases per comment.
"""
from __future__ import annotations

from keybert import KeyBERT

from src.common import T_KEYWORDS, T_SENTIMENT, run_enricher

MODEL = "all-MiniLM-L6-v2"
TOP_N = 5


def main() -> None:
    kw = KeyBERT(model=MODEL)

    def enrich(rec: dict) -> dict:
        text = (rec.get("text") or "").strip()
        if text:
            pairs = kw.extract_keywords(
                text, keyphrase_ngram_range=(1, 2), stop_words="english", top_n=TOP_N)
            rec["keywords"] = [k for k, _ in pairs]
        else:
            rec["keywords"] = []
        return rec

    run_enricher("keyword", T_SENTIMENT, T_KEYWORDS, enrich)


if __name__ == "__main__":
    main()
