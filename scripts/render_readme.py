#!/usr/bin/env python3
"""Render README.md to a self-contained HTML (images inlined as base64).

Run:  uv run --with markdown python scripts/render_readme.py
Then: wkhtmltopdf --enable-local-file-access README.html README.pdf
"""
from __future__ import annotations

import base64
import mimetypes
import re
from pathlib import Path

import markdown

ROOT = Path(__file__).resolve().parent.parent
MD = ROOT / "README.md"
HTML = ROOT / "README.html"

CSS = """
@page { size: A4; margin: 18mm 16mm; }
body { font-family: -apple-system, Helvetica, Arial, sans-serif; font-size: 11pt;
       line-height: 1.45; color: #1f2328; max-width: 100%; }
h1 { font-size: 20pt; border-bottom: 2px solid #d0d7de; padding-bottom: .2em; }
h2 { font-size: 15pt; border-bottom: 1px solid #d0d7de; padding-bottom: .2em; margin-top: 1.3em; }
h3 { font-size: 12.5pt; margin-top: 1.1em; }
code { font-family: "SFMono-Regular", Menlo, Consolas, monospace; font-size: 9.5pt;
       background: #f0f1f2; padding: .1em .3em; border-radius: 4px; }
pre { background: #f6f8fa; border: 1px solid #d0d7de; border-radius: 6px; padding: 12px;
      overflow-x: auto; page-break-inside: avoid; }
pre code { background: none; padding: 0; font-size: 9pt; line-height: 1.4; }
blockquote { color: #57606a; border-left: 4px solid #d0d7de; margin: 0; padding: .2em 1em; background: #f6f8fa; }
table { border-collapse: collapse; }
th, td { border: 1px solid #d0d7de; padding: 6px 10px; }
img { max-width: 100%; border: 1px solid #d0d7de; border-radius: 6px; }
a { color: #0969da; }
"""


def inline_images(html: str) -> str:
    def repl(m: re.Match) -> str:
        src = m.group(1)
        p = ROOT / src
        if not p.exists():
            return m.group(0)
        mime = mimetypes.guess_type(p.name)[0] or "image/png"
        b64 = base64.b64encode(p.read_bytes()).decode()
        return f'src="data:{mime};base64,{b64}"'

    return re.sub(r'src="([^"]+)"', repl, html)


def main() -> None:
    body = markdown.markdown(
        MD.read_text(encoding="utf-8"),
        extensions=["fenced_code", "tables", "sane_lists"],
    )
    body = inline_images(body)
    HTML.write_text(
        f"<!doctype html><html><head><meta charset='utf-8'>"
        f"<style>{CSS}</style></head><body>{body}</body></html>",
        encoding="utf-8",
    )
    print(f"wrote {HTML}")


if __name__ == "__main__":
    main()
