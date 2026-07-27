"""Convert a Markdown file to PDF, styled for the thesis progress reports.

Renders Vietnamese text and wide metric tables correctly by going
Markdown -> HTML -> Chromium print-to-PDF (Playwright).

Setup (one time):
    pip install markdown playwright
    playwright install chromium

Usage:
    python md_to_pdf.py                                  # THESIS_SUMMARY_2026_VI.md
    python md_to_pdf.py THESIS_PROGRESS_2026_VI.md
    python md_to_pdf.py input.md output.pdf
    python md_to_pdf.py input.md --landscape            # for very wide tables
    python md_to_pdf.py input.md --break-before-h2      # each ## starts a new page

Falls back to WeasyPrint (pip install weasyprint) if Playwright is absent.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import markdown

_LIST_ITEM = re.compile(r"^\s*([-*+]|\d+\.)\s+")

# A browser may always break a line after a hyphen, so "1e-3" in a narrow
# hyperparameter column rendered as "1e-" / "3". U+2011 looks identical and
# does not offer a break opportunity.
_SCI_NOTATION = re.compile(r"(?<=\de)-(?=\d)")


def protect_hyphens(text: str) -> str:
    return _SCI_NOTATION.sub("‑", text)


def normalize_lists(text: str) -> str:
    """Insert a blank line before a list that directly follows a paragraph.

    CommonMark (GitHub) lets a bullet list interrupt a paragraph; Python-Markdown
    does not, so "**Heading.**\\n- item" rendered as one paragraph with literal
    "-" characters. Adding the blank line makes the PDF match GitHub.
    """
    out: list[str] = []
    prev = ""
    in_fence = False
    for line in text.split("\n"):
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
        if (
            not in_fence
            and _LIST_ITEM.match(line)
            and prev.strip()
            and not _LIST_ITEM.match(prev)
            and not prev.lstrip().startswith(("|", ">"))
        ):
            out.append("")
        out.append(line)
        prev = line
    return "\n".join(out)

# Fonts that ship with Windows and cover the full Vietnamese diacritic set.
FONT_STACK = '"Segoe UI", "Times New Roman", "Arial Unicode MS", sans-serif'
MONO_STACK = '"Cascadia Mono", "Consolas", "Courier New", monospace'

CSS = """
@page {{
    size: A4 {orientation};
    margin: 16mm 14mm 18mm 14mm;
}}

body {{
    font-family: {font};
    font-size: 10.5pt;
    line-height: 1.5;
    color: #1a1a1a;
    -webkit-print-color-adjust: exact;
    print-color-adjust: exact;
}}

/* ---------- headings ---------- */
h1 {{
    font-size: 20pt;
    margin: 0 0 4pt 0;
    padding-bottom: 6pt;
    border-bottom: 2.5pt solid #1a1a1a;
    line-height: 1.25;
}}
h2 {{
    font-size: 14pt;
    margin: 20pt 0 8pt 0;
    padding-bottom: 3pt;
    border-bottom: 0.8pt solid #999;
    line-height: 1.3;
    {break_before_h2}
}}
h3 {{ font-size: 11.5pt; margin: 14pt 0 6pt 0; }}
h4 {{ font-size: 10.5pt; margin: 12pt 0 4pt 0; }}
h1, h2, h3, h4 {{ break-after: avoid; page-break-after: avoid; }}

/* ---------- text ---------- */
p {{ margin: 0 0 7pt 0; orphans: 3; widows: 3; }}
ul, ol {{ margin: 0 0 8pt 0; padding-left: 18pt; }}
li {{ margin-bottom: 3pt; }}
strong {{ font-weight: 600; }}
a {{ color: #14508c; text-decoration: none; }}
hr {{
    border: none;
    border-top: 0.6pt solid #d0d0d0;
    margin: 14pt 0;
}}

/* ---------- tables (the metric tables are the point of this report) ---------- */
table {{
    width: 100%;
    border-collapse: collapse;
    margin: 8pt 0 12pt 0;
    font-size: 8.5pt;
    font-variant-numeric: tabular-nums;
    break-inside: avoid;
    page-break-inside: avoid;
}}
th, td {{
    border: 0.5pt solid #b8b8b8;
    padding: 3.5pt 5pt;
    text-align: left;
    vertical-align: top;
    word-break: normal;
    /* break-word, NOT anywhere: `anywhere` also shrinks a cell's min-content
       width, which split "1e-3" into "1e-" / "3" in the hyperparameter table. */
    overflow-wrap: break-word;
    hyphens: none;
}}
th {{
    background: #ececec;
    font-weight: 600;
    white-space: nowrap;
}}
tbody tr:nth-child(even) td {{ background: #f8f8f8; }}

/* ---------- blockquote: used for the "how to read this" call-outs ---------- */
blockquote {{
    margin: 9pt 0;
    padding: 7pt 11pt;
    border-left: 3pt solid #14508c;
    background: #f2f6fa;
    break-inside: avoid;
    page-break-inside: avoid;
}}
blockquote p {{ margin: 0; }}

/* ---------- code ---------- */
code {{
    font-family: {mono};
    font-size: 8.8pt;
    background: #f0f0f0;
    padding: 0.5pt 3pt;
    border-radius: 2pt;
}}
pre {{
    font-family: {mono};
    font-size: 8.5pt;
    background: #f5f5f5;
    padding: 7pt 9pt;
    border-left: 2.5pt solid #ccc;
    overflow-x: auto;
    break-inside: avoid;
}}
pre code {{ background: none; padding: 0; }}
"""

HTML_SHELL = """<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="utf-8">
<title>{title}</title>
<style>{css}</style>
</head>
<body>
{content}
</body>
</html>
"""

FOOTER = (
    '<div style="width:100%;font-size:7.5pt;color:#777;'
    'font-family:{font};padding:0 14mm;">'
    '<span style="float:left;">{title}</span>'
    '<span style="float:right;">'
    '<span class="pageNumber"></span> / <span class="totalPages"></span>'
    "</span></div>"
)


def build_html(md_path: Path, landscape: bool, break_before_h2: bool) -> str:
    """Markdown file -> a single self-contained HTML string."""
    text = protect_hyphens(normalize_lists(md_path.read_text(encoding="utf-8")))

    body = markdown.markdown(
        text,
        extensions=["tables", "fenced_code", "sane_lists", "attr_list", "nl2br"],
        output_format="html5",
    )

    css = CSS.format(
        font=FONT_STACK,
        mono=MONO_STACK,
        orientation="landscape" if landscape else "portrait",
        break_before_h2="break-before: page; page-break-before: always;"
        if break_before_h2
        else "",
    )
    return HTML_SHELL.format(title=md_path.stem, css=css, content=body)


def render_playwright(html: str, out_path: Path, title: str, landscape: bool) -> None:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        # Everything is inline, so no network wait is needed.
        page.set_content(html, wait_until="load")
        page.emulate_media(media="print")
        page.pdf(
            path=str(out_path),
            format="A4",
            landscape=landscape,
            print_background=True,
            display_header_footer=True,
            header_template='<div style="font-size:1pt;"></div>',  # empty header
            footer_template=FOOTER.format(font=FONT_STACK, title=title),
            margin={
                "top": "16mm",
                "bottom": "18mm",
                "left": "14mm",
                "right": "14mm",
            },
        )
        browser.close()


def render_weasyprint(html: str, out_path: Path) -> None:
    from weasyprint import HTML

    HTML(string=html).write_pdf(str(out_path))


def main(argv: list[str]) -> int:
    args = [a for a in argv[1:] if not a.startswith("--")]
    flags = {a for a in argv[1:] if a.startswith("--")}

    landscape = "--landscape" in flags
    break_before_h2 = "--break-before-h2" in flags

    root = Path(__file__).resolve().parent
    md_path = Path(args[0]) if args else root / "THESIS_SUMMARY_2026_VI.md"
    if not md_path.is_absolute():
        md_path = root / md_path
    if not md_path.exists():
        print(f"Not found: {md_path}")
        return 1

    out_path = Path(args[1]) if len(args) > 1 else md_path.with_suffix(".pdf")
    if not out_path.is_absolute():
        out_path = root / out_path

    html = build_html(md_path, landscape, break_before_h2)

    try:
        render_playwright(html, out_path, md_path.stem, landscape)
        engine = "Playwright/Chromium"
    except PermissionError:
        print(f"Cannot write {out_path.name} - close it in your PDF viewer first.")
        return 1
    except ImportError:
        try:
            render_weasyprint(html, out_path)
            engine = "WeasyPrint"
        except ImportError:
            print(
                "No PDF engine installed. Pick one:\n"
                "  pip install playwright && playwright install chromium   (recommended)\n"
                "  pip install weasyprint"
            )
            return 1

    size_kb = out_path.stat().st_size / 1024
    print(f"{md_path.name} -> {out_path.name}  ({size_kb:.0f} KB, via {engine})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
