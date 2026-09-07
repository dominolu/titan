from __future__ import annotations

import html
import re
from pathlib import Path

from docx import Document
from docx.oxml.ns import qn
from docx.table import Table
from docx.text.paragraph import Paragraph


ROOT = Path("/Users/dominolu/dev/titan")
SOURCE = ROOT / "docs/design/Titan_OpenCTP_Connector_技术方案.docx"
OUTPUT = ROOT / "docs/design/X_Article.html"


def iter_blocks(document):
    for child in document.element.body.iterchildren():
        if child.tag == qn("w:p"):
            yield Paragraph(child, document)
        elif child.tag == qn("w:tbl"):
            yield Table(child, document)


def run_html(run) -> str:
    text = html.escape(run.text).replace("\n", "<br>")
    if not text:
        return ""
    if run.bold:
        text = f"<strong>{text}</strong>"
    if run.italic:
        text = f"<em>{text}</em>"
    if run.font.strike:
        text = f"<s>{text}</s>"
    return text


def paragraph_inner(paragraph: Paragraph) -> str:
    parts: list[str] = []
    for child in paragraph._p:
        if child.tag == qn("w:r"):
            parts.append(run_html(next(r for r in paragraph.runs if r._r is child)))
        elif child.tag == qn("w:hyperlink"):
            rid = child.get(qn("r:id"))
            target = paragraph.part.rels[rid].target_ref if rid in paragraph.part.rels else "#"
            label = "".join(node.text or "" for node in child.iter(qn("w:t")))
            parts.append(
                f'<a href="{html.escape(target, quote=True)}" target="_blank" rel="noopener">'
                f"{html.escape(label)}</a>"
            )
    return "".join(parts).strip()


def cell_html(cell, header: bool) -> str:
    tag = "th" if header else "td"
    content = "<br>".join(
        paragraph_inner(p) or html.escape(p.text)
        for p in cell.paragraphs
        if p.text.strip()
    )
    return f"<{tag}>{content}</{tag}>"


def table_html(table: Table) -> str:
    rows: list[str] = []
    for row_index, row in enumerate(table.rows):
        cells = "".join(cell_html(cell, row_index == 0) for cell in row.cells)
        rows.append(f"<tr>{cells}</tr>")
    return '<div class="table-wrap"><table>' + "".join(rows) + "</table></div>"


def slug(text: str) -> str:
    value = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "-", text).strip("-")
    return value or "section"


def paragraph_kind(paragraph: Paragraph) -> tuple[str, int]:
    style = paragraph.style.name if paragraph.style else ""
    if style == "Title":
        return "title", 0
    if style.startswith("Heading"):
        match = re.search(r"(\d+)", style)
        return "heading", int(match.group(1)) if match else 2
    if style.startswith("List Bullet"):
        return "ul", 0
    if style.startswith("List Number"):
        return "ol", 0
    if style == "Code Block":
        return "code", 0
    if style == "Caption Small":
        return "caption", 0
    return "paragraph", 0


def build_article() -> str:
    doc = Document(SOURCE)
    blocks = list(iter_blocks(doc))
    start = next(
        i for i, block in enumerate(blocks)
        if isinstance(block, Paragraph) and block.text.strip() == "1 执行摘要"
    )

    body: list[str] = []
    active_list: str | None = None

    def close_list():
        nonlocal active_list
        if active_list:
            body.append(f"</{active_list}>")
            active_list = None

    for block in blocks[start:]:
        if isinstance(block, Table):
            close_list()
            body.append(table_html(block))
            continue

        text = block.text.strip()
        if not text:
            continue
        kind, level = paragraph_kind(block)
        inner = paragraph_inner(block) or html.escape(text)

        if kind in {"ul", "ol"}:
            if active_list != kind:
                close_list()
                body.append(f"<{kind}>")
                active_list = kind
            body.append(f"<li>{inner}</li>")
            continue

        close_list()
        if kind == "heading":
            tag_level = min(max(level + 1, 2), 4)
            body.append(f'<h{tag_level} id="{slug(text)}">{inner}</h{tag_level}>')
        elif kind == "code":
            body.append(f"<pre><code>{html.escape(block.text)}</code></pre>")
        elif kind == "caption":
            body.append(f'<p class="caption">{inner}</p>')
        else:
            body.append(f"<p>{inner}</p>")
    close_list()

    article_body = "\n".join(body)
    return f'''<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Titan OpenCTP Connector 技术方案</title>
  <style>
    :root {{ color-scheme: light; --ink:#111827; --muted:#556070; --line:#d9e1ea; --navy:#17375e; --pale:#eef4fa; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:#f3f5f7; color:var(--ink); font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",Arial,sans-serif; }}
    .toolbar {{ position:sticky; top:0; z-index:10; display:flex; align-items:center; justify-content:center; gap:14px; padding:12px; background:rgba(255,255,255,.96); border-bottom:1px solid var(--line); }}
    .toolbar button {{ border:0; border-radius:999px; padding:10px 20px; color:white; background:#0f1419; font-size:15px; font-weight:700; cursor:pointer; }}
    .toolbar span {{ color:var(--muted); font-size:13px; }}
    article {{ width:min(900px,calc(100% - 32px)); margin:28px auto 72px; padding:56px 64px 72px; background:white; box-shadow:0 8px 28px rgba(15,23,42,.08); }}
    .eyebrow {{ margin:0 0 16px; color:#1d4ed8; font-size:14px; font-weight:800; letter-spacing:.08em; }}
    h1 {{ margin:0 0 12px; font-size:42px; line-height:1.16; letter-spacing:-.025em; }}
    .subtitle {{ margin:0 0 26px; color:var(--muted); font-size:20px; line-height:1.5; }}
    .lead {{ margin:0 0 42px; font-size:18px; line-height:1.8; }}
    h2 {{ margin:52px 0 18px; font-size:30px; line-height:1.3; }}
    h3 {{ margin:38px 0 14px; font-size:23px; line-height:1.35; }}
    h4 {{ margin:28px 0 10px; font-size:18px; line-height:1.4; }}
    p, li {{ font-size:16px; line-height:1.78; }}
    p {{ margin:0 0 16px; }}
    ul, ol {{ margin:8px 0 22px; padding-left:28px; }}
    li {{ margin:5px 0; }}
    a {{ color:#1d4ed8; text-decoration:underline; text-underline-offset:2px; }}
    pre {{ overflow-x:auto; margin:18px 0 24px; padding:18px 20px; border:1px solid #e2e8f0; border-radius:8px; background:#f6f8fa; white-space:pre; }}
    code {{ font:13px/1.55 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; }}
    .caption {{ margin-top:-12px; color:var(--muted); font-size:13px; }}
    .table-wrap {{ overflow-x:auto; margin:18px 0 28px; }}
    table {{ width:100%; min-width:620px; border-collapse:collapse; font-size:14px; line-height:1.5; }}
    th, td {{ padding:10px 12px; border:1px solid var(--line); text-align:left; vertical-align:top; }}
    th {{ color:white; background:var(--navy); font-weight:700; }}
    tbody tr:nth-child(even) td {{ background:var(--pale); }}
    @media (max-width:700px) {{ article {{ width:100%; margin:0; padding:34px 22px 60px; box-shadow:none; }} h1 {{ font-size:34px; }} .toolbar span {{ display:none; }} }}
    @media print {{ body {{ background:white; }} .toolbar {{ display:none; }} article {{ width:auto; margin:0; padding:0; box-shadow:none; }} }}
  </style>
</head>
<body>
  <div class="toolbar">
    <button id="copyButton" type="button">复制文章正文</button>
    <span id="copyStatus">复制后粘贴到 x.com 的 Articles 编辑器</span>
  </div>
  <article id="article">
    <p class="eyebrow">TITAN 技术设计</p>
    <h1>Titan OpenCTP Connector 技术方案</h1>
    <p class="subtitle">自建薄 C++ Connector 与版本化 C ABI</p>
    <p class="lead">本方案定义 Titan 对接 CTP、CTP 股票期权、TTS、中泰 XTP、华鑫奇点、易盛 TAP 和量投 QDP 的统一工程路径。核心决策是保留 CTP API 与 SPI 在 C++ 边界内，通过小型、稳定、版本化的 C ABI 暴露给 Rust Sidecar，并以独立 Connector 进程和明确的柜台 Profile 管理版本差异、故障隔离及柜台特例。</p>
    {article_body}
  </article>
  <script>
    const button = document.getElementById('copyButton');
    const status = document.getElementById('copyStatus');
    const article = document.getElementById('article');
    function fallbackCopy() {{
      const range = document.createRange();
      range.selectNodeContents(article);
      const selection = window.getSelection();
      selection.removeAllRanges();
      selection.addRange(range);
      const ok = document.execCommand('copy');
      selection.removeAllRanges();
      if (!ok) throw new Error('copy failed');
    }}
    button.addEventListener('click', async () => {{
      try {{
        const rich = '<article>' + article.innerHTML + '</article>';
        if (navigator.clipboard && window.ClipboardItem) {{
          await navigator.clipboard.write([new ClipboardItem({{
            'text/html': new Blob([rich], {{type:'text/html'}}),
            'text/plain': new Blob([article.innerText], {{type:'text/plain'}})
          }})]);
        }} else {{
          fallbackCopy();
        }}
        status.textContent = '已复制，可以粘贴到 X Articles';
      }} catch (error) {{
        try {{ fallbackCopy(); status.textContent = '已复制，可以粘贴到 X Articles'; }}
        catch (_) {{ status.textContent = '自动复制失败，请在正文中按 ⌘A 和 ⌘C'; }}
      }}
    }});
  </script>
</body>
</html>
'''


def main():
    if not SOURCE.exists():
        raise FileNotFoundError(SOURCE)
    OUTPUT.write_text(build_article(), encoding="utf-8")
    print(OUTPUT)


if __name__ == "__main__":
    main()
