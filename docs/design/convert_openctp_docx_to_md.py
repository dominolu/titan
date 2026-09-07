from __future__ import annotations

import re
from pathlib import Path

from docx import Document
from docx.oxml.ns import qn
from docx.table import Table
from docx.text.paragraph import Paragraph


ROOT = Path("/Users/dominolu/dev/titan")
SOURCE = ROOT / "docs/design/Titan_OpenCTP_Connector_技术方案.docx"
OUTPUT = ROOT / "docs/design/Titan_OpenCTP_Connector_技术方案.md"


def iter_blocks(document):
    for child in document.element.body.iterchildren():
        if child.tag == qn("w:p"):
            yield Paragraph(child, document)
        elif child.tag == qn("w:tbl"):
            yield Table(child, document)


def escape_inline(text: str) -> str:
    text = text.replace("\\", "\\\\")
    for char in ("*", "_", "`", "["):
        text = text.replace(char, "\\" + char)
    return text


def run_markdown(run) -> str:
    text = escape_inline(run.text)
    if not text:
        return ""
    if run.font.strike:
        text = f"~~{text}~~"
    if run.bold:
        text = f"**{text}**"
    if run.italic:
        text = f"*{text}*"
    return text


def paragraph_inline(paragraph: Paragraph) -> str:
    parts: list[str] = []
    run_by_element = {id(run._r): run for run in paragraph.runs}
    for child in paragraph._p:
        if child.tag == qn("w:r"):
            run = run_by_element.get(id(child))
            if run is not None:
                parts.append(run_markdown(run))
        elif child.tag == qn("w:hyperlink"):
            rid = child.get(qn("r:id"))
            target = paragraph.part.rels[rid].target_ref if rid in paragraph.part.rels else "#"
            label = "".join(node.text or "" for node in child.iter(qn("w:t")))
            parts.append(f"[{escape_inline(label)}]({target})")
    return "".join(parts).strip() or escape_inline(paragraph.text.strip())


def table_cell(cell) -> str:
    parts = [paragraph_inline(p) for p in cell.paragraphs if p.text.strip()]
    return "<br>".join(parts).replace("|", "\\|")


def table_markdown(table: Table) -> list[str]:
    if not table.rows:
        return []
    matrix = [[table_cell(cell) for cell in row.cells] for row in table.rows]
    width = max(len(row) for row in matrix)
    matrix = [row + [""] * (width - len(row)) for row in matrix]
    lines = ["| " + " | ".join(matrix[0]) + " |"]
    lines.append("| " + " | ".join(["---"] * width) + " |")
    for row in matrix[1:]:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def fence_language(code: str) -> str:
    stripped = code.lstrip()
    if stripped.startswith("{") and '"' in stripped:
        return "json"
    if "#define " in code or "typedef struct" in code or "extern \"C\"" in code:
        return "cpp"
    if re.search(r"^\s*\[[A-Za-z0-9_.-]+\]", code, re.MULTILINE):
        return "toml"
    if re.search(r"^(crates|native|tests|packaging)/", stripped):
        return "text"
    return "text"


def build_markdown() -> str:
    doc = Document(SOURCE)
    lines: list[str] = []
    list_type: str | None = None
    ordered_index = 0

    def close_list():
        nonlocal list_type, ordered_index
        if list_type is not None:
            lines.append("")
        list_type = None
        ordered_index = 0

    for block in iter_blocks(doc):
        if isinstance(block, Table):
            close_list()
            lines.extend(table_markdown(block))
            lines.append("")
            continue

        text = block.text.strip()
        if not text:
            continue
        style = block.style.name if block.style else ""
        content = paragraph_inline(block)

        if style.startswith("List Bullet"):
            if list_type != "bullet":
                close_list()
                list_type = "bullet"
            lines.append(f"- {content}")
            continue
        if style.startswith("List Number"):
            if list_type != "ordered":
                close_list()
                list_type = "ordered"
            ordered_index += 1
            lines.append(f"{ordered_index}. {content}")
            continue

        close_list()
        if style == "Title":
            lines.extend([f"# {content}", ""])
        elif style.startswith("Heading"):
            match = re.search(r"(\d+)", style)
            level = min(max(int(match.group(1)) if match else 2, 1), 6)
            lines.extend([f"{'#' * level} {content}", ""])
        elif style == "Code Block":
            lang = fence_language(block.text)
            lines.extend([f"```{lang}", block.text.rstrip(), "```", ""])
        elif style == "Caption Small":
            lines.extend([f"*{content}*", ""])
        else:
            lines.extend([content, ""])

    close_list()
    cleaned: list[str] = []
    for line in lines:
        if line == "" and cleaned and cleaned[-1] == "":
            continue
        cleaned.append(line.rstrip())
    return "\n".join(cleaned).strip() + "\n"


def main():
    if not SOURCE.exists():
        raise FileNotFoundError(SOURCE)
    OUTPUT.write_text(build_markdown(), encoding="utf-8")
    print(OUTPUT)


if __name__ == "__main__":
    main()
