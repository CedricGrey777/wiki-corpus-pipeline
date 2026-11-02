"""Only synthetic fixtures are shipped. No downloaded Wikipedia text is committed."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema([(name, pa.string()) for name in ("id", "url", "title", "text")])


def row(article_id, text, title=None):
    return {"id": article_id, "url": f"https://example.invalid/article/{article_id}",
            "title": title or f"Synthetic article {article_id}", "text": text}


def write_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), path)


def create_fixtures(directory) -> list[str]:
    directory = Path(directory)
    shared = "共同的合成文本，用於核對跨批次精確去重。"
    first = [row("10", shared + "  \r\n"), row("20", "  if x:\n    print('保留縮進、標點及大小寫')\n"),
             row("30", "Cafe\u0301 是測試用的合成文字。"), row("20", "  if x:\n    print('保留縮進、標點及大小寫')\n"),
             row("40", None), {"id": None, "url": None, "title": "缺字段", "text": "不應發布"}]
    second = [row("02", shared), row("50", "另一篇獨立的合成文章。"), row("60", "Café 是測試用的合成文字。")]
    paths = [directory / "synthetic-00.parquet", directory / "synthetic-01.parquet"]
    write_rows(paths[0], first)
    write_rows(paths[1], second)
    return [str(path.resolve()) for path in paths]
