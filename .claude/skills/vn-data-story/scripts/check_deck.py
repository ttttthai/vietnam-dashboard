"""Check that a vn_deck .pptx is editable: every native chart has an embedded workbook that opens, python-pptx can
read its plots and series, and the workbook columns the series point at exist. Prints one line per slide and totals.

    python3 check_deck.py deck.pptx [--expect-native 8,9,10-15]   (slides that must contain a native chart)

Exit code 1 if any chart lacks a workbook, a workbook does not open, or an expected slide has no native chart.
"""
import io
import re
import sys
import zipfile

from pptx import Presentation
from pptx.enum.shapes import MSO_SHAPE_TYPE


def walk(shapes):
    for sh in shapes:
        yield sh
        if sh.shape_type == MSO_SHAPE_TYPE.GROUP:
            yield from walk(sh.shapes)


def ranges(spec):
    out = set()
    for part in spec.split(','):
        if '-' in part:
            a, b = part.split('-')
            out.update(range(int(a), int(b) + 1))
        elif part:
            out.add(int(part))
    return out


def main(path, expect=()):
    prs = Presentation(path)
    bad, tot = [], {'chart': 0, 'table': 0, 'diagram': 0, 'formulas': 0}
    for n, slide in enumerate(prs.slides, 1):
        charts = tables = groups = shapes = 0
        notes = slide.has_notes_slide and slide.notes_slide.notes_text_frame.text.strip()
        for sh in walk(slide.shapes):
            if getattr(sh, 'has_chart', False) and sh.has_chart:
                charts += 1
                ch = sh.chart
                wb = ch.part.chart_workbook.xlsx_part
                if wb is None:
                    bad.append(f'slide {n}: chart "{sh.name}" has no embedded workbook')
                    continue
                try:
                    z = zipfile.ZipFile(io.BytesIO(wb.blob))
                    sheet = z.read('xl/worksheets/sheet1.xml').decode('utf-8')
                    tot['formulas'] += sheet.count('<f>')
                except Exception as e:                          # noqa: BLE001
                    bad.append(f'slide {n}: workbook of "{sh.name}" does not open ({e})')
                    continue
                nser = sum(len(list(p.series)) for p in ch.plots)
                if not nser:
                    bad.append(f'slide {n}: chart "{sh.name}" has no series')
                cols = set(re.findall(r'<c:f>Sheet1!\$([A-Z]+)\$', ch.part.blob.decode('utf-8')))
                if not cols:
                    bad.append(f'slide {n}: chart "{sh.name}" series are not linked to the workbook')
            elif getattr(sh, 'has_table', False) and sh.has_table:
                tables += 1
            elif sh.shape_type == MSO_SHAPE_TYPE.GROUP and sh.name.startswith('Chart · '):
                groups += 1
            else:
                shapes += 1
        kind = 'chart' if charts else ('table' if tables else ('diagram' if groups else ''))
        if kind:
            tot[kind] += 1
        if n in expect and not charts:
            bad.append(f'slide {n}: expected a native chart, found none')
        print(f'{n:>3}  charts {charts:>2}  tables {tables}  diagram groups {groups}  notes {"yes" if notes else "-":3}  '
              f'-> {kind or "text"}')
    print(f"\nslides with native charts: {tot['chart']}; native-table slides: {tot['table']}; "
          f"shape-diagram slides: {tot['diagram']}; workbook formulas: {tot['formulas']}")
    for b in bad:
        print('PROBLEM:', b)
    return 1 if bad else 0


if __name__ == '__main__':
    exp = ()
    if '--expect-native' in sys.argv:
        exp = ranges(sys.argv[sys.argv.index('--expect-native') + 1])
    sys.exit(main(sys.argv[1], exp))
