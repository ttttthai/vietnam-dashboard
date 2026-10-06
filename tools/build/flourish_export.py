"""Export the datasets behind the dashboard's Flourish charts as CSV (one file per visualisation).

Flourish visualisations (created 2026-10-06 via the Flourish connector; publish them in Flourish to embed):
  30475879  GDP growth 2011–2030: actuals, forecasts, target   (line-bar-pie)
  30475880  Where bank credit goes (latest SBV sector month)    (sankey)
  30475882  34 provinces: income vs fertility                   (scatter)

Usage:  python3 tools/build/flourish_export.py [out_dir]   (default: data/flourish/)
Then upload each CSV to its visualisation (Flourish connector: flourish_update_visualisation_data,
or the Data tab in the Flourish editor). Column order must stay the same so the bindings keep working.
"""
import csv, json, os, re, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, 'data', 'flourish')
load = lambda f: json.load(open(os.path.join(ROOT, 'data', f), encoding='utf-8'))
E, S, F = load('economy.json'), load('society.json'), load('finance.json')['FINSYS']
os.makedirs(OUT, exist_ok=True)

def write(name, header, rows):
    with open(os.path.join(OUT, name), 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f); w.writerow(header); w.writerows(rows)
    print('wrote', name, len(rows), 'rows')

# 1 · GDP growth: actuals from ECON_OFFICIAL, forecasts/target from ECONFLOW.projections (never averaged)
EO, P = E['ECON_OFFICIAL'], E['ECONFLOW']['projections']['series']
cols = [('IMF WEO Apr-2026', 'gdp_growth_pct_imf'), ('World Bank Oct-2026', 'gdp_growth_pct_wb'),
        ('ADB Sep-2026', 'gdp_growth_pct_adb'), ('AMRO 2026', 'gdp_growth_pct_amro'), ('Official target (≥10%)', 'gdp_growth_pct_target')]
rows = [[EO['y0'] + i, v] + [''] * len(cols) for i, v in enumerate(EO['gdp_growth']) if EO['y0'] + i >= 2011]
last = rows[-1]
for j in range(len(cols) - 1): last[2 + j] = last[1]          # forecast lines start from the last actual
for y in range(last[0] + 1, 2031):
    r = [y, '']
    for _, k in cols:
        s = P[k]; v = s['base'][s['years'].index(y)] if y in s['years'] else None
        r.append('' if v is None else v)
    rows.append(r)
write('gdp_growth.csv', ['Year', 'Actual (NSO)'] + [c for c, _ in cols], rows)

# 2 · Credit Sankey: SBV sector levels (latest month) → sectors; other services → estimated parts
L, per = F['sectors']['levels'], F['sectors']['periods'][-1]
tot, os_ = L['total'][-1], L['other_services'][-1]
src = f'All credit to the economy ({per})'
sec = [('Agriculture, forestry, fisheries', 'agriculture_forestry_fisheries'), ('Industry', 'industry'), ('Construction', 'construction'),
       ('Trade', 'trade'), ('Transport & telecoms', 'transport_telecom'), ('Other services', 'other_services')]
rows = [[src, n, round(L[k][-1] / 1000, 1), f'{L[k][-1] / tot * 100:.1f}% of credit · SBV, {per}'] for n, k in sec]
it = {i['key']: i for i in F['other_services_breakdown']['items']}
reb, ret, con = it['re_business']['value_bn'], it['re_total']['value_bn'], it['consumer']['value_bn']
own = ret - reb
for n, v, note in [('Real-estate business', reb, f"MoC, {it['re_business']['as_of']}"),
                   ('≈ Own-use housing loans', own, 'derived: total RE credit − RE business'),
                   ('≈ Other consumer loans', con - own, 'derived: consumer credit − own-use housing'),
                   ('≈ Unclassified (finance, hospitality, education, health…)', os_ - reb - con, 'residual; parts from different months, so an estimate')]:
    rows.append(['Other services', n, round(v / 1000, 1), f'{v / os_ * 100:.0f}% of other services · {note}'])
write('credit_sankey.csv', ['Source', 'Target', 'Value (tn VND)', 'Note'], rows)

# 3 · Provinces scatter: GRDP per capita (USD) vs fertility, region from the page's PROVS list
html = open(os.path.join(ROOT, 'vietnam_dashboard.html'), encoding='utf-8').read()
reg = {m.group(1): int(m.group(2)) for m in re.finditer(r"n:'([^']+)',\s*v:(\d)", html)}
RN = ['', 'Northern midlands & mountains', 'Red River Delta', 'North Central', 'South Central Coast & Central Highlands', 'Southeast', 'Mekong Delta']
G, D, rows = S['SOC_GRDP'], S['SOC_PROV_DATA'], []
for p, d in D.items():
    g = G.get(p, {})
    rows.append([p, (g.get('grdp_pc_usd') or {}).get('2025'), d.get('tfr'), RN[reg[p]] if reg.get(p) else '',
                 round(d['pop'] / 1e6, 2), (g.get('growth') or {}).get('2025'), d.get('urban_pct')])
write('provinces_scatter.csv', ['Province', 'GRDP per capita 2025 (USD)', 'Fertility rate 2024 (children per woman)', 'Region',
                                'Population 2025 (m)', 'GRDP growth 2025 (%)', 'Urban share (%)'], rows)
