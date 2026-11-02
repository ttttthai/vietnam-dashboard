"""Export the datasets behind the dashboard's Flourish charts as CSV (one file per visualisation).

Flourish visualisations (created 2026-10-06 via the Flourish connector; publish them in Flourish to embed).
On the page (data/flourish.json, after the 2026-10-06 chart audit, data/research/chart_audit.md):
  30475882  34 provinces: GRDP per person vs GRDP growth, 2020 → 2024 → 2025 (scatter, time slider)
  30476438  GDP by economic activity, % of GDP at current prices, 2010–2025       (bar chart race)
Retired from the page (duplicate a native chart; drafts stay in the Flourish account, ids in _meta.retired):
  30475879 30475880 30476086 30476088 30476092 30476094 30476096 30476097 30476098 30476099 30476100 30476104
  Their export blocks (#1, #2, #4–#13) are kept so the CSVs stay reproducible if anyone reuses the drafts elsewhere.
Registry of all charts (tab, story chapter, URLs): data/flourish.json.

Usage:  python3 tools/build/flourish_export.py [out_dir]   (default: data/flourish/)
Then upload each CSV (30476438 also takes gdp_by_activity_race_captions.csv as its "captions" dataset) to its visualisation (Flourish connector: flourish_update_visualisation_data,
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

# 3 · Provinces scatter with a time slider (30475882): one row per province × year, years 2020, 2024, 2025.
# Only metrics that exist for every slider year (SOC_GRDP): GRDP per person in USD (x), real GRDP growth % (y),
# average population (size). Fertility is NOT on the slider: TFR exists for 2020 (PROV_PREV) and 2024 (SOC_PROV_DATA)
# only, so it would have no 2025 frame. Region (colour + filter) from the page's PROVS list.
html = open(os.path.join(ROOT, 'vietnam_dashboard.html'), encoding='utf-8').read()
reg = {m.group(1): int(m.group(2)) for m in re.finditer(r"n:'([^']+)',\s*v:(\d)", html)}
RN = ['', 'Northern midlands & mountains', 'Red River Delta', 'North Central', 'South Central Coast & Central Highlands', 'Southeast', 'Mekong Delta']
G, D, rows = S['SOC_GRDP'], S['SOC_PROV_DATA'], []
SC_YEARS = ['2020', '2024', '2025']
SC_STATUS = {'2020': 'official', '2024': 'preliminary', '2025': 'estimate'}
for y in SC_YEARS:
    for p in D:
        g = G[p]
        rows.append([p, y, g['grdp_pc_usd'][y], g['growth'][y], round(g['pop_avg_thousand'][y] / 1000, 2),
                     RN[reg[p]] if reg.get(p) else '', g['grdp_pc_mvnd'][y], SC_STATUS[y]])
write('provinces_scatter.csv', ['Province', 'Year', 'GRDP per person (USD)', 'Real GRDP growth (%)', 'Average population (m)',
                                'Region', 'GRDP per person (million VND)', 'NSO status'], rows)
for y in SC_YEARS:   # numbers behind the title/subtitle (printed so the text can be re-checked after a data update)
    v = sorted(((G[p]['grdp_pc_usd'][y], p) for p in D), reverse=True)
    print(f'  scatter {y}: richest {v[0][1]} {v[0][0]}, poorest {v[-1][1]} {v[-1][0]}, ratio {v[0][0] / v[-1][0]:.1f}x')

# ── Flagship charts added 2026-10-06 (one block per visualisation; ids in data/flourish.json) ──
import datetime as _dt
POL = load('policy.json')['POLICY']
DIR = json.load(open(os.path.join(ROOT, 'strategy_directives.json'), encoding='utf-8'))
MAC = load('invest_macro.json')['MACRO']
EN = json.loads(re.search(r'<script id="i18n-en" type="application/json">(.*?)</script>', html, re.S).group(1))['exact']
yv = lambda s, y: s['base'][s['years'].index(y)] if y in s['years'] else None   # projection value for year y (None if absent)
blank = lambda v: '' if v is None else v
qk = lambda s: (int(s[:4]), (int(s[5:7]) - 1) // 3)                      # ISO date → (year, quarter index)

# 4 · GDP per capita (USD): NSO actuals, IMF WEO path from the last actual, 2030 official target (single point)
gp, ip, tp = EO['gdppc_usd'], P['gdp_per_capita_usd_imf'], P['gdp_per_capita_usd_target']
rows = [[EO['y0'] + i, v, '', ''] for i, v in enumerate(gp)]
rows[-1][2] = rows[-1][1]
for y in range(rows[-1][0] + 1, 2031): rows.append([y, '', blank(yv(ip, y)), blank(yv(tp, y))])
write('gdp_per_capita.csv', ['Year', 'Actual (NSO)', 'IMF WEO Apr-2026', 'Official 2030 target'], rows)

# 5 · CPI annual average: NSO actuals 2015–2025, institution forecasts to 2028, 2026 target, Jan–Sep 2026 average
cc = [('IMF WEO Apr-2026', 'cpi_pct_imf'), ('World Bank May-2026', 'cpi_pct_wb'), ('ADB Sep-2026', 'cpi_pct_adb'), ('AMRO 2026', 'cpi_pct_amro')]
rows = [[EO['y0'] + i, v] + [''] * (len(cc) + 2) for i, v in enumerate(EO['cpi_avg']) if EO['y0'] + i >= 2015]
for j in range(len(cc)): rows[-1][2 + j] = rows[-1][1]
for y in range(rows[-1][0] + 1, 2029):
    r = [y, ''] + [blank(yv(P[k], y)) for _, k in cc] + [blank(yv(P['cpi_pct_target'], y)), EO['ytd_2026']['cpi_avg_9M'] if y == 2026 else '']
    rows.append(r)
write('cpi.csv', ['Year', 'Actual (NSO)'] + [c for c, _ in cc] + ['Official 2026 target (~4.5%)', 'Jan–Sep 2026 average (NSO)'], rows)

# 6 · Population (million): NSO average population, GSO/UNFPA 2019-based and UN WPP 2024 medium variants (5-year points)
PS = S['POP_SERIES']; yrs = sorted({int(y) for y in PS['actual']} | {int(y) for y in PS['un']['medium']})
rows = [[y, blank(PS['actual'].get(str(y))), blank(PS['gso']['medium'].get(str(y))), blank(PS['un']['medium'].get(str(y)))] for y in yrs]
write('population.csv', ['Year', 'Actual (NSO)', 'GSO/UNFPA projection (medium)', 'UN WPP 2024 (medium)'], rows)

# 7 · SBV policy rates as step series (each value holds until the next change; last date = data as-of)
rk = [('Refinancing rate', 'refinancing_rate'), ('Rediscount rate', 'rediscount_rate'), ('SBV overnight lending rate', 'overnight_rate'), ('OMO rate', 'omo_rate')]
ser = {k: dict(zip(i['series']['dates'], i['series']['values'])) for i in POL['mon']['instruments'] for _, k in rk if i['id'] == k}
rows = []
for d in sorted({d for s in ser.values() for d in s}):
    r = [d]
    for _, k in rk:
        past = [dd for dd in ser[k] if dd <= d]
        r.append(ser[k][max(past)] if past else '')
    rows.append(r)
write('policy_rates.csv', ['Date'] + [n for n, _ in rk], rows)

# 8 · Policy moves per quarter 2023–2026 by side and direction (tightening negative); neutral moves excluded, as on the page
H = [(h, side) for side in ('mon', 'fis') for i in POL[side]['instruments'] for h in i.get('history', [])]
rows = []
for y in range(2023, 2027):
    for q in range(4):
        c = {(s, d): 0 for s in ('mon', 'fis') for d in ('easing', 'tightening')}
        for h, s in H:
            if h['direction'] in ('easing', 'tightening') and int(h['date'][:4]) == y and (int(h['date'][5:7]) - 1) // 3 == q: c[(s, h['direction'])] += 1
        lab = f'Q{q + 1} {y}' + (' (incl. scheduled)' if any(h['date'] > POL['mon']['as_of'] and qk(h['date']) == (y, q) for h, _ in H) else '')
        rows.append([lab, c[('mon', 'easing')], c[('fis', 'easing')], -c[('mon', 'tightening')], -c[('fis', 'tightening')]])
write('policy_moves.csv', ['Quarter', 'Monetary easing', 'Fiscal easing', 'Monetary tightening', 'Fiscal tightening'], rows)

# 9 · Total credit / GDP: derived actuals (SBV credit / NSO nominal GDP) and the dashboard scenario band to 2030
A, cg = F['annual'], F['projections']['series']['credit_to_gdp_pct']
rows = []
for y, c in zip(A['years'], A['credit']):
    g = EO['gdp_vnd_bn'][y - EO['y0']] if 0 <= y - EO['y0'] < len(EO['gdp_vnd_bn']) else None
    if c and g: rows.append([y, round(c / g * 100, 1), '', '', ''])
rows[-1][2:5] = [rows[-1][1]] * 3
for y in cg['years']: rows.append([y, '', cg['base'][cg['years'].index(y)], cg['low'][cg['years'].index(y)], cg['high'][cg['years'].index(y)]])
write('credit_to_gdp.csv', ['Year', 'Actual (≈ derived)', 'Scenario: base (credit +15%/yr)', 'Scenario: low (+12%/yr)', 'Scenario: high (+18%/yr)'], rows)

# 10 · Credit growth vs deposit growth, % a year, 2015–2025
rows = [[y, c, d] for y, c, d in zip(A['years'], A['credit_growth'], A['deposit_growth'])]
write('credit_deposit_growth.csv', ['Year', 'Credit growth', 'Deposit growth'], rows)

# 11 · Tracked documents per quarter by issuing level, from Q3 2024 (as the page's wave chapter)
LV = [('Party', 'party'), ('National Assembly', 'assembly'), ('Government', 'government'), ('Ministries', 'ministry'), ('Local government', 'local')]
DD = [d for d in DIR['directives'] if d.get('date') and d['date'] >= '2024-07-01']
last = qk(DIR['as_of']); rows = []; y, q = 2024, 2
while (y, q) <= last:
    rows.append([f'Q{q + 1} {y}' + (' (to ' + _dt.date.fromisoformat(DIR['as_of']).strftime('%-d %b %Y') + ')' if (y, q) == last else '')] + [sum(1 for d in DD if qk(d['date']) == (y, q) and d['level'] == k) for _, k in LV])
    y, q = (y + 1, 0) if q == 3 else (y, q + 1)
while rows and not any(rows[0][1:]): rows.pop(0)                        # drop leading empty quarters
write('documents_per_quarter.csv', ['Quarter'] + [n for n, _ in LV], rows)

# 12 · Drafts in the pipeline: first → latest recorded milestone (Gantt; end = day after the latest milestone)
nxt = lambda s: (_dt.date.fromisoformat(s) + _dt.timedelta(days=1)).isoformat()
rows = []
for d in DIR['directives']:
    dr = d.get('draft')
    if not dr: continue
    ms = sorted(m['date'] for m in dr.get('milestones') or []) or [d['date']]
    lm = max(dr.get('milestones') or [{'date': d['date'], 'event': ''}], key=lambda m: m['date'])
    rows.append([EN.get(d['ref'], d['ref']), ms[0], nxt(ms[-1]), EN.get(dr.get('process_stage'), dr.get('process_stage')),
                 {k: n for n, k in LV}.get(d['level'], d['level']), len(dr.get('milestones') or []),
                 EN.get(lm['event'], ''), EN.get(dr.get('expected') or '', ''),   # English only: untranslated text left blank
                 'Due at the 2nd NA session (17 Oct–20 Nov 2026)' if (dr.get('expected') or '').startswith('Kỳ họp thứ 2') else 'Other or no stated timing'])
rows.sort(key=lambda r: r[1])
write('drafts_gantt.csv', ['Draft', 'First milestone', 'Day after latest milestone', 'Stage', 'Level', 'Milestones recorded', 'Latest milestone', 'Expected', 'Timing'], rows)

# 13 · Public investment disbursed to 30 Sep 2026, % of the 2026 plan, central vs local budgets (MoF)
pi = MAC['public_invest']
rows = [['Central budget', pi['central_pct'], round(pi['central_bn'] / 1000, 1)], ['Local budgets', pi['local_pct'], round(pi['local_bn'] / 1000, 1)],
        ['Total', pi['pct'], round(pi['disbursed_bn'] / 1000, 1)]]
write('public_investment_9m.csv', ['Budget', 'Disbursed (% of 2026 plan)', 'Disbursed (tn VND)'], rows)

# 14 · GDP by economic activity, % of GDP at current prices, 2010–2025 (bar chart race). Source: the page's
# GDP_SUB_OFFICIAL (NSO PxWeb V03.04-05, revised 2021 vintage; 2024 preliminary, 2025 estimate) for the 19 non-farm
# activities, plus agriculture, forestry & fishing as one bar from GDP_SECTORS 'agr' 2010–2025 (NSO V03.02; not split at
# current prices). Taxes less subsidies on products (~8% of GDP) is not an activity, so the bars sum to ~92%, not 100%.
# Category = the page's three sector groups (colours = the page's GDP_SECTORS colours).
GSO = json.loads(re.search(r'const GDP_SUB_OFFICIAL = (\{.*?\});', html).group(1).replace('y0:', '"y0":').replace('data:', '"data":'))
GY = json.loads(re.search(r'const GDP_YEARS\s*=\s*(\[[^\]]*\])', html).group(1))
AGR = json.loads(re.search(r"\{key:'agr'[^}]*?data:(\[[^\]]*\])", html).group(1))
AN = [('mining', 'Mining & quarrying', 'Industry & construction'), ('manufacturing', 'Manufacturing', 'Industry & construction'),
      ('electricity_gas', 'Electricity & gas', 'Industry & construction'), ('water_waste', 'Water supply & waste', 'Industry & construction'),
      ('construction', 'Construction', 'Industry & construction'), ('wholesale_retail', 'Wholesale & retail trade', 'Services'),
      ('transport_storage', 'Transport & storage', 'Services'), ('accommodation_food', 'Hotels & restaurants', 'Services'),
      ('ict', 'Information & communications', 'Services'), ('finance_insurance', 'Finance, banking & insurance', 'Services'),
      ('real_estate', 'Real estate', 'Services'), ('professional_science_tech', 'Professional, science & tech', 'Services'),
      ('admin_support', 'Administrative & support', 'Services'), ('public_admin_defence', 'Public admin, defence & social security', 'Services'),
      ('education', 'Education & training', 'Services'), ('health_social', 'Health & social work', 'Services'),
      ('arts_entertainment', 'Arts & entertainment', 'Services'), ('other_services', 'Other services', 'Services'),
      ('households_employers', 'Households as employers', 'Services')]
y0, n = GSO['y0'], len(GSO['data']['manufacturing'])
yrs = list(range(y0, y0 + n))
assert set(GSO['data']) == {k for k, _, _ in AN}, 'GDP_SUB_OFFICIAL keys changed'
rows = [[nm, cat] + GSO['data'][k] for k, nm, cat in AN]
rows.append(['Agriculture, forestry & fishing', 'Agriculture, forestry & fishing'] + [AGR[GY.index(y)] for y in yrs])
write('gdp_by_activity_race.csv', ['Activity', 'Sector group'] + [str(y) for y in yrs], rows)
rk = lambda j: {r[0]: i + 1 for i, r in enumerate(sorted(rows, key=lambda r: -r[2 + j]))}
r0, r1 = rk(0), rk(n - 1)
for r in sorted(rows, key=lambda r: r1[r[0]]):   # rank and share at both ends (numbers behind the title)
    print(f'  race {r[0]}: #{r0[r[0]]} {r[2]}% ({yrs[0]}) -> #{r1[r[0]]} {r[-1]}% ({yrs[-1]})')
# 14b · Captions for the race (second Flourish dataset "captions"); every number computed from the rows above
R = {r[0]: dict(zip(yrs, r[2:])) for r in rows}
from decimal import Decimal, ROUND_HALF_UP
f1 = lambda v: str(Decimal(str(v)).quantize(Decimal('0.1'), ROUND_HALF_UP))   # half-up, so 4.25 → 4.3
caps = [[2010, 2014, f"2010: manufacturing {f1(R['Manufacturing'][2010])}% of GDP, agriculture {f1(R['Agriculture, forestry & fishing'][2010])}%, mining {f1(R['Mining & quarrying'][2010])}% (#{r0['Mining & quarrying']})"],
        [2015, 2019, f"Mining falls from {f1(R['Mining & quarrying'][2014])}% of GDP in 2014 to {f1(R['Mining & quarrying'][2015])}% in 2015"],
        [2020, 2022, f"COVID: hotels & restaurants drop from {f1(R['Hotels & restaurants'][2019])}% (2019) to {f1(R['Hotels & restaurants'][2021])}% (2021); health peaks at {f1(R['Health & social work'][2021])}%"],
        [2024, '', f"2024 preliminary, 2025 estimate. Manufacturing {f1(R['Manufacturing'][2025])}%, mining #{r1['Mining & quarrying']} at {f1(R['Mining & quarrying'][2025])}%"]]
write('gdp_by_activity_race_captions.csv', ['From', 'To', 'Caption'], caps)
