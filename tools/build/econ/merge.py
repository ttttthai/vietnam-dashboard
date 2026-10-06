import json
P = json.load(open('part1.json'))
R = json.load(open('remit/remit.json'))
B = json.load(open('budget/budget.json'))
NB = json.load(open('nso_budget.json'))

YEARS = P['bop']['years']  # 2015..2025
ry = R['years']


def pick(series, years=YEARS):
    return [series[ry.index(y)] if y in ry else None for y in years]


# ---- gdp_exp ----
g = P['gdp_exp']
chk = g.pop('check_X_minus_M_vs_NSO_net_exports_bn')
g['qa'] = {'X_minus_M_minus_NSO_net_exports_bn': chk,
           'note': 'X and M (World Bank WDI) reproduce NSO exactly for 2015-2023 and 2025; for 2024 WDI holds an older vintage (X-M = 742,103 vs NSO revised net exports 801,814; WDI discrepancy 65,428 vs NSO 5,716). Use net_exports/discrepancy (NSO) for the identity; 2024 X and M are flagged.'}
g['flags'] = {'X': {'2024': 'older vintage than NSO V03.08'}, 'M': {'2024': 'older vintage than NSO V03.08'}}

# ---- bop ----
bop = P['bop']
s = bop['series']
s['remit_in'] = pick(R['remit_wb_busd'])
bop['sources']['remit_in'] = 'https://data360api.worldbank.org/data360/data?DATABASE_ID=WB_KNOMAD&INDICATOR=WB_KNOMAD_MRI&REF_AREA=VNM (World Bank/KNOMAD remittance inflows, current vintage; 2024-2025 actuals not available -> null; see remittances block for SBV and HCMC figures)'
bop['sources']['remit_out'] = 'not published as a separate series (IMF BOP gives secondary_income_out total only)'
bop['external_debt_service']['mof'] = {
    'years': ry,
    'gross_service_busd': R['ext_debt_service_busd'], 'principal_busd': R['ext_debt_principal_busd'], 'interest_busd': R['ext_debt_interest_busd'],
    'service_pct_exports': R['ext_debt_service_pct_exports'],
    'service_pct_exports_old_definition': R.get('ext_debt_service_pct_exports_old_definition'),
    'government_service_busd': R['gov_ext_debt_service_busd'], 'government_principal_busd': R['gov_ext_debt_principal_busd'], 'government_interest_busd': R['gov_ext_debt_interest_busd'],
    'guaranteed_service_busd': R['guaranteed_ext_debt_service_busd'], 'guaranteed_principal_busd': R['guaranteed_ext_debt_principal_busd'], 'guaranteed_interest_busd': R['guaranteed_ext_debt_interest_busd'],
    'note': "MoF public-debt bulletins, national external debt table 'TỔNG TRẢ NỢ TRONG KỲ' = GROSS repayment incl. rolled-over short-term debt (principal ~93-149 bn/yr): not comparable with WB IDS. Ratio to exports excludes short-term principal (definition change 2021). 2024 preliminary; 2015 and 2025 full-year not found.",
    'sources': {k: v for k, v in R['sources'].items() if k.startswith(('ext_debt', 'gov_ext', 'guaranteed'))},
}

# ---- remittances ----
remittances = {
    'years': ry,
    'national_sbv_busd': R['remit_sbv_busd'],
    'national_wb_knomad_busd': R['remit_wb_busd'],
    'national_wb_projection_busd': R.get('remit_wb_projection_busd'),
    'national_wb_older_vintage_press': R.get('remit_wb_older_vintage_press'),
    'hcmc_busd': R['remit_hcmc_busd'],
    'partial_2026': R.get('partial_2026'),
    'unit': 'bn USD',
    'sources': {k: v for k, v in R['sources'].items() if k.startswith('remit')},
    'notes': [n for n in R['notes'] if n.startswith('remit')],
}

# ---- labour export ----
labour = {
    'years': ry,
    'workers_sent': R['workers_sent'],
    'remit_est_busd': [None] * len(ry),
    'remit_statements': R['worker_remit_statements'],
    'workers_stock_statements': R['workers_stock'],
    'partial_2026': {'workers_sent_8M2026': 90319, 'plan_2026': 112000},
    'sources': {k: v for k, v in R['sources'].items() if k.startswith(('workers', 'worker_'))},
    'notes': ['remit_est_busd left null: DOLAB/MOLISA/MoHA only state ranges (3-3.5 bn in 2018, 3.5-4 bn in 2023-24, 6.5-7 bn in Oct 2025), see remit_statements.'] +
             [n for n in R['notes'] if n.startswith(('workers', 'worker_'))],
}

# ---- budget ----
budget = {
    'years': B['years'], 'basis': B['basis'], 'unit': 'bn VND',
    'revenue': B['revenue'], 'expenditure': B['expenditure'],
    'principal_repayment': B['principal_repayment'], 'deficit': B['deficit'], 'deficit_pct_gdp': B['deficit_pct_gdp'],
    'total_borrowing': B['total_borrowing'], 'extra': B['extra'],
    'nso_yearbook_breakdown': NB,
    'growth_9M2026': {'revenue_9M_bn': 2187300.0, 'revenue_9M_pct_of_plan': 86.5, 'revenue_yoy_pct': 12.1, 'domestic_revenue_9M_bn': 1880300.0,
                      'expenditure_9M_bn': 1873700.0, 'expenditure_9M_pct_of_plan': 59.3, 'expenditure_yoy_pct': 14.5,
                      'source': P['gdp_exp']['growth_real_pct']['source']},
    'sources': B['sources'],
    'notes': B['notes'],
}

notes = [
    'All values copied from official tables/releases or computed only by unit conversion/ratios (pct_gdp, *_net = liabilities - assets); null = not published/not found.',
    'GDP by expenditure: NSO PxWeb V03.08 (updated 15/08/2026); 2024 = Sơ bộ, 2025 = Ước tính. NSO publishes only net exports; gross X and M from World Bank WDI (which reproduces NSO).',
    "invest_by_owner is NSO 'vốn đầu tư thực hiện toàn xã hội' (realised investment, incl. non-GFCF items) — larger than GFCF; do not mix with gdp_exp.I.",
    'BOP: IMF BOP (BPM6) annual 2015-2025 and quarterly 2025Q1-2026Q1 (2026Q2 not yet published by IMF/SBV as of 2026-10-05). IMF matches SBV/NSO V03.20 for 2025 and most years; 2021 differs (revisions) — see bop.sbv_nso_v0320.',
    'Vietnam BOP reports primary income only as a total (mostly FDI profits/interest paid abroad); compensation of employees and investment income splits, and charges for IP, are not published -> null.',
    "currency_deposits_assets (residents' net acquisition of foreign currency & deposits, incl. household FX/cash holdings) is the main 'leakage' item (12-21 bn USD/yr 2022-2025), alongside large negative errors_omissions.",
    'Gross loan drawdowns vs principal repayments are not published in the IMF BOP; loans_net is net incurrence. MoF gross debt-service and WB IDS principal/interest are in bop.external_debt_service.',
    'Travel/transport by type come from NSO trade-in-services statistics (V09.15), whose totals differ from BOP services.',
    'Budget: 2018-2024 final accounts (quyết toán), 2025 MoF estimate (Apr 2026), 2026 plan (NQ 245/2025/QH15). VAT/CIT/special-consumption national splits are not published (revenue reported by ownership sector) -> null.',
]

out = {'gdp_exp': g, 'invest_by_owner': P['invest_by_owner'], 'bop': bop, 'tourism': P['tourism'],
       'remittances': remittances, 'labour_export': labour, 'budget': budget, 'notes': notes,
       'meta': {'compiled': '2026-10-05', 'builder': 'build_part1.py + build_nsobudget.py + merge.py'}}
json.dump(out, open('econ_flows.json', 'w'), ensure_ascii=False, indent=1)
print('ok')
