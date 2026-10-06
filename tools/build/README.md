# Build tooling (copied from the session scratchpad, 2026-10-06)

Research JSON and builder scripts used to generate the data blocks embedded in `vietnam_dashboard.html`.
Paths inside the scripts still point at the old scratchpad (`/private/tmp/...`); adjust before re-running.

- `i18n/merge.py` — merges `en_exact_*.json` + `en_extra.json` → `i18n_en.json` and embeds it into the HTML (paths need updating).
- `econ/story_econ.js` — source of the Economy "story" renderer (now inlined in the HTML).
- `bank/`, `policy/`, `econ/` — builders for FINSYS, POLICY, ECONFLOW data objects.
