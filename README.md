# Triennial Report Generator (Databricks Streamlit App)

Generate **publication-ready DOCX** chapter drafts (NIH-style triennial narrative) from a master Excel submission file, with **UID-anchored citations** converted into **numeric footnotes** and a **References** section.

This repo is designed to run as a **Databricks Apps (Streamlit)** application, but it can also run locally.

---

## What this app does

- Loads a **master Excel** workbook containing activity submissions (one row per activity).
- Lets users filter by:
  - **Field** (chapter focus)
  - (optionally) **Fiscal year(s)** or other available filters surfaced in the UI
- Builds a chapter draft with the standard structure:
  1. **Introduction** (concise; citation-free by hard rule in current `app.py`)
  2. **Summary** (concise; citation-free by hard rule in current `app.py`)
  3. **Thematic sections** in a fixed, NIH-aligned ordering
  4. **Acronyms**
  5. **References**
- For each activity row, calls a Databricks Model Serving LLM endpoint to generate a **single paragraph**.
- Enforces **deterministic post-processing** for:
  - sentence completeness and punctuation
  - NIH ICO acronym usage (e.g., *NCI*, *NIAID*, not full institute names)
  - removing percent signs (`%` → `percent`) per client style
  - removing raw UID tokens from prose while preserving footnote markers
  - preventing “citation scaffolding” phrases (“as evidenced by…”, “for example…”, etc.)
- Converts UID markers like `[^378_NIAID]` into stable numeric footnotes (`[^fn1]`, `[^fn2]`, …), deduplicated **per UID**, and builds a **References** list in the same order as first appearance.
- Exports Markdown → DOCX via **Pandoc** using a supplied `reference.docx` for styling.

---

## Repository contents

| File | Purpose |
|---|---|
| `app.py` | Main Streamlit application: UI, filtering, LLM calls, narrative assembly, citation/footnote logic, DOCX export |
| `app.yaml` | Databricks Apps entrypoint (`streamlit run app.py`) and environment settings |
| `requirements.txt` | Python dependencies (Streamlit, Pandas, Requests, Pandoc bundling via `pypandoc-binary`, etc.) |
| `style_prompt.json` | System prompt template used to enforce NIH/triennial house style and structural constraints |
| `reference.docx` | Word reference template used by Pandoc to enforce formatting (fonts, styles, spacing, etc.) |
| `h2_pagebreak.lua` | Optional Pandoc Lua filter to insert a DOCX page break before the 2nd H2 heading |
| `h2_square_bracket_footnotes.lua` | Optional Pandoc Lua filter to render footnote references as `[n]` in text |
| `Triennial Data Source_*.xlsx` | Master data source workbook (example / expected schema) |
| `Triennial_Data_Cancer.docx` | Example generated DOCX output (for validation / review) |
| `Streamlit_web_PDF_Triennial Report Generator.pdf` | UI snapshot / documentation export |

---

## Data expectations (Excel)

`app.py` normalizes columns to the canonical names below (best-effort mapping; exact headers are recommended):

- Submitting ICO  
- Lead ICO  
- Unique ID  
- Collaborating ICOs/Agencies/Orgs  
- Activity Name  
- Activity Description  
- Activity Type  
- Field  
- Importance  
- Web address(es)  
- PMID(s)  
- Notes  
- Notes.1  

> **UID**: The app uses the “Unique ID” value as the stable key for routing activities to sections and building footnotes.

---

## Citation model: how the app prevents “hallucinated” citations

The key design principle is: **the model must explicitly mark which sentences are supported by the activity brief**.

### 1) The model is instructed to append a token
For each activity paragraph, the LLM prompt includes:

- Append `[[CITE]]` **only** at the end of sentences directly supported by the activity brief.
- Do **not** append `[[CITE]]` to bridging / generic context sentences.

### 2) The app deterministically converts tokens to UID markers
`apply_uid_markers_from_cite_tokens()`:

- strips any pre-existing `[^...]` markers (for determinism)
- converts each `[[CITE]]` into `[^<UID>]`, preserving punctuation order
- **does not** fall back to paragraph-end citations if the model did not mark support

### 3) UID markers become numeric footnotes and References
`build_footnotes_from_uid_markers()`:

- scans markdown for `[^<UID>]`
- assigns first-seen UID → `fn1`, `fn2`, …
- replaces `[^UID]` in prose with `[^fn#]`
- generates Pandoc footnote definitions:
  - `[^fn1]: <reference text>`
- generates `## References` list in matching order.

### Reference text selection rules
For a given UID row:

1. Prefer **PMID** if present (PubMed metadata fetched via NCBI E-utilities to create an NLM-ish reference line)
2. Otherwise use the first usable **URL**
3. Otherwise emit a safe fallback (“Source unavailable”)

---

## Pandoc and DOCX export

`export_docx()` writes Markdown to `report.md`, then runs Pandoc with:

- `--reference-doc reference.docx` (if present)
- optional Lua filters:
  - `--lua-filter h2_pagebreak.lua`
  - `--lua-filter h2_square_bracket_footnotes.lua`

### How Pandoc is resolved (no runtime downloads)
`ensure_pandoc()` checks, in order:

1. `pandoc` on PATH
2. `pypandoc-binary` bundled pandoc
3. `$PANDOC_PATH` (if set to a real file)
4. `/tmp/pandoc/bin/pandoc` (legacy baked location)

**Recommended:** keep `pypandoc` and `pypandoc-binary` in `requirements.txt` for Databricks Apps builds.

---

## Running in Databricks Apps

### 1) Repo layout recommendation

Place these files alongside `app.py` (or under `./assets/` — the app will auto-detect):

```
.
├─ app.py
├─ app.yaml
├─ requirements.txt
├─ style_prompt.json
├─ reference.docx
├─ h2_pagebreak.lua
├─ h2_square_bracket_footnotes.lua
└─ Triennial Data Source_Master File of All Submissions_OEPR Ch3 Writers (1).xlsx
```

The app resolves “relative paths” from the app folder (or `./assets/` if that folder exists), then stages files into:

- `/tmp/triennial_assets`
- `/tmp/triennial_out`

### 2) Configure environment variables (optional but recommended)

| Variable | Default | Meaning |
|---|---|---|
| `TRIENNIAL_ENDPOINT` | `databricks-meta-llama-3-3-70b-instruct` | Databricks Model Serving endpoint name |
| `TRIENNIAL_TEMPERATURE` | `0.25` | LLM temperature (some sections override with smaller temps) |
| `TRIENNIAL_MAX_TOKENS_ROW` | `550` | Token cap for row generation (legacy; the app also uses `ROW_MAX_TOKENS`) |
| `PANDOC_PATH` | *(unset)* | Explicit pandoc binary (if not using pypandoc-binary) |
| `STREAMLIT_GATHER_USAGE_STATS` | `false` (in `app.yaml`) | Disable Streamlit telemetry |

---

## Running locally

### 1) Install dependencies
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2) Run Streamlit
```bash
streamlit run app.py
```

### 3) Ensure Pandoc is available
- Easiest: rely on `pypandoc-binary` (already in requirements)
- Or install Pandoc system-wide and ensure `pandoc` is on PATH
- Or set `PANDOC_PATH=/path/to/pandoc`

---

## Output

The app produces:

- A generated DOCX named:
  - `Triennial_Data_<Field>.docx`
- A staged Markdown file:
  - `report.md` (in the output staging directory)

When running in Databricks, the app can also publish output to DBFS (if enabled in the UI path settings) and/or provide a direct Streamlit download button.

---

## Design constraints enforced in code

- **No acronym expansion in narrative text** (client requirement)
- **No “Pandoc” / “pypandoc” mentions** in generated narrative (hard scrub)
- **No inline URLs** in prose (citations appear only as footnotes)
- **UIDs must not appear in prose**, only inside footnote markers
- **Sentence-terminal citations** (not paragraph-terminal), driven by `[[CITE]]` tokens
- Deterministic cleanup to avoid truncated or ungrammatical endings

---

## Troubleshooting

### Pandoc not found
If DOCX export fails with “Pandoc is required…”:
- Ensure `pypandoc` + `pypandoc-binary` are installed (recommended)
- Or install Pandoc and verify `pandoc --version`
- Or set `PANDOC_PATH`

### PubMed reference text missing
If NLM-ish formatting cannot be fetched:
- Network access to NCBI E-utilities may be blocked.
- The app falls back to a canonical PubMed URL reference line.

### Citations appear too often / too rarely
This is governed by the LLM’s use of `[[CITE]]`. If needed:
- tighten the row prompt constraints (in `build_row_paragraph_prompt`)
- add auditing in the UI for `[[CITE]]` token counts per paragraph

---

## Security / privacy notes

- The app may call external NCBI endpoints (PubMed E-utilities) to format PMID references.
- Do not include confidential internal URLs unless you intend them to appear in footnotes.

---

## License

Add your project’s chosen license here (e.g., MIT, Apache-2.0, proprietary/internal).
