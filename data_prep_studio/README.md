# Data Prep Studio

A guided, self-service ETL tool for non-technical users, built in Python.

**Import → Review → Transform → Output → Export / Push**

```
pip install -r requirements.txt
streamlit run app.py
```

## What each step does

| Step | What the user can do |
|---|---|
| **1 · Import** | Upload CSV, JSON / JSON Lines or Excel (.xlsx), or pull from an API (GET/POST; bearer token, API-key header, or username/password). Unreadable files and failed requests produce plain-language errors with a hint on what to try. |
| **2 · Review** | See row/field counts, examples and a data-quality table (missing, invalid, extra-space counts). Rename fields, change detected types (text, integer, decimal, date, boolean), untick fields to drop them. Choose how unreadable values are handled. The original data is shown side by side and is never modified. |
| **3 · Transform** | Add steps from a form: filter rows, select/reorder fields, standardise text, standardise date format, handle missing values, remove duplicates, calculated field (arithmetic or join text). Reorder or delete steps. A plain-English summary is shown before **Run**. Settings can be saved/loaded as a recipe file. |
| **4 · Output** | Step-by-step log, success/failure banner, source-vs-output counts, per-field empty-value comparison, preview, the unreadable values, and the rows that couldn't be processed (with the reason, downloadable). Go back, adjust, and **Re-run** without re-uploading. A warning appears if settings changed since the last run. |
| **5 · Export** | Download CSV or JSON, or send to an application API: choose which fields to send and what to call them, optional wrapper key, batch size, auth, sample of the exact payload, an explicit confirmation, then a clear success/failure result with the status, response text and what to try. |

## Design choices worth knowing

* **The source is never changed.** It is loaded once as text; everything else works on copies. A test asserts this.
* **Nothing is silently dropped.** Rows removed *on purpose* (filters, duplicates) are counted separately from rows that *couldn't be processed*, which are set aside with a reason.
* **Types are explicit.** Auto-detection suggests, the user confirms. Columns with leading zeros (zip codes, IDs) stay text. Detection uses a sample of up to 5,000 rows; the full data is then checked, and anything that doesn't fit is flagged.
* **Dates:** "03/04/2024" is ambiguous, so there is a day-first switch in *Detection settings*.
* **Delivery is safe by default:** the sample payload is viewable, sending needs a tick-box confirmation, and it stops after 3 failed requests in a row. Tokens are masked in every error message.
* **Credentials and data stay in the browser session.** Nothing is written to disk.

## Try the API features without a real service

```
python mock_api.py          # terminal 1: http://127.0.0.1:8099, token: demo-token
streamlit run app.py        # terminal 2
```
*Import → Connect to an API*: URL `http://127.0.0.1:8099/customers`, Bearer token `demo-token`.
*Export → Send*: URL `http://127.0.0.1:8099/ingest` (records need `id` and `name`, so map two fields to those names; mapping others without them shows the 422 error handling).
Sample messy data to practise on: `sample_data/customers.csv`.

## Files

| File | Purpose |
|---|---|
| `app.py` | The interface (Streamlit). |
| `etl_core.py` | All data logic; no UI code, usable from scripts too. |
| `mock_api.py` | Tiny local API for trying import/push. |
| `test_etl_core.py` | 37 engine tests (loading, typing, every transformation, export, API import/push against the mock). |
| `test_app_smoke.py` | 9 end-to-end UI journeys using a stand-in for Streamlit (see below). |

Run the tests: `python -m unittest -v`

## Honest limits

* **Verified:** engine and user journeys with Python 3.12 / pandas 3.0.2. The UI tests use a *stand-in* for Streamlit that enforces some of its real rules (widget values vanish when not drawn; no assigning to a widget key after it exists; unique keys). **The app has not been opened in a real browser by its author**, so expect to find cosmetic or version-specific issues on first run. pandas 2.x and other Streamlit versions weren't tested.
* Performance: 200,000 rows × 10 fields (15 MB CSV) loads and runs a 4-step job in a few seconds. Data is held in memory, so file size is bounded by RAM.
* Not included yet: API pagination, old `.xls` files, European number formats like `1.234,56`, scheduling, undo/redo inside a step, resuming a partially failed push (re-sending resends accepted records).
