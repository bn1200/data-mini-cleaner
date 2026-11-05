# Data Prep Studio

A guided Streamlit workspace for importing, profiling, cleaning, and exporting tabular data.

## Run locally

From the project root:

```powershell
uv sync
uv run streamlit run data_prep_studio/app.py
```

To try the sample API, start `python data_prep_studio/mock_api.py` in another terminal. Connect to `http://127.0.0.1:8099/customers` using Bearer authentication with the demo token `demo-token`.

The sample dataset is `data_prep_studio/sample_data/customers.csv`.

## Deploy to Render

Connect this repository to Render and create a new **Blueprint** using `render.yaml`. The Blueprint installs the app dependencies, binds Streamlit to Render's `$PORT`, checks `/_stcore/health`, and uses the free web-service plan.

## Workflow

1. **Import** CSV, JSON, JSON Lines, Excel `.xlsx`, or fetch JSON records from an API.
2. **Review** the detected schema and data-quality profile; rename, retype, or exclude fields.
3. **Transform** with filters, field selection, text/date standardisation, missing-value handling, deduplication, and calculated fields. Steps can be reordered, removed, and saved as reusable recipes.
4. **Output** previews results, logs row changes, and makes unreadable rows available separately.
5. **Export** CSV/JSON or send mapped records to an API after reviewing the payload and confirming the send.

The uploaded source is not modified. Unreadable rows are set aside by default, API credentials remain in the active app session, and API sending stops after three consecutive failed requests.

## Tests

```powershell
Set-Location data_prep_studio
..\.venv\Scripts\python.exe -m unittest -v
```
