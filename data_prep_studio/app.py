from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from typing import Any

import pandas as pd
import streamlit as st

import etl_core as etl


PAGES = ["1 · Import", "2 · Review", "3 · Transform", "4 · Output", "5 · Export"]
TYPE_OPTIONS = list(etl.SUPPORTED_TYPES)


def _initialise() -> None:
    defaults: dict[str, Any] = {
        "nav": PAGES[0],
        "load": None,
        "schema_specs": [],
        "schema_df": pd.DataFrame(columns=["Source field", "New name", "Type", "Keep"]),
        "schema_ver": 0,
        "steps": [],
        "sticky": {"dayfirst": False, "tokens_text": ", ".join(etl.DEFAULT_MISSING_TOKENS),
                   "invalid_handling": "reject"},
        "result": None,
        "result_signature": "",
        "import_error": "",
        "recipe_ver": 0,
        "map_ver": 0,
        "mapping_df": None,
        "push_result": None,
        "push_sticky": {"url": "", "auth_kind": "none", "token": "", "header_name": "",
                        "header_value": "", "username": "", "password": "", "wrapper": "",
                        "batch": 100},
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _signature() -> str:
    sticky = st.session_state.sticky
    content = {
        "schema": st.session_state.schema_df.to_dict(orient="records"),
        "steps": st.session_state.steps,
        "dayfirst": sticky["dayfirst"],
        "tokens": sticky["tokens_text"],
        "invalid_handling": sticky["invalid_handling"],
    }
    return hashlib.sha256(json.dumps(content, sort_keys=True, default=str).encode()).hexdigest()


def _tokens() -> list[str]:
    return [token.strip() for token in st.session_state.sticky["tokens_text"].split(",")]


def _specs() -> list[etl.ColumnSpec]:
    frame = st.session_state.schema_df
    return [
        etl.ColumnSpec(
            source=str(row["Source field"]),
            name=str(row["New name"]),
            detected=str(row["Detected"]),
            dtype=str(row["Type"]),
            keep=bool(row["Keep"]),
        )
        for _, row in frame.iterrows()
    ]


def _schema_changed() -> None:
    key = f"schema_{st.session_state.schema_ver}"
    edits = st.session_state.get(key, {}).get("edited_rows", {})
    frame = st.session_state.schema_df.copy()
    for row_index, changes in edits.items():
        for column, value in changes.items():
            if column in frame.columns and row_index in frame.index:
                frame.at[row_index, column] = value
    st.session_state.schema_df = frame


def _mapping_changed() -> None:
    key = f"mapping_{st.session_state.map_ver}"
    edits = st.session_state.get(key, {}).get("edited_rows", {})
    frame = st.session_state.mapping_df.copy()
    for row_index, changes in edits.items():
        for column, value in changes.items():
            if column in frame.columns and row_index in frame.index:
                frame.at[row_index, column] = value
    st.session_state.mapping_df = frame


def _set_page(page: str) -> None:
    st.session_state.nav = page


def _export_back() -> None:
    state = st.session_state
    sticky = state.push_sticky
    sticky["url"] = state.get("w_push_url", sticky["url"])
    sticky["auth_kind"] = state.get("w_push_auth_kind", sticky["auth_kind"])
    for name in ("token", "header_name", "header_value", "username", "password"):
        sticky[name] = state.get(f"w_push_{name}", sticky[name])
    sticky["wrapper"] = state.get("w_push_wrapper", sticky["wrapper"])
    sticky["batch"] = state.get("w_push_batch", sticky["batch"])
    state.nav = PAGES[3]


def _start_over() -> None:
    state = st.session_state
    state.load = None
    state.schema_specs = []
    state.schema_df = pd.DataFrame(columns=["Source field", "New name", "Detected", "Type", "Keep"])
    state.steps = []
    state.result = None
    state.result_signature = ""
    state.import_error = ""
    state.mapping_df = None
    state.push_result = None
    state.nav = PAGES[0]


def _run_job() -> None:
    state = st.session_state
    state.result = etl.run_job(
        state.load.df.copy(deep=True),
        _specs(),
        list(state.steps),
        missing_tokens=_tokens(),
        dayfirst=bool(state.sticky["dayfirst"]),
        invalid_handling=state.sticky["invalid_handling"],
    )
    state.result_signature = _signature()
    state.nav = PAGES[3]


def _delete_step(index: int) -> None:
    steps = st.session_state.steps
    if 0 <= index < len(steps):
        steps.pop(index)
        st.session_state.steps = steps


def _move_step(index: int, offset: int) -> None:
    steps = st.session_state.steps
    target = index + offset
    if 0 <= index < len(steps) and 0 <= target < len(steps):
        steps[index], steps[target] = steps[target], steps[index]
        st.session_state.steps = steps


def _add_step() -> None:
    state = st.session_state
    kind = state.get("nb_kind_0", "dedupe")
    columns = list(state.schema_df.loc[state.schema_df["Keep"], "New name"])
    if kind == "dedupe":
        step = etl.make_step("dedupe", columns=[], keep="first")
    elif kind == "filter":
        step = etl.make_step(
            "filter", column=state.get("nb_fcol_0", columns[0] if columns else ""),
            op=state.get("nb_fop_0", "equals"), value=state.get("nb_fval_text_0", ""),
            mode=state.get("nb_fmode_0", "keep"),
        )
    elif kind == "select":
        selected = state.get("nb_scols_0", columns)
        step = etl.make_step("select", columns=list(selected))
    elif kind == "text":
        step = etl.make_step("text", columns=list(state.get("nb_tcols_0", [])),
                             ops=list(state.get("nb_tops_0", ["trim"])))
    elif kind == "date_format":
        step = etl.make_step("date_format", columns=list(state.get("nb_dcols_0", [])),
                             format=state.get("nb_dformat_0", "%Y-%m-%d"))
    elif kind == "missing":
        step = etl.make_step("missing", columns=list(state.get("nb_mcols_0", [])),
                             action=state.get("nb_maction_0", "fill_value"),
                             value=state.get("nb_mvalue_0", ""))
    else:
        mode = state.get("nb_cmode_0", "arithmetic")
        if mode == "concat":
            step = etl.make_step("calc", name=state.get("nb_cname_0", ""),
                                 mode="concat", columns=list(state.get("nb_ccols_0", [])),
                                 separator=state.get("nb_csep_0", " "))
        else:
            right_kind = state.get("nb_crk_0", "number")
            step = etl.make_step(
                "calc", name=state.get("nb_cname_0", ""), mode="arithmetic",
                left=state.get("nb_cleft_0", columns[0] if columns else ""),
                op=state.get("nb_cop_0", "+"), right_kind=right_kind,
                right=(state.get("nb_cright_col_0", columns[0] if columns else "")
                       if right_kind == "column" else state.get("nb_cnum_0", "0")),
                decimals=state.get("nb_cdecimals_0", 2),
            )
    state.steps = [*state.steps, step]


def _recipe_apply() -> None:
    state = st.session_state
    upload = state.get("recipe_upload")
    if upload is None:
        state.recipe_error = "Choose a recipe file first."
        return
    try:
        raw_recipe = upload.getvalue()
        text = raw_recipe.decode("utf-8-sig") if isinstance(raw_recipe, bytes) else str(raw_recipe)
        recipe = json.loads(text)
        specs, steps, opts, _warnings = etl.recipe_from_dict(recipe, _specs())
        state.schema_specs = specs
        state.schema_df = pd.DataFrame([
            {"Source field": spec.source, "New name": spec.name, "Detected": spec.detected,
             "Type": spec.dtype, "Keep": spec.keep}
            for spec in specs
        ])
        state.steps = steps
        state.sticky["tokens_text"] = ", ".join(opts["missing_tokens"])
        state.sticky["dayfirst"] = opts["dayfirst"]
        state.sticky["invalid_handling"] = opts["invalid_handling"]
        state.schema_ver += 1
        state.recipe_applied = True
        state.recipe_error = ""
    except (etl.ETLError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        state.recipe_error = exc.message if isinstance(exc, etl.ETLError) else "The recipe file isn't valid JSON."


def _auth_config(prefix: str) -> etl.AuthConfig:
    state = st.session_state
    kind = state.get(f"w_{prefix}_auth_kind", "none")
    return etl.AuthConfig(
        kind=kind,
        token=state.get(f"w_{prefix}_token", ""),
        header_name=state.get(f"w_{prefix}_header_name", ""),
        header_value=state.get(f"w_{prefix}_header_value", ""),
        username=state.get(f"w_{prefix}_username", ""),
        password=state.get(f"w_{prefix}_password", ""),
    )


def _mapping() -> dict[str, str]:
    frame = st.session_state.mapping_df
    if frame is None:
        return {}
    return {
        str(row["Source field"]): str(row["Output field"]).strip() if bool(row["Send"]) else ""
        for _, row in frame.iterrows()
    }


def _load_schema(df: pd.DataFrame) -> None:
    specs = etl.infer_schema(df)
    state = st.session_state
    state.schema_specs = specs
    state.schema_df = pd.DataFrame([
        {"Source field": spec.source, "New name": spec.name, "Detected": spec.detected,
         "Type": spec.dtype, "Keep": spec.keep}
        for spec in specs
    ])
    state.schema_ver += 1
    state.steps = []
    state.result = None
    state.result_signature = ""
    state.mapping_df = None


def _render_import() -> None:
    state = st.session_state
    st.header("1 · Import your data")
    st.write("Upload a file or connect to a JSON API. Your original data stays unchanged.")
    mode = st.radio("Import source", ["Upload a file", "Connect to an API"], key="w_import_mode")
    if mode == "Upload a file":
        upload = st.file_uploader(
            "Choose a CSV, JSON, JSON Lines, or Excel (.xlsx) file",
            type=["csv", "json", "jsonl", "ndjson", "xlsx"], key="uploader",
        )
        if st.button("Load file", key="load_file", disabled=upload is None):
            try:
                state.load = etl.load_file(upload.name, upload.getvalue())
                _load_schema(state.load.df)
                state.import_error = ""
            except etl.ETLError as exc:
                state.load = None
                state.import_error = f"{exc.message} {exc.hint}"
    else:
        st.text_input("API URL", key="w_api_url", placeholder="https://example.com/customers")
        st.selectbox("Authentication", ["none", "bearer", "api_key", "basic"], key="w_api_auth_kind")
        auth_kind = state.get("w_api_auth_kind", "none")
        if auth_kind == "bearer":
            st.text_input("API token", key="w_api_token", type="password")
        elif auth_kind == "api_key":
            st.text_input("Header name", key="w_api_header_name")
            st.text_input("Header value", key="w_api_header_value", type="password")
        elif auth_kind == "basic":
            st.text_input("Username", key="w_api_username")
            st.text_input("Password", key="w_api_password", type="password")
        st.text_input("Records path (optional)", key="w_api_records_path", placeholder="data.items")
        if st.button("Connect and fetch", key="fetch_api"):
            try:
                state.load = etl.fetch_api(
                    state.get("w_api_url", ""), _auth_config("api"),
                    state.get("w_api_records_path", ""),
                )
                _load_schema(state.load.df)
                state.import_error = ""
            except etl.ETLError as exc:
                state.load = None
                state.import_error = f"{exc.message} {exc.hint}"
    if state.import_error:
        st.error(state.import_error)
    if state.load is not None:
        st.success(f"Loaded {len(state.load.df):,} rows and {len(state.load.df.columns):,} fields.")
        if state.load.warnings:
            for warning in state.load.warnings:
                st.warning(warning)
        st.dataframe(state.load.df.head(10), use_container_width=True)
        st.button("Review fields", key="import_next",
                  on_click=_set_page, args=(PAGES[1],))
    st.button("Start over", key="start_over", on_click=_start_over)


def _render_review() -> None:
    state = st.session_state
    st.header("2 · Review and prepare fields")
    if state.load is None:
        st.info("Import a dataset to review its fields.")
        return
    st.write("Confirm the suggested data types, rename fields, and choose which ones to keep.")
    first, second = st.columns(2)
    with first:
        state.sticky["dayfirst"] = st.checkbox(
            "Interpret ambiguous dates as day first", value=state.sticky["dayfirst"], key="w_dayfirst"
        )
    with second:
        state.sticky["tokens_text"] = st.text_input(
            "Additional missing-value tokens (comma separated)",
            value=state.sticky["tokens_text"], key="w_tokens_text",
        )
    state.sticky["invalid_handling"] = st.radio(
        "Unreadable values", ["reject", "blank"], format_func=lambda value: (
            "Set aside affected rows" if value == "reject" else "Leave unreadable cells blank"
        ), key="w_invalid_handling",
    )
    editor_key = f"schema_{state.schema_ver}"
    st.data_editor(
        state.schema_df.drop(columns=["Detected"]),
        key=editor_key, on_change=_schema_changed,
        column_config={
            "Type": st.column_config.SelectboxColumn("Type", options=TYPE_OPTIONS),
            "Keep": st.column_config.CheckboxColumn("Keep"),
        },
        disabled=["Source field"], hide_index=True, use_container_width=True,
    )
    names = state.schema_df.loc[state.schema_df["Keep"], "New name"].astype(str).str.strip()
    clash = names.str.casefold().duplicated().any() or names.eq("").any()
    if clash:
        st.error("Two or more kept fields have the same name, or a name is empty.")
    try:
        schema = etl.apply_schema(state.load.df, _specs(), _tokens(), state.sticky["dayfirst"])
        _issues, unreadable_count = etl.issues_table(state.load.df, _specs(), schema)
        profile = etl.profile_table(state.load.df, _specs(), schema)
        metric_cols = st.columns(3)
        metric_cols[0].metric("Rows", f"{len(state.load.df):,}")
        metric_cols[1].metric("Fields", f"{len(state.load.df.columns):,}")
        metric_cols[2].metric("Unreadable values", f"{unreadable_count:,}")
        st.subheader("Data quality profile")
        st.dataframe(profile, hide_index=True, use_container_width=True)
        st.subheader("Original data preview")
        st.dataframe(state.load.df.head(12), use_container_width=True)
    except etl.ETLError as exc:
        st.error(exc.message)
        clash = True
    back, proceed = st.columns(2)
    with back:
        st.button("Back to import", key="review_back", on_click=_set_page, args=(PAGES[0],))
    with proceed:
        st.button("Continue to transformations", key="review_next", disabled=clash,
                  on_click=_set_page, args=(PAGES[2],))


def _render_step_builder(columns: list[str]) -> None:
    state = st.session_state
    kind = st.selectbox(
        "Transformation", ["dedupe", "filter", "select", "text", "date_format", "missing", "calc"],
        key="nb_kind_0",
    )
    if kind == "filter":
        st.selectbox("Field", columns, key="nb_fcol_0")
        st.selectbox("Condition", ["equals", "contains", "gt", "gte", "lt", "lte", "is_missing"], key="nb_fop_0")
        st.text_input("Compare with", key="nb_fval_text_0")
        st.selectbox("Action", ["keep", "remove"], key="nb_fmode_0")
    elif kind == "select":
        st.multiselect("Fields to keep and order", columns, default=columns, key="nb_scols_0")
    elif kind == "text":
        st.multiselect("Text fields", columns, key="nb_tcols_0")
        st.multiselect("Standardisation", ["trim", "collapse", "lower", "upper", "title"],
                       default=["trim"], key="nb_tops_0")
    elif kind == "date_format":
        st.multiselect("Date fields", columns, key="nb_dcols_0")
        st.text_input("Date format", value="%Y-%m-%d", key="nb_dformat_0")
    elif kind == "missing":
        st.multiselect("Fields (leave empty for all)", columns, key="nb_mcols_0")
        st.selectbox("Action", ["fill_value", "fill_mean", "fill_median", "fill_mode", "drop_rows"],
                     key="nb_maction_0")
        st.text_input("Fill value", key="nb_mvalue_0")
    elif kind == "calc":
        st.text_input("New field name", key="nb_cname_0")
        st.selectbox("Calculation", ["arithmetic", "concat"], key="nb_cmode_0")
        if state.get("nb_cmode_0", "arithmetic") == "concat":
            st.multiselect("Fields to join", columns, key="nb_ccols_0")
            st.text_input("Separator", value=" ", key="nb_csep_0")
        else:
            st.selectbox("Left field", columns, key="nb_cleft_0")
            st.selectbox("Operation", ["+", "-", "*", "/"], key="nb_cop_0")
            st.selectbox("Right side", ["number", "column"], key="nb_crk_0")
            if state.get("nb_crk_0", "number") == "column":
                st.selectbox("Right field", columns, key="nb_cright_col_0")
            else:
                st.text_input("Number", value="0", key="nb_cnum_0")
            st.number_input("Decimal places (0–8)", min_value=0, max_value=8, value=2, key="nb_cdecimals_0")
    st.button("Add transformation", key="nb_add_0", on_click=_add_step)


def _render_recipe() -> None:
    state = st.session_state
    recipe = etl.recipe_to_dict(_specs(), state.steps, _tokens(),
                                bool(state.sticky["dayfirst"]), state.sticky["invalid_handling"])
    st.download_button("Download recipe", json.dumps(recipe, indent=2), file_name="cleaning_recipe.json",
                       mime="application/json", key="recipe_dl")
    uploaded = st.file_uploader("Load a saved recipe", type=["json"], key=f"recipe_up_{state.recipe_ver}")
    state.recipe_upload = uploaded
    if uploaded is not None:
        st.button("Apply recipe", key="recipe_apply", on_click=_recipe_apply)
    if state.get("recipe_applied"):
        st.success("Settings applied.")
        state.recipe_applied = False
    if state.get("recipe_error"):
        st.error(state.recipe_error)


def _render_transform() -> None:
    state = st.session_state
    st.header("3 · Build your cleaning steps")
    if state.load is None:
        st.info("Import and review a dataset before adding transformations.")
        return
    columns = list(state.schema_df.loc[state.schema_df["Keep"], "New name"].astype(str))
    st.write("Add the steps you want, then run them on a copy of the source data.")
    for index, step in enumerate(state.steps):
        left, up, down, remove = st.columns([6, 1, 1, 1])
        left.write(f"**{index + 1}.** {etl.describe_step(step)}")
        up.button("↑", key=f"up_{index}", disabled=index == 0,
                  on_click=_move_step, args=(index, -1))
        down.button("↓", key=f"down_{index}", disabled=index == len(state.steps) - 1,
                    on_click=_move_step, args=(index, 1))
        remove.button("Remove", key=f"del_{index}", on_click=_delete_step, args=(index,))
    _render_step_builder(columns)
    _render_recipe()
    back, run = st.columns(2)
    with back:
        st.button("Back to review", key="tr_back", on_click=_set_page, args=(PAGES[1],))
    with run:
        st.button("Run cleaning process", key="run", disabled=not bool(state.schema_df["Keep"].any()),
                  on_click=_run_job)


def _render_output() -> None:
    state = st.session_state
    st.header("4 · Review your cleaned output")
    result = state.result
    if result is None:
        st.info("Run the cleaning process to see your output.")
        st.button("Back to transformations", key="out_steps", on_click=_set_page, args=(PAGES[2],))
        return
    stale = state.result_signature != _signature()
    if stale:
        st.warning("These results are out of date because your settings changed.")
        st.button("Re-run with current settings", key="out_rerun_stale", on_click=_run_job)
    if result.success:
        st.success("Finished — your cleaning process completed.")
        output = result.output if result.output is not None else pd.DataFrame()
        metrics = st.columns(4)
        metrics[0].metric("Source rows", f"{len(state.load.df):,}")
        metrics[1].metric("Output rows", f"{len(output):,}")
        metrics[2].metric("Set aside (couldn't process)", f"{len(result.rejected):,}")
        metrics[3].metric("Fields", f"{len(output.columns):,}")
        if result.warnings:
            for warning in result.warnings:
                st.warning(warning)
        if result.step_log:
            st.subheader("Process log")
            st.dataframe(pd.DataFrame([asdict(entry) for entry in result.step_log]), hide_index=True)
        st.subheader("Source vs output")
        st.dataframe(etl.compare_table(result), hide_index=True, use_container_width=True)
        st.subheader("Cleaned data preview")
        st.dataframe(output.head(20), use_container_width=True)
        if not result.rejected.empty:
            st.subheader("Rows set aside")
            st.dataframe(result.rejected.head(20), hide_index=True, use_container_width=True)
            st.download_button("Download rows set aside", result.rejected.to_csv(index=False).encode("utf-8"),
                               file_name="rows_set_aside.csv", mime="text/csv", key="rej_dl")
    else:
        for error in result.errors:
            st.error(error)
    back, edit = st.columns(2)
    with back:
        st.button("Edit transformations", key="out_steps", on_click=_set_page, args=(PAGES[2],))
    with edit:
        st.button("Continue to export", key="out_next", disabled=not result.success,
                  on_click=_set_page, args=(PAGES[4],))


def _initialise_mapping() -> None:
    state = st.session_state
    if state.mapping_df is not None:
        return
    output = state.result.output if state.result is not None else pd.DataFrame()
    rows = []
    for column in output.columns:
        if column in {"customer_id", "id"}:
            target = "id"
        elif column in {"full_name", "Name", "Customer name", "name"}:
            target = "name"
        else:
            target = ""
        rows.append({"Source field": column, "Send": bool(target), "Output field": target or column})
    state.mapping_df = pd.DataFrame(rows, columns=["Source field", "Send", "Output field"])
    state.map_ver += 1


def _render_export() -> None:
    state = st.session_state
    st.header("5 · Export or send your data")
    result = state.result
    if result is None or not result.success or result.output is None:
        st.info("Run a successful cleaning process before exporting.")
        return
    st.write("Download the cleaned dataset or send selected fields to an API.")
    csv_col, json_col = st.columns(2)
    with csv_col:
        st.download_button("Download CSV", etl.to_csv_bytes(result.output), file_name="cleaned_data.csv",
                           mime="text/csv", key="dl_csv")
    with json_col:
        st.download_button("Download JSON", etl.to_json_bytes(result.output), file_name="cleaned_data.json",
                           mime="application/json", key="dl_json")
    st.divider()
    st.subheader("Send to an API")
    sticky = state.push_sticky
    st.text_input("Destination URL", value=sticky["url"], key="w_push_url", placeholder="https://example.com/ingest")
    st.selectbox("Authentication", ["none", "bearer", "api_key", "basic"], index=["none", "bearer", "api_key", "basic"].index(sticky["auth_kind"]), key="w_push_auth_kind")
    auth_kind = state.get("w_push_auth_kind", "none")
    if auth_kind == "bearer":
        st.text_input("API token", value=sticky["token"], key="w_push_token", type="password")
    elif auth_kind == "api_key":
        st.text_input("Header name", value=sticky["header_name"], key="w_push_header_name")
        st.text_input("Header value", value=sticky["header_value"], key="w_push_header_value", type="password")
    elif auth_kind == "basic":
        st.text_input("Username", value=sticky["username"], key="w_push_username")
        st.text_input("Password", value=sticky["password"], key="w_push_password", type="password")
    st.text_input("Optional wrapper key", value=sticky["wrapper"], key="w_push_wrapper")
    st.number_input("Records per request", min_value=1, max_value=1000, value=sticky["batch"], key="w_push_batch")
    sticky["url"] = state.get("w_push_url", sticky["url"])
    sticky["auth_kind"] = state.get("w_push_auth_kind", sticky["auth_kind"])
    for name in ("token", "header_name", "header_value", "username", "password"):
        sticky[name] = state.get(f"w_push_{name}", sticky[name])
    sticky["wrapper"] = state.get("w_push_wrapper", sticky["wrapper"])
    sticky["batch"] = state.get("w_push_batch", sticky["batch"])
    _initialise_mapping()
    st.data_editor(
        state.mapping_df,         key=f"mapping_{state.map_ver}", on_change=_mapping_changed,
        column_config={
            "Send": st.column_config.CheckboxColumn("Send"),
        },
        disabled=["Source field"], hide_index=True, use_container_width=True,
    )
    st.checkbox("Preview the exact payload", key="push_preview")
    if state.get("push_preview"):
        try:
            config = etl.PushConfig(
                state.get("w_push_url", ""), _auth_config("push"), _mapping(),
                state.get("w_push_wrapper", ""),
                int(state.get("w_push_batch", 100)),
            )
            st.json(etl.preview_payload(result.output, config))
        except etl.ETLError as exc:
            st.info(exc.message)
    st.checkbox("I confirm that these records should be sent.", key="push_confirm")
    if st.button("Send records", key="push_go", disabled=not bool(state.get("push_confirm", False))):
        try:
            config = etl.PushConfig(
                state.get("w_push_url", ""), _auth_config("push"), _mapping(),
                state.get("w_push_wrapper", ""), int(state.get("w_push_batch", 100)),
            )
            state.push_result = etl.push_records(result.output, config)
        except etl.ETLError as exc:
            state.push_result = etl.PushResult(False, exc.message)
    push_result = state.push_result
    if push_result is not None:
        (st.success if push_result.success else st.error)(push_result.message)
        if push_result.failures:
            st.dataframe(pd.DataFrame(push_result.failures), hide_index=True)
            if push_result.stopped_early:
                st.warning("Sending stopped after three consecutive failed requests.")
    st.button("Back to output", key="exp_back", on_click=_export_back)


def main() -> None:
    st.set_page_config(page_title="Data Prep Studio", page_icon="🧹", layout="wide")
    _initialise()
    st.markdown(
        """
        <style>
        .block-container {max-width: 1180px; padding-top: 2rem; padding-bottom: 4rem;}
        [data-testid="stHeader"] {background: transparent;}
        .stApp {background: linear-gradient(180deg, #f7fbfa 0%, #ffffff 220px);}
        h1, h2, h3 {letter-spacing: -0.035em;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title("Data Prep Studio")
    st.caption("A guided workspace for importing, understanding, cleaning, and sharing data.")
    st.radio("Workflow", PAGES, horizontal=True, key="nav", label_visibility="collapsed")
    page = st.session_state.nav
    if page == PAGES[0]:
        _render_import()
    elif page == PAGES[1]:
        _render_review()
    elif page == PAGES[2]:
        _render_transform()
    elif page == PAGES[3]:
        _render_output()
    else:
        _render_export()


if __name__ == "__main__":
    main()
