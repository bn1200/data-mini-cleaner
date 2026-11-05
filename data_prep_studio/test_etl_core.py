"""Tests for etl_core. Run with:  python -m unittest -v   (or pytest)"""
import json
import threading
import unittest

import pandas as pd

import etl_core as etl
import mock_api

CSV = open("sample_data/customers.csv", "rb").read()


def fresh():
    lr = etl.load_file("customers.csv", CSV)
    return lr.df, etl.infer_schema(lr.df)


def spec(specs, source, **kw):
    """Update and return the schema spec matching source."""
    for s in specs:
        if s.source == source:
            for k, v in kw.items():
                setattr(s, k, v)
    return specs


class Loading(unittest.TestCase):
    """Tests for loading raw files into data frames."""

    def test_csv_all_text_and_shape(self):
        df, _ = fresh()
        self.assertEqual(df.shape, (32, 10))
        self.assertTrue(all(isinstance(v, str) for v in df.to_numpy().ravel()))

    def test_csv_semicolon_and_latin1(self):
        lr = etl.load_file("a.csv", "name;city\nJosé;Bo\n".encode("cp1252"))
        self.assertEqual(list(lr.df.columns), ["name", "city"])
        self.assertEqual(lr.df.at[0, "name"], "José")
        self.assertTrue(lr.warnings)

    def test_csv_errors(self):
        for name, data in [("a.csv", b""), ("a.csv", b"a,b\n"), ("a.xyz", b"x"), ("noext", b"x")]:
            with self.assertRaises(etl.ETLError, msg=name):
                etl.load_file(name, data)
        with self.assertRaises(etl.ETLError):  # ragged rows
            etl.load_file("a.csv", b"a,b\n1,2\n1,2,3,4,5\n")

    def test_duplicate_and_blank_headers(self):
        lr = etl.load_file("a.csv", b"a,a,\n1,2,3\n")
        self.assertEqual(list(lr.df.columns), ["a", "a_2", "column_3"])

    def test_json_variants(self):
        lr = etl.load_file("a.json", json.dumps([{"a": 1, "b": {"c": True}}, {"a": 2}]).encode())
        self.assertEqual(list(lr.df.columns), ["a", "b.c"])
        self.assertEqual(lr.df.at[0, "b.c"], "true")
        self.assertEqual(lr.df.at[1, "b.c"], "")
        self.assertEqual(lr.df.at[0, "a"], "1")                      # not "1.0"
        wrapped = etl.load_file("a.json", json.dumps({"meta": 1, "rows": [{"x": 1}]}).encode())
        self.assertEqual(wrapped.df.shape, (1, 1))
        jl = etl.load_file("a.jsonl", b'{"x": 1}\n{"x": 2}\n')
        self.assertEqual(len(jl.df), 2)
        path = etl.load_file("a.json", json.dumps({"data": {"items": [{"x": 1}]}}).encode(), records_path="data.items")
        self.assertEqual(len(path.df), 1)

    def test_json_errors(self):
        for data in [b"{bad json", b"[1,2,3]", b"[]", b'"hello"']:
            with self.assertRaises(etl.ETLError, msg=data):
                etl.load_file("a.json", data)
        with self.assertRaises(etl.ETLError):
            etl.load_file("a.json", b'{"a": []}', records_path="nope.x")

    def test_excel_roundtrip(self):
        import io
        buf = io.BytesIO()
        pd.DataFrame({"n": [1, 2], "d": pd.to_datetime(["2024-01-01", "2024-02-01"])}).to_excel(buf, index=False)
        self.assertEqual(etl.list_excel_sheets(buf.getvalue()), ["Sheet1"])
        lr = etl.load_file("a.xlsx", buf.getvalue())
        specs = etl.infer_schema(lr.df)
        self.assertEqual([s.detected for s in specs], ["integer", "date"])
        with self.assertRaises(etl.ETLError):
            etl.load_file("a.xlsx", b"not a zip")


class TypeInference(unittest.TestCase):
    """Tests for type detection and conversion heuristics."""

    def test_detected_types(self):
        _, specs = fresh()
        got = {s.source: s.detected for s in specs}
        self.assertEqual(got["customer_id"], "integer")
        self.assertEqual(got["signup_date"], "date")      # 31/32 parse -> still date, 1 flagged
        self.assertEqual(got["monthly_fee"], "decimal")   # "1,200.50" and "N/A"(missing)
        self.assertEqual(got["orders"], "integer")
        self.assertEqual(got["is_active"], "boolean")
        self.assertEqual(got["zip_code"], "text")         # leading zeros are identifiers
        self.assertEqual(got["full_name"], "text")

    def test_edge_cases(self):
        tok = etl.DEFAULT_MISSING_TOKENS
        s = lambda *v: pd.Series(list(v), dtype=object)
        self.assertEqual(etl.detect_type(s("1", "2", ""), tok), "integer")
        self.assertEqual(etl.detect_type(s("1", "2.5"), tok), "decimal")
        self.assertEqual(etl.detect_type(s("", "NA", "null"), tok), "text")
        self.assertEqual(etl.detect_type(s("12345678901234567890"), tok), "text")
        self.assertEqual(etl.detect_type(s("20240101", "20240102"), tok), "integer")
        self.assertEqual(etl.detect_type(s("Y", "N", "y"), tok), "boolean")
        self.assertEqual(etl.detect_type(s("1", "2", "3", "4", "5", "6", "7", "8", "9", "x"), tok), "integer")
        self.assertEqual(etl.detect_type(s("1", "2", "x", "y"), tok), "text")

    def test_conversion_values(self):
        tok = etl.DEFAULT_MISSING_TOKENS
        r = pd.Series(["$1,234.50", " 7 ", "abc", "", "NA"], dtype=object)
        typed, invalid, missing = etl.convert_series(r, "decimal", tok)
        self.assertEqual(typed.iloc[0], 1234.5)
        self.assertEqual(list(invalid), [False, False, True, False, False])
        self.assertEqual(list(missing), [False, False, False, True, True])
        d, inv, _ = etl.convert_series(pd.Series(["2024-03-31", "03/04/2024", "x"], dtype=object), "date", tok, dayfirst=True)
        self.assertEqual(d.iloc[1], pd.Timestamp("2024-04-03"))
        self.assertEqual(list(inv), [False, False, True])
        d2, _, _ = etl.convert_series(pd.Series(["03/04/2024"], dtype=object), "date", tok, dayfirst=False)
        self.assertEqual(d2.iloc[0], pd.Timestamp("2024-03-04"))
        i, inv, _ = etl.convert_series(pd.Series(["3", "3.0", "3.5"], dtype=object), "integer", tok)
        self.assertEqual(list(inv), [False, False, True])


class Schema(unittest.TestCase):
    """Tests for schema application and validation."""

    def test_rename_keep_validate(self):
        df, specs = fresh()
        spec(specs, "full_name", name="Name")
        spec(specs, "email", keep=False)
        res = etl.apply_schema(df, specs)
        self.assertIn("Name", res.typed.columns)
        self.assertNotIn("email", res.typed.columns)
        spec(specs, "plan", name="Name")
        with self.assertRaises(etl.ETLError):
            etl.apply_schema(df, specs)

    def test_issue_reporting(self):
        df, specs = fresh()
        res = etl.apply_schema(df, specs)
        issues, total = etl.issues_table(df, specs, res)
        self.assertEqual(total, 2)                       # bad date + "not sure"
        self.assertEqual(set(issues["field"]), {"signup_date", "orders"})
        prof = etl.profile_table(df, specs, res).set_index("Field")
        self.assertEqual(prof.at["monthly_fee", "Missing"], 1)
        self.assertEqual(prof.at["full_name", "Extra spaces"], 1)

    def test_changing_type_changes_issues(self):
        df, specs = fresh()
        spec(specs, "district", dtype="integer")
        res = etl.apply_schema(df, specs)
        self.assertGreater(int(res.invalid["district"].sum()), 20)


class Transforms(unittest.TestCase):
    """Tests for ETL transform steps and pipeline execution."""

    def run_steps(self, steps, specs=None, **kw):
        df, base = fresh()
        return etl.run_job(df, specs or base, steps, **kw)

    def test_no_steps_rejects_invalid_rows_and_never_touches_source(self):
        df, specs = fresh()
        before = df.copy(deep=True)
        res = etl.run_job(df, specs, [])
        self.assertTrue(res.success)
        pd.testing.assert_frame_equal(df, before)                    # source unchanged
        self.assertEqual(len(res.output), 30)                       # 32 - 2 invalid rows
        self.assertEqual(len(res.rejected), 2)
        self.assertEqual(sorted(res.rejected["source_row"]), [7, 9])
        self.assertIn("source: full_name", res.rejected.columns)
        res2 = etl.run_job(df, specs, [], invalid_handling="blank")
        self.assertEqual(len(res2.output), 32)
        self.assertTrue(res2.warnings)
        self.assertEqual(int(res2.output["orders"].isna().sum()), 1)

    def test_filter_variants(self):
        f = lambda **p: etl.make_step("filter", **{"mode": "keep", "value": "", **p})
        out = lambda step: self.run_steps([step], invalid_handling="blank").output
        self.assertEqual(set(out(f(column="plan", op="equals", value="PREMIUM"))["plan"]), {"premium"})
        self.assertTrue((out(f(column="orders", op="gte", value="10"))["orders"] >= 10).all())
        self.assertEqual(len(out(f(column="signup_date", op="gt", value="2024-12-31"))), 0)
        self.assertGreater(len(out(f(column="signup_date", op="gte", value="2024-06-01"))), 0)
        self.assertEqual(out(f(column="district", op="is_missing")).shape[0], 1)
        self.assertEqual(out(f(column="district", op="is_missing", mode="remove")).shape[0], 31)
        self.assertTrue(out(f(column="is_active", op="equals", value="yes"))["is_active"].all())
        self.assertTrue(out(f(column="email", op="contains", value="@EXAMPLE"))["email"].str.contains("@").all())

    def test_filter_type_errors_are_friendly(self):
        r = self.run_steps([etl.make_step("filter", column="orders", op="equals", value="abc", mode="keep")])
        self.assertFalse(r.success)
        self.assertIn("isn't a number", r.errors[0])
        r = self.run_steps([etl.make_step("filter", column="plan", op="gt", value="1", mode="keep")])
        self.assertFalse(r.success)

    def test_select_and_reorder(self):
        r = self.run_steps([etl.make_step("select", columns=["plan", "customer_id"])])
        self.assertEqual(list(r.output.columns), ["plan", "customer_id"])

    def test_text_standardising(self):
        steps = [etl.make_step("text", columns=["full_name"], ops=["trim", "collapse", "title"])]
        r = self.run_steps(steps, invalid_handling="blank")
        names = r.output["full_name"]
        self.assertTrue((names == names.str.strip()).all())
        self.assertTrue((names == names.str.title()).all())

    def test_date_format(self):
        steps = [etl.make_step("date_format", columns=["signup_date"], format="%d/%m/%Y")]
        r = self.run_steps(steps)
        self.assertRegex(r.output["signup_date"].iloc[0], r"^\d{2}/\d{2}/\d{4}$")
        bad = self.run_steps([etl.make_step("date_format", columns=["plan"], format="%Y")])
        self.assertFalse(bad.success)

    def test_missing_values(self):
        d = lambda **p: self.run_steps([etl.make_step("missing", **p)], invalid_handling="blank")
        r = d(columns=["monthly_fee"], action="fill_value", value="0")
        self.assertEqual(int(r.output["monthly_fee"].isna().sum()), 0)
        r = d(columns=["monthly_fee"], action="fill_mean")
        self.assertEqual(int(r.output["monthly_fee"].isna().sum()), 0)
        r = d(columns=["orders"], action="fill_median")
        self.assertEqual(int(r.output["orders"].isna().sum()), 0)
        r = d(columns=["district"], action="fill_mode")
        self.assertEqual(int(r.output["district"].isna().sum()), 0)
        r = d(columns=["email"], action="drop_rows")
        self.assertEqual(len(r.output), 31)
        self.assertEqual(r.step_log[-1].removed, 1)
        self.assertFalse(d(columns=["plan"], action="fill_mean").success)
        self.assertFalse(d(columns=["monthly_fee"], action="fill_value", value="abc").success)
        r = d(columns=[], action="drop_rows")                         # all fields
        self.assertTrue(r.success)

    def test_dedupe(self):
        r = self.run_steps([etl.make_step("dedupe", columns=[], keep="first")], invalid_handling="blank")
        self.assertEqual(len(r.output), 30)
        self.assertEqual(r.step_log[-1].removed, 2)
        r = self.run_steps([etl.make_step("dedupe", columns=["plan"], keep="last")], invalid_handling="blank")
        self.assertEqual(len(r.output), 2)

    def test_calculated_fields(self):
        r = self.run_steps([etl.make_step("calc", name="annual", mode="arithmetic", left="monthly_fee",
                                          op="*", right_kind="number", right="12", decimals=1)], invalid_handling="blank")
        row = r.output.iloc[0]
        self.assertAlmostEqual(row["annual"], row["monthly_fee"] * 12, places=1)
        r = self.run_steps([etl.make_step("calc", name="fee_per_order", mode="arithmetic", left="monthly_fee",
                                          op="/", right_kind="column", right="orders", decimals=2)], invalid_handling="blank")
        self.assertTrue(r.success)
        zero_orders = int((r.output["orders"] == 0).sum()) if r.output is not None else 0
        self.assertEqual(zero_orders, 0)                       # zero-divisor rows were set aside
        self.assertTrue(len(r.rejected) >= 1)
        self.assertIn("Division by zero", r.rejected["problem"].iloc[0])
        r = self.run_steps([etl.make_step("calc", name="label", mode="concat", columns=["customer_id", "plan"], separator="-")],
                           invalid_handling="blank")
        self.assertEqual(r.output["label"].iloc[0], "1001-" + r.output["plan"].iloc[0])
        r = self.run_steps([etl.make_step("calc", name="x", mode="arithmetic", left="plan", op="+", right_kind="number", right="1")])
        self.assertFalse(r.success)

    def test_int_arithmetic_stays_integer(self):
        r = self.run_steps([etl.make_step("calc", name="o2", mode="arithmetic", left="orders", op="+",
                                          right_kind="number", right="2", decimals=None)], invalid_handling="blank")
        self.assertEqual(str(r.output["o2"].dtype), "Int64")

    def test_failure_stops_pipeline_and_logs_skipped(self):
        steps = [etl.make_step("select", columns=["plan"]),
                 etl.make_step("filter", column="orders", op="equals", value="1", mode="keep")]
        r = self.run_steps(steps)
        self.assertFalse(r.success)
        self.assertIsNone(r.output)
        self.assertIn("Step 2", r.errors[0])

    def test_validate_steps_tracks_created_and_removed_fields(self):
        cols = ["a", "b"]
        steps = [etl.make_step("calc", name="c", mode="concat", columns=["a", "b"], separator=""),
                 etl.make_step("select", columns=["c"]),
                 etl.make_step("filter", column="a", op="is_missing", mode="keep")]
        problems = etl.validate_steps(cols, steps)
        self.assertEqual(len(problems), 1)
        self.assertIn("Step 3", problems[0])

    def test_rerun_is_repeatable(self):
        df, specs = fresh()
        steps = [etl.make_step("dedupe", columns=[], keep="first")]
        a = etl.run_job(df, specs, steps, invalid_handling="blank").output
        b = etl.run_job(df, specs, steps, invalid_handling="blank").output
        pd.testing.assert_frame_equal(a, b)

    def test_compare_table(self):
        df, specs = fresh()
        res = etl.run_job(df, specs, [etl.make_step("missing", columns=["district"], action="fill_value", value="Unknown")])
        t = etl.compare_table(res).set_index("Field")
        self.assertEqual(t.at["district", "Empty in source"], 1)
        self.assertEqual(t.at["district", "Empty in output"], 0)

    def test_description_for_every_step_type(self):
        steps = [
            etl.make_step("filter", column="a", op="equals", value="1", mode="keep"),
            etl.make_step("select", columns=["a"]),
            etl.make_step("text", columns=["a"], ops=["trim", "lower"]),
            etl.make_step("date_format", columns=["a"], format="%Y"),
            etl.make_step("missing", columns=[], action="drop_rows"),
            etl.make_step("dedupe", columns=[], keep="first"),
            etl.make_step("calc", name="n", mode="arithmetic", left="a", op="+", right_kind="number", right="1", decimals=None),
            etl.make_step("calc", name="n", mode="concat", columns=["a"], separator=" "),
        ]
        for s in steps:
            self.assertTrue(etl.describe_step(s))


class Export(unittest.TestCase):
    """Tests for CSV and JSON export serialization."""

    def test_csv_json(self):
        df, specs = fresh()
        out = etl.run_job(df, specs, [], invalid_handling="blank").output
        csv_text = etl.to_csv_bytes(out).decode()
        self.assertTrue(csv_text.startswith("customer_id,"))
        self.assertIn("2024-", csv_text)
        self.assertTrue(etl.to_csv_bytes(out, excel_friendly=True).startswith(b"\xef\xbb\xbf"))
        recs = json.loads(etl.to_json_bytes(out))
        self.assertEqual(len(recs), 32)
        self.assertIsInstance(recs[0]["customer_id"], int)
        self.assertIsInstance(recs[0]["is_active"], bool)
        self.assertRegex(recs[0]["signup_date"], r"^\d{4}-\d{2}-\d{2}$")
        nulls = [r for r in recs if r["orders"] is None]
        self.assertEqual(len(nulls), 1)                      # NA -> JSON null, not "NaN"


class Recipe(unittest.TestCase):
    """Tests for recipe serialization and round-tripping."""

    def test_roundtrip(self):
        df, specs = fresh()
        spec(specs, "full_name", name="Name")
        steps = [etl.make_step("dedupe", columns=[], keep="first")]
        d = json.loads(json.dumps(etl.recipe_to_dict(specs, steps, ["", "na"], True, "blank")))
        specs2, steps2, opts, warns = etl.recipe_from_dict(d, etl.infer_schema(df))
        self.assertEqual(steps2, steps)
        self.assertEqual(next(s for s in specs2 if s.source == "full_name").name, "Name")
        self.assertTrue(opts["dayfirst"])
        with self.assertRaises(etl.ETLError):
            etl.recipe_from_dict({"version": 99}, specs)
        with self.assertRaises(etl.ETLError):
            etl.recipe_from_dict({"nonsense": 1}, specs)


class ApiTests(unittest.TestCase):
    """Tests for API fetch and push operations."""

    @classmethod
    def setUpClass(cls):
        mock_api.RECEIVED.clear()
        cls.server = mock_api.make_server(0)
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.auth = etl.AuthConfig(kind="bearer", token=mock_api.TOKEN)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def test_fetch_ok_with_records_path(self):
        lr = etl.fetch_api(self.base + "/customers", auth=self.auth, records_path="data.items")
        self.assertEqual(lr.df.shape[0], 3)
        self.assertEqual(lr.df.at[0, "tags"], '["vip"]')
        auto = etl.fetch_api(self.base + "/customers", auth=self.auth)      # auto-detected
        self.assertEqual(auto.df.shape, lr.df.shape)

    def test_fetch_failures_are_explained(self):
        with self.assertRaises(etl.ETLError) as c:
            etl.fetch_api(self.base + "/customers", auth=etl.AuthConfig(kind="bearer", token="wrong-secret"))
        self.assertIn("401", c.exception.message)
        self.assertNotIn("wrong-secret", str(c.exception))
        self.assertIn("credentials", c.exception.hint)
        with self.assertRaises(etl.ETLError) as c:
            etl.fetch_api(self.base + "/nope", auth=self.auth)
        self.assertIn("404", c.exception.message)
        for bad in ["not a url", "ftp://x.y", ""]:
            with self.assertRaises(etl.ETLError):
                etl.fetch_api(bad)
        with self.assertRaises(etl.ETLError) as c:
            etl.fetch_api("http://127.0.0.1:1/x", timeout=2)
        self.assertIn("connect", c.exception.message.lower())
        with self.assertRaises(etl.ETLError):
            etl.fetch_api(self.base + "/customers", auth=etl.AuthConfig(kind="bearer"))   # no token entered

    def test_kv_parsing(self):
        self.assertEqual(etl.parse_kv_lines("A: 1\nB: x:y\n\n", ":", "header"), {"A": "1", "B": "x:y"})
        with self.assertRaises(etl.ETLError):
            etl.parse_kv_lines("oops", ":", "header")

    def _df(self):
        return pd.DataFrame({"cust": pd.array([1, 2, 3], dtype="Int64"), "who": ["a", "b", "c"], "extra": ["x", "y", "z"]})

    def test_push_success_batches_and_mapping(self):
        mock_api.RECEIVED.clear()
        cfg = etl.PushConfig(self.base + "/ingest", auth=self.auth, mapping={"cust": "id", "who": "name", "extra": ""}, batch_size=2)
        res = etl.push_records(self._df(), cfg)
        self.assertTrue(res.success, res.message)
        self.assertEqual((res.requests_total, res.records_accepted), (2, 3))
        self.assertEqual(mock_api.RECEIVED, [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}])

    def test_push_wrapper_and_single(self):
        mock_api.RECEIVED.clear()
        cfg = etl.PushConfig(self.base + "/ingest", auth=self.auth, mapping={"cust": "id", "who": "name"}, wrapper_key="records", batch_size=100)
        self.assertTrue(etl.push_records(self._df(), cfg).success)
        self.assertEqual(len(mock_api.RECEIVED), 3)
        cfg.batch_size = 1
        res = etl.push_records(self._df(), cfg)
        self.assertEqual(res.requests_total, 3)
        self.assertEqual(etl.preview_payload(self._df(), cfg), {"records": {"id": 1, "name": "a"}})

    def test_push_failure_reports_details_and_stops_early(self):
        cfg = etl.PushConfig(self.base + "/ingest", auth=self.auth, mapping={"cust": "id"}, batch_size=1)   # no 'name' -> 422
        res = etl.push_records(self._df().assign(cust=lambda d: d.cust), cfg)
        self.assertFalse(res.success)
        self.assertEqual(res.records_accepted, 0)
        self.assertEqual(res.failures[0]["status"], 422)
        self.assertIn("name", res.failures[0]["problem"])
        big = pd.DataFrame({"cust": range(10)})
        res = etl.push_records(big, cfg)
        self.assertTrue(res.stopped_early)
        self.assertEqual(len(res.failures), 3)

    def test_push_bad_credentials_and_config(self):
        cfg = etl.PushConfig(self.base + "/ingest", auth=etl.AuthConfig(kind="bearer", token="bad-secret"),
                             mapping={"cust": "id", "who": "name"})
        res = etl.push_records(self._df(), cfg)
        self.assertFalse(res.success)
        self.assertEqual(res.failures[0]["status"], 401)
        self.assertNotIn("bad-secret", json.dumps(res.failures))
        with self.assertRaises(etl.ETLError):
            etl.push_records(self._df(), etl.PushConfig(self.base + "/ingest", mapping={}))
        with self.assertRaises(etl.ETLError):
            etl.push_records(self._df(), etl.PushConfig(self.base + "/ingest", mapping={"cust": "x", "who": "x"}))
        with self.assertRaises(etl.ETLError):
            etl.push_records(self._df(), etl.PushConfig(self.base + "/ingest", method="DELETE", mapping={"cust": "id"}))
        unreachable = etl.PushConfig("http://127.0.0.1:1/x", mapping={"cust": "id"}, timeout=2)
        res = etl.push_records(self._df(), unreachable)
        self.assertFalse(res.success)
        self.assertEqual(res.failures[0]["status"], "no response")


if __name__ == "__main__":
    unittest.main()
