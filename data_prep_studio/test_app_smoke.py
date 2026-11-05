"""
testing system
"""
import datetime
import json
import runpy
import sys
import threading
import types
import unittest
from pathlib import Path

import mock_api

ROOT = Path(__file__).resolve().parent
APP = str(ROOT / "app.py")
CSV = (ROOT / "sample_data" / "customers.csv").read_bytes()


class StopApp(Exception):
    pass


class RerunRequest(Exception):
    pass


class Upload:
    def __init__(self, name, data):
        self.name, self._data = name, data

    def getvalue(self):
        return self._data


class Harness:
    def __init__(self):
        self.state = State(self)
        self.uploads, self.pressed = {}, set()
        self.registry, self.editors = {}, {}
        self.downloads, self.metrics, self.out = {}, {}, []
        self.known, self.current, self.instantiated = set(), set(), set()
        self.in_script = self.in_callback = False
        self.st = self._build()

    # ---- messages -------------------------------------------------------
    def say(self, kind, text):
        self.out.append((kind, str(text)))

    def texts(self, *kinds):
        return [t for k, t in self.out if not kinds or k in kinds]

    def seen(self, text, *kinds):
        return any(text in t for t in self.texts(*kinds))

    # ---- widgets --------------------------------------------------------
    def widget(self, key, default, kind="widget"):
        assert key, f"{kind} without a key (Streamlit would complain about duplicate IDs)"
        assert key not in self.current, f"DuplicateWidgetID: key {key!r} used twice in one run"
        self.current.add(key)
        self.instantiated.add(key)
        if key not in self.state:
            dict.__setitem__(self.state, key, default)
        return self.state[key]

    def key(self, prefix):
        found = [k for k in self.current if k.startswith(prefix)]
        assert len(found) == 1, f"expected one widget starting {prefix!r}, found {found}"
        return found[0]

    def _build(self):
        h = self
        st = types.ModuleType("streamlit")

        class Ctx:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def __getattr__(self, name): return getattr(st, name)

        def say(kind):
            return lambda *a, **k: h.say(kind, a[0] if a else "")

        for name in ["title", "header", "subheader", "caption", "markdown", "write", "info", "success", "warning", "error"]:
            setattr(st, name, say(name))
        st.divider = lambda *a, **k: None
        st.set_page_config = lambda **k: None
        st.json = lambda obj, **k: h.say("json", json.dumps(obj))
        st.dataframe = lambda df, **k: h.say("dataframe", f"{len(df)} rows")

        def metric(label, value, delta=None, **k):
            del delta
            h.metrics[label] = value
        st.metric = metric
        st.session_state = self.state
        st.sidebar = Ctx()
        st.columns = lambda spec, **k: [Ctx() for _ in range(spec if isinstance(spec, int) else len(spec))]
        st.tabs = lambda labels: [Ctx() for _ in labels]
        st.expander = lambda *a, **k: Ctx()
        st.spinner = lambda *a, **k: Ctx()
        st.container = lambda *a, **k: Ctx()

        def stop(): raise StopApp()
        def rerun(): raise RerunRequest()
        st.stop, st.rerun = stop, rerun

        st.radio = lambda label, options, index=0, format_func=str, key=None, **k: h.widget(key, options[index])
        st.selectbox = lambda label, options=None, index=0, format_func=str, key=None, **k: h.widget(key, (options or [None])[index])
        st.multiselect = lambda label, options, default=None, format_func=str, key=None, **k: h.widget(key, list(default or []))
        st.checkbox = lambda label, value=False, key=None, **k: h.widget(key, value)
        st.text_input = lambda label, value="", key=None, **k: h.widget(key, value)
        st.text_area = lambda label, value="", key=None, **k: h.widget(key, value)
        st.number_input = lambda label, min_value=None, max_value=None, value=None, key=None, **k: h.widget(
            key, value if value is not None else (min_value or 0))
        st.date_input = lambda label, value=None, key=None, **k: h.widget(key, value or datetime.date(2024, 1, 1))
        st.file_uploader = lambda label, type=None, key=None, **k: (h.widget(key, None, "uploader") and None) or h.uploads.get(key)

        def button(label, key=None, on_click=None, args=(), disabled=False, **k):
            h.widget(key, None, "button")
            h.registry[key] = (on_click, args, disabled, label)
            return key in h.pressed and not disabled
        st.button = button

        def download_button(label, data, file_name=None, mime=None, key=None, **k):
            h.widget(key, None, "download_button")
            h.downloads[key] = data
            return False
        st.download_button = download_button

        def data_editor(data, key=None, on_change=None, **k):
            h.widget(key, {"edited_rows": {}, "added_rows": [], "deleted_rows": []}, "data_editor")
            h.editors[key] = on_change
            return data
        st.data_editor = data_editor

        class Bar:
            def progress(self, *a, **k): pass
            def empty(self): pass
        st.progress = lambda *a, **k: Bar()
        st.column_config = types.SimpleNamespace(
            CheckboxColumn=lambda *a, **k: {}, SelectboxColumn=lambda *a, **k: {})
        return st

    # ---- driving the app ----------------------------------------------------
    def run(self):
        sys.modules["streamlit"] = self.st
        for _ in range(12):
            self.out, self.current, self.instantiated, self.metrics = [], set(), set(), {}
            self.in_script = True
            try:
                runpy.run_path(APP, run_name="__main__")
            except StopApp:
                pass
            except RerunRequest:
                self.pressed = set()
                continue
            finally:
                self.in_script = False
            # Streamlit forgets the value of any widget that wasn't drawn in this run
            for k in self.known - self.current:
                self.state.pop(k, None)
                self.registry.pop(k, None)
            self.known = set(self.current)
            self.pressed = set()
            return
        raise AssertionError("app kept asking to rerun")

    def _callback(self, fn, args=()):
        self.in_callback = True
        try:
            if fn:
                fn(*args)
        finally:
            self.in_callback = False

    def click(self, key):
        assert key in self.registry, f"no button {key!r} on screen. On screen: {sorted(self.registry)[:40]}"
        on_click, args, disabled, label = self.registry[key]
        assert not disabled, f"button {key!r} ({label}) is disabled"
        self._callback(on_click, args)
        self.pressed = {key}
        self.run()

    def set(self, key, value):
        assert key in self.current, f"widget {key!r} isn't on screen. On screen: {sorted(self.current)[:40]}"
        dict.__setitem__(self.state, key, value)
        self.run()

    def edit(self, key, row, col, value):
        assert key in self.editors, f"no editor {key!r}"
        dict.__setitem__(self.state, key, {"edited_rows": {row: {col: value}}, "added_rows": [], "deleted_rows": []})
        self._callback(self.editors[key])
        self.run()

    def start(self):
        self.run()
        self.uploads["uploader"] = Upload("customers.csv", CSV)
        self.run()
        self.click("load_file")


class State(dict):
    def __init__(self, h):
        super().__init__()
        object.__setattr__(self, "_h", h)

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError:
            raise AttributeError(k) from None

    def __setattr__(self, k, v):
        self[k] = v

    def __delattr__(self, k):
        del self[k]

    def __setitem__(self, k, v):
        h = self._h
        if h.in_script and not h.in_callback and k in h.instantiated:
            raise AssertionError(f"Streamlit would refuse: session_state[{k!r}] set after that widget was created in this run")
        super().__setitem__(k, v)


REVIEW, TRANSFORM, OUTPUT, EXPORT = "2 · Review", "3 · Transform", "4 · Output", "5 · Export"


class AppFlow(unittest.TestCase):
    def setUp(self):
        self.h = Harness()

    def test_import_and_review(self):
        h = self.h
        h.run()
        self.assertTrue(h.seen("1 · Import your data", "header"))
        h.start()
        self.assertEqual(h.state.load.df.shape, (32, 10))
        self.assertTrue(h.seen("Loaded", "success"))
        h.click("import_next")
        self.assertEqual(h.state.nav, REVIEW)
        self.assertFalse(h.texts("error"))
        self.assertEqual(h.metrics["Rows"], "32")
        self.assertEqual(h.metrics["Unreadable values"], "2")
        key = f"schema_{h.state.schema_ver}"
        names = h.state.schema_df["Source field"].tolist()
        h.edit(key, names.index("full_name"), "New name", "Name")
        self.assertIn("Name", h.state.schema_df["New name"].tolist())
        h.edit(key, names.index("district"), "Type", "integer")
        self.assertGreater(int(h.metrics["Unreadable values"].replace(",", "")), 20)
        h.edit(key, names.index("district"), "Type", "text")
        self.assertEqual(h.metrics["Unreadable values"], "2")
        h.edit(key, names.index("plan"), "New name", "Name")             # clash with the rename above
        self.assertTrue(h.seen("same name", "error"))
        self.assertTrue(h.registry["review_next"][2], "Continue must be disabled while names clash")
        h.edit(key, names.index("plan"), "New name", "plan")
        self.assertFalse(h.texts("error"))

    def test_settings_and_edits_survive_changing_page(self):
        h = self.h
        h.start()
        h.click("import_next")
        h.set("w_dayfirst", True)
        h.set("w_tokens_text", "na, missing")
        key = f"schema_{h.state.schema_ver}"
        h.edit(key, 1, "New name", "Customer name")
        h.click("review_next")                               # settings widgets are not drawn here
        self.assertNotIn("w_dayfirst", h.state)
        h.click("tr_back")
        self.assertTrue(h.state.sticky["dayfirst"])
        self.assertTrue(h.state["w_dayfirst"])
        self.assertEqual(h.state["w_tokens_text"], "na, missing")
        self.assertEqual(h.state.schema_df.at[1, "New name"], "Customer name")

    def add_step(self, kind, **widgets):
        h = self.h
        h.set(h.key("nb_kind_"), kind)
        for prefix, value in widgets.items():
            h.set(h.key(prefix), value)
        n = len(h.state.steps)
        h.click(h.key("nb_add_"))
        self.assertEqual(len(h.state.steps), n + 1, h.texts("error"))

    def run_pipeline(self):
        h = self.h
        h.start()
        h.click("import_next")
        h.click("review_next")
        self.assertEqual(h.state.nav, TRANSFORM)
        self.add_step("dedupe")
        self.add_step("filter", nb_fcol_="plan", nb_fval_text_="premium")
        self.add_step("calc", nb_cname_="annual_fee", nb_cleft_="monthly_fee", nb_cop_="*", nb_crk_="number", nb_cnum_="12")
        h.click("run")

    def test_full_journey_transform_output_export(self):
        h = self.h
        self.run_pipeline()
        self.assertEqual(h.state.nav, OUTPUT)
        res = h.state.result
        self.assertTrue(res.success, res.errors)
        self.assertTrue(h.seen("Finished", "success"))
        self.assertIn("annual_fee", res.output.columns)
        self.assertEqual(set(res.output["plan"]), {"premium"})
        self.assertEqual(len(res.rejected), 2)
        self.assertEqual(h.metrics["Set aside (couldn't process)"], "2")
        self.assertIn("rej_dl", h.downloads)
        # stale-output warning once a step is removed
        h.click("out_steps")
        h.click("del_2")
        h.set("nav", OUTPUT)
        self.assertTrue(h.seen("out of date", "warning"))
        h.click("out_rerun_stale")
        self.assertFalse(h.seen("out of date", "warning"))
        self.assertNotIn("annual_fee", h.state.result.output.columns)
        # export
        h.click("out_next")
        self.assertEqual(h.state.nav, EXPORT)
        self.assertTrue(h.downloads["dl_csv"].startswith(b"customer_id,"))
        recs = json.loads(h.downloads["dl_json"])
        self.assertEqual(len(recs), len(h.state.result.output))

    def test_recipe_roundtrip(self):
        h = self.h
        self.run_pipeline()
        h.click("out_steps")
        recipe = h.downloads["recipe_dl"]
        for _ in range(3):
            h.click("del_0")
        self.assertEqual(h.state.steps, [])
        h.uploads[f"recipe_up_{h.state.recipe_ver}"] = Upload("r.json", recipe)
        h.run()
        h.click("recipe_apply")
        self.assertEqual(len(h.state.steps), 3)
        self.assertTrue(h.seen("Settings applied", "success"))

    def test_bad_files_show_clear_errors(self):
        h = self.h
        h.run()
        h.uploads["uploader"] = Upload("empty.csv", b"")
        h.run()
        h.click("load_file")
        self.assertIsNone(h.state.load)
        self.assertTrue(h.seen("empty", "error"))
        h.uploads["uploader"] = Upload("notes.pdf", b"%PDF")
        h.run()
        h.click("load_file")
        self.assertTrue(h.seen("aren't supported", "error"))

    def test_start_over(self):
        h = self.h
        h.start()
        h.click("start_over")
        h.run()
        self.assertIsNone(h.state.load)


class ApiFlow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = mock_api.make_server(0)
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        self.h = Harness()

    def api_import(self, token):
        h = self.h
        h.run()
        h.set("w_import_mode", "Connect to an API")
        h.set("w_api_url", self.base + "/customers")
        h.set("w_api_auth_kind", "bearer")
        h.set("w_api_token", token)
        h.click("fetch_api")

    def test_api_import_ok_and_failure(self):
        h = self.h
        self.api_import("wrong-token")
        self.assertIsNone(h.state.load)
        self.assertTrue(h.seen("401", "error"))
        self.assertFalse(any("wrong-token" in t for _, t in h.out))
        h.set("w_api_token", mock_api.TOKEN)
        h.click("fetch_api")
        self.assertEqual(h.state.load.kind, "api")
        self.assertEqual(len(h.state.load.df), 3)

    def test_push_end_to_end(self):
        h = self.h
        mock_api.RECEIVED.clear()
        self.api_import(mock_api.TOKEN)
        h.click("import_next")
        h.click("review_next")
        h.click("run")
        h.click("out_next")
        self.assertEqual(h.state.nav, EXPORT)
        cols = list(h.state.result.output.columns)
        h.set("w_push_url", self.base + "/ingest")
        h.set("w_push_auth_kind", "bearer")
        h.set("w_push_token", mock_api.TOKEN)
        key = f"mapping_{h.state.map_ver}"
        for i, c in enumerate(cols):
            if c not in ("id", "name"):
                h.edit(key, i, "Send", False)
        h.set("push_preview", True)
        self.assertTrue(h.seen('"id"', "json"))
        self.assertTrue(h.registry["push_go"][2], "Send must be disabled until the user confirms")
        h.set("push_confirm", True)
        h.click("push_go")
        pr = h.state.push_result
        self.assertTrue(pr.success, pr.message)
        self.assertEqual([r["name"] for r in mock_api.RECEIVED], ["Aminata Kamara", "mohamed sesay", "Fatmata Bangura"])
        self.assertTrue(h.seen("accepted", "success"))
        # leave and come back: URL and credentials are still there
        h.click("exp_back")
        h.click("out_next")
        self.assertEqual(h.state["w_push_url"], self.base + "/ingest")

    def test_push_failure_is_reported(self):
        h = self.h
        self.api_import(mock_api.TOKEN)
        h.click("import_next")
        h.click("review_next")
        h.click("run")
        h.click("out_next")
        h.set("w_push_url", self.base + "/ingest")
        h.set("w_push_auth_kind", "bearer")
        h.set("w_push_token", "nope")
        h.set("push_confirm", True)
        h.click("push_go")
        self.assertFalse(h.state.push_result.success)
        self.assertTrue(h.seen("accepted", "error"))


if __name__ == "__main__":
    unittest.main()
