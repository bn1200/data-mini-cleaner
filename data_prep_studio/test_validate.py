import unittest

import pandas as pd

from models import ColumnRule, DatasetSchema
from validate import (
    check_expected_columns,
    find_duplicates,
    find_missing_values,
    validate_column_rule,
    validate_schema,
)


class ValidationTests(unittest.TestCase):
    def test_expected_columns_are_reported(self):
        frame = pd.DataFrame({"name": ["Ada"], "extra": [1]})
        schema = DatasetSchema(
            expected_columns=["name", "email"],
            column_rules=[],
        )

        missing, unexpected, issues = check_expected_columns(frame, schema)

        self.assertEqual(missing, ["email"])
        self.assertEqual(unexpected, ["extra"])
        self.assertEqual([issue.code for issue in issues],
                         ["MISSING_EXPECTED_COLUMN", "UNEXPECTED_COLUMN"])

    def test_missing_values_include_blank_strings(self):
        frame = pd.DataFrame({"name": ["Ada", "  ", None]})

        counts, ratios = find_missing_values(frame)

        self.assertEqual(counts, {"name": 2})
        self.assertAlmostEqual(ratios["name"], 2 / 3)

    def test_duplicate_ratio_handles_empty_data(self):
        self.assertEqual(find_duplicates(pd.DataFrame({"name": []})), (0, 0.0))
        self.assertEqual(find_duplicates(pd.DataFrame({"name": ["Ada", "Ada"]})), (1, 0.5))

    def test_column_rules_report_missing_and_bad_numbers(self):
        frame = pd.DataFrame({"amount": ["12", "bad", ""]})
        rule = ColumnRule(name="amount", dtype="number", allow_missing=False)

        issues = validate_column_rule(frame, rule)

        self.assertEqual([issue.code for issue in issues],
                         ["MISSING_REQUIRED_VALUE", "INVALID_NUMBER"])
        self.assertEqual([issue.row_number for issue in issues], [4, 3])

    def test_required_column_is_reported_when_absent(self):
        issues = validate_column_rule(pd.DataFrame({"other": [1]}), ColumnRule(name="amount"))

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].code, "REQUIRED_COLUMN_NOT_FOUND")

    def test_schema_validation_combines_checks(self):
        frame = pd.DataFrame({"email": ["invalid"]})
        schema = DatasetSchema(
            expected_columns=["email", "name"],
            column_rules=[ColumnRule(name="email", dtype="email")],
        )

        missing, unexpected, issues = validate_schema(frame, schema)

        self.assertEqual(missing, ["name"])
        self.assertEqual(unexpected, [])
        self.assertEqual({issue.code for issue in issues},
                         {"MISSING_EXPECTED_COLUMN", "INVALID_EMAIL"})


if __name__ == "__main__":
    unittest.main()
