import unittest
import yaml

from cbc_dbt_pre_commit.find_missing_model_unique_tests import check_unique_constraints


class TestCheckUniqueConstraints(unittest.TestCase):

    def test_model_with_unique_constraint_single_column(self):
        yaml_data = """
models:
  - name: test_model_with_unique_constraint
    columns:
      - name: id
        unique: true
      - name: name
        meta:
          dimension:
            hidden: true
        """
        data = yaml.safe_load(yaml_data)
        errors = check_unique_constraints(data, unique_columns=["id"])
        self.assertEqual(errors, [])

    def test_model_missing_unique_constraint_single_column(self):
        yaml_data = """
models:
  - name: test_model_missing_unique_constraint
    columns:
      - name: id
      - name: name
        meta:
          dimension:
            hidden: true
        """
        data = yaml.safe_load(yaml_data)
        errors = check_unique_constraints(data, unique_columns=["id"])
        self.assertIn(
            "Missing unique constraint on column 'id' in model 'test_model_missing_unique_constraint'.",
            errors,
        )

    def test_model_with_unique_constraint_combination(self):
        yaml_data = """
models:
  - name: test_model_with_unique_constraint_combination
    columns:
      - name: id
        unique: true
      - name: date
        unique: true
        """
        data = yaml.safe_load(yaml_data)
        errors = check_unique_constraints(data, unique_columns=[["id", "date"]])
        self.assertEqual(errors, [])

    def test_model_missing_unique_constraint_combination(self):
        yaml_data = """
models:
  - name: test_model_missing_unique_constraint_combination
    columns:
      - name: id
        unique: true
      - name: date
        """
        data = yaml.safe_load(yaml_data)
        errors = check_unique_constraints(data, unique_columns=[["id", "date"]])
        self.assertIn(
            "Missing unique constraints on columns ['date'] in model 'test_model_missing_unique_constraint_combination'.",
            errors,
        )

    def test_model_no_unique_columns_required(self):
        yaml_data = """
models:
  - name: test_model_no_unique_columns_required
    columns:
      - name: id
      - name: date
        """
        data = yaml.safe_load(yaml_data)
        errors = check_unique_constraints(data)
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()