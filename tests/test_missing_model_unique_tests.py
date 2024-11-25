import unittest

import yaml

from cbc_dbt_pre_commit.find_missing_model_unique_tests import check_unique_constraints


class TestCheckUniqueConstraints(unittest.TestCase):

    def test_model_with_unique_constraint_single_column(self):
        """Test model with a single unique c"""
        yaml_data = """
models:
  - name: test_model_with_unique_constraint
    columns:
      - name: unique_col
        data_tests:
          - unique
        """
        data = yaml.safe_load(yaml_data)
        errors = check_unique_constraints(data)
        self.assertEqual(errors, [])

    def test_model_missing_unique_constraint_single_column(self):
        yaml_data = """
models:
  - name: test_model_missing_unique_constraint
    columns:
      - name: col1
        data_tests:
            - not_null
        """
        data = yaml.safe_load(yaml_data)
        errors = check_unique_constraints(data)
        self.assertIn(
            "Model 'test_model_missing_unique_constraint' has no unique constraints defined.",
            errors,
        )

    def test_model_with_unique_constraint_combination(self):
        yaml_data = """
models:
  - name: test_model_missing_unique_constraint_combination
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - col1
            - col2
        """
        data = yaml.safe_load(yaml_data)
        errors = check_unique_constraints(data)
        self.assertEqual(errors, [])

    def test_model_missing_unique_constraint_combination(self):
        yaml_data = """
models:
  - name: test_model_missing_unique_constraint_combination
    data_tests:
        - col1
        - col2
        """
        data = yaml.safe_load(yaml_data)
        errors = check_unique_constraints(data)
        print("Err msg", errors)
        self.assertIn(
            # "Missing unique constraint 'test_model_missing_unique_constraint_combination'.",
            "Model 'test_model_missing_unique_constraint_combination' has no unique constraints defined.",
            errors,
        )


if __name__ == "__main__":
    unittest.main()
