import argparse
from typing import List, Optional, Sequence, Union
import yaml

def check_unique_constraints(
        data: dict, unique_columns: Optional[List[Union[str, List[str]]]] = None
) -> list:
    errors = []

    for model in data.get("models", []):
        model_name = model["name"]
        # Check columns for unique constraints
        columns = model.get("columns", [])
        data_tests = model.get("data_tests", [])

        has_unique_constraints = False

    #check unique constraints at the column level
    for column in columns:
        if "unique" in column.get("data_tests", []):
            has_unique_constraints = True
            break

    # check unique combinations at the model level
    if not has_unique_constraints and data_tests:
        for test in data_tests:
            if "dbt_utils.unique_combination_of_columns" in test:
                has_unique_constraints = True
                break

    # check specific columns or combinations id specified
    if not has_unique_constraints and unique_columns:
        for col in unique_columns:
            if isinstance(col, str):  # single column
                for c in columns:
                    if any(c["name"] == col and "unique" in c.get("data_tests", [])):
                        has_unique_constraints = True
                        break
            elif isinstance(col, list): # combination of columns
                found_combination = any(
                    test.get("dbt_utils.unique_combination_of_columns") and
                    set(test["dbt_utils.unique_combination_of_columns"]).get("combination_of_columns", []) == set(col)
                    for test in data_tests
                )
                if found_combination:
                    has_unique_constraints = True
                    break

    # If no unique constraints are found, append an error
    if not has_unique_constraints:
        errors.append(
            f"Model '{model_name}' has no unique constraints defined."
        )

    return errors




    #     if unique_columns:
    #         for col in unique_columns:
    #             if isinstance(col, str):
    #                 # Check if a single column has a unique constraint
    #                 column_data = next((c for c in columns if c["name"] == col), None)
    #                 if column_data and  column_data.get("unique"):
    #                     has_unique_constraints = True
    #                     break
    #             elif isinstance(col, list):
    #                 # Check if a combination of columns has a unique constraint
    #                 missing_columns = [c for c in col if not any(col_data["name"] == c and col_data.get("unique") for col_data in columns)]
    #
    #                 if not missing_columns:
    #                     has_unique_constraints = True
    #                     break
    #
    #                     # If not found, check the data_tests section for unique combination tests
    #                 found_combination = any(
    #                     test.get("dbt_utils.unique_combination_of_columns") and
    #                     set(test["dbt_utils.unique_combination_of_columns"].get("combination_of_columns", [])) == set(col)
    #                     for test in data_tests
    #                 )
    #                 if found_combination:
    #                     has_unique_constraints = True
    #                     break
    #
    #     if not has_unique_constraints:
    #         errors.append(
    #             f"Model '{model_name}' has no unique constraints."
    #         )
    #
    # return errors

def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("filenames", nargs="*", help="Filenames to check")
    parser.add_argument(
        "--unique-columns",
        type=str,
        help="Comma-separated list of columns or column combinations for unique constraints (use '+' for combinations)",
    )
    args = parser.parse_args(argv)

    # Parse unique column argument, supporting combinations like "col1+col2"
    unique_columns = [
        combo.split("+") if "+" in combo else combo
        for combo in args.unique_columns.split(",")
    ] if args.unique_columns else None

    error_flag = False
    for file_path in args.filenames:
        try:
            with open(file_path, "r") as file:
                data = yaml.safe_load(file)
                errors = check_unique_constraints(data, unique_columns)
                if errors:
                    print(f"Errors found in '{file_path}':")
                    for error in errors:
                        print(error)
                    error_flag = True
        except Exception as e:
            print(f"Failed to process '{file_path}': {e}")
            error_flag = True

    if error_flag:
        return 1

    return 0

if __name__ == "__main__":
    exit(main(None))