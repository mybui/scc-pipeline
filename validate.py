import great_expectations as ge
import sys

def validate_dataframe(df):
    ge_df = ge.from_pandas(df)

    unique_row_count = len(df.drop_duplicates())
    result_unique = ge_df.expect_table_row_count_to_equal(unique_row_count)

    result_no_nulls = ge_df.expect_table_row_count_to_equal(len(ge_df.dropna()))

    results = {
        "unique_rows": result_unique['success'],
        "no_null_rows": result_no_nulls['success']
    }

    all_checks_passed = result_unique['success'] and result_no_nulls['success']
    results['all_checks_passed'] = all_checks_passed

    # Exit with an error code if any check fails
    if not results['all_checks_passed']:
        sys.exit('Data does not pass validation checks: unique and no-null rows. Exit.')