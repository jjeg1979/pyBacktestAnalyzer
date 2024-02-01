from src.parsegbx.parsegbx import (
    read_htm_from,
    read_table_from,
    read_rows_from,
    extract_row_data_from,
    separate_columns_in,
    convert_to_dataframe,
    assign_correct_data_type_to,
)


def main() -> None:
    """Main function"""
    htm_str = read_htm_from("payload/acd7_S_3_01_231231_set133.htm")
    table = read_table_from(htm_str)
    rows = read_rows_from(table)
    row_data = extract_row_data_from(
        rows,
        ["Closed Transactions:"],
        ["Genbox", "balance", "Deposit"],
        ["Closed P/L:"],
    )
    df = assign_correct_data_type_to(
        convert_to_dataframe(separate_columns_in(row_data))
    )

    print(df)


if __name__ == "__main__":
    main()
