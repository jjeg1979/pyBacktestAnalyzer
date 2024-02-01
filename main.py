from src.parsegbx.parsegbx import (
    parse_gbx_bt,
)


def main() -> None:
    """Main function"""
    file: str = "payload/acd7_S_3_01_231231_set133.htm"

    df = parse_gbx_bt(file)

    print(df)


if __name__ == "__main__":
    main()
