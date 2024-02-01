from src.parsegbx.parsegbx import (
    parse_gbx_bt,
)


def main() -> None:
    """Main function"""
    file: str = "payload/acd7_S_3_01_231231_set133.htm"

    df = parse_gbx_bt(file)

    print(f"Number of ops in the Backtest {file.rsplit('/', maxsplit=1)[-1].rsplit('.', maxsplit=1)[0]}: {df.shape[0]}")  # type: ignore


if __name__ == "__main__":
    main()
