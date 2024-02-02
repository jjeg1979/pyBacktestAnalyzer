from pathlib import Path
from typing import List, Dict

from src.parsegbx.parsegbx import (
    parse_gbx_bt,
)

from src.parsegbx.gatherbtfiles import (
    process_and_group_files,  # type: ignore
)


def main() -> None:
    """Main function"""
    group_names: List[str] = ["IS", "OS", "ISOS"]
    files: Dict[str, List[Path]] = process_and_group_files(group_names=group_names)("payload")  # type: ignore
    # for groupname, filenames in files.items():  # type: ignore
    #     print(f"Group: {groupname}")
    #     for file in filenames:  # type: ignore
    #         print(f"  - {file.name}")
    file = files["ISOS"][0]  # type: ignore

    df = parse_gbx_bt(str(file))  # type: ignore

    print(f"Number of ops in the Backtest {file.stem}: {df.shape[0]}")  # type: ignore


if __name__ == "__main__":
    main()
