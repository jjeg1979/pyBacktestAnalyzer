from functools import reduce, partial
from pathlib import Path
from typing import List, Dict, Callable

type L = str | List[Path] | dict[str, List[Path]]

FileProcessorFunction = Callable[[L], L]


def compose(*functions: FileProcessorFunction) -> FileProcessorFunction:
    """Composes functions into a single function"""
    return reduce(lambda f, g: lambda x: g(f(x)), functions, lambda x: x)  # type: ignore


def process_and_group_files(group_names: List[str]) -> FileProcessorFunction:
    """
    Scan files in the given directory.

    Args:
        directory (str): The path of the directory to scan.

    Returns:
        List[Path]: A list of Path objects representing files in the directory.
    """
    groups = initialize_groups(group_names)
    return compose(
        scan_files,
        partial(filter_by_extension, extension=".htm"),
        partial(group_files_by_prefix, file_groups=groups),
    )


def initialize_groups(group_names: List[str]) -> Dict[str, List[str]]:
    """
    Initialize a dictionary with keys as group names and values as empty lists.

    Args:
        group_names (List[str]): List of strings representing group names.

    Returns:
        Dict[str, List[str]]: A dictionary with keys as group names and values as empty lists.
    """
    return {group_name: [] for group_name in group_names}


def scan_files(directory: str) -> List[Path]:
    """
    Scan files in the given directory.

    Args:
        directory (str): The path of the directory to scan.

    Returns:
        List[Path]: A list of Path objects representing files in the directory.
    """
    dir_path = Path(directory)

    if not dir_path.exists():
        print(f"Directory '{directory}' does not exist.")
        return []

    files = [file for file in dir_path.iterdir() if file.is_file()]

    return files


def filter_by_extension(files: List[Path], extension: str) -> List[Path]:
    """
    Filter files based on the provided extension.

    Args:
        files (List[Path]): The list of Path objects representing files.
        extension (str): The extension to filter files.

    Returns:
        List[Path]: A list of Path objects filtered by the provided extension.
    """
    filtered_files = [
        file for file in files if file.suffix.lower() == extension.lower()
    ]
    return filtered_files


def group_files_by_prefix(
    files: List[Path], file_groups: Dict[str, List[Path]]
) -> Dict[str, List[Path]]:
    """
    Group files based on the filename prefix.

    Args:
        files (List[Path]): The list of Path objects representing files.
        file_groups (Dict[str, List[str]]): A pre-initialized dictionary with keys corresponding to group names.

    Returns:
        Dict[str, List[str]]: A dictionary with keys corresponding to group names,
        where each key corresponds to a list of filenames (without parent directory or extension) in that category.
    """

    for file in files:
        filename = file.stem

        for group_name, group_list in file_groups.items():
            if filename.endswith(f"_{group_name}"):
                group_list.append(file)
                break
        else:
            file_groups["ISOS"].append(file)

    return file_groups
