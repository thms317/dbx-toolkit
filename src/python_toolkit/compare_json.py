"""Module to compare two JSON files and print differences."""

import json
from pathlib import Path
from typing import Any


def load_json(file_path: str) -> dict[str, Any]:
    """Load JSON data from a file."""
    with Path(file_path).open() as file:
        return dict(json.load(file))


def compare_json(file1: str, file2: str) -> None:
    """Compare two JSON files and print differences."""
    # Load JSON data from both files
    data1 = load_json(file1)
    data2 = load_json(file2)

    # Compare the two JSON objects
    if data1 == data2:
        print("The JSON files are identical.")
    else:
        print("The JSON files are NOT identical.")
        # Print detailed differences
        print("\nDifferences:")
        compare_dicts(data1, data2)


def compare_dicts(dict1: dict[str, Any], dict2: dict[str, Any], path: str = "") -> None:
    """Recursively compare two dictionaries and print differences."""
    for key, value in dict1.items():
        if key not in dict2:
            print(f"Key '{key}' not found in the second file at path '{path}'")
        elif isinstance(value, dict) and isinstance(dict2[key], dict):
            compare_dicts(value, dict2[key], path + f".{key}" if path else key)
        elif value != dict2[key]:
            print(f"Value mismatch at '{path + '.' + key if path else key}':")
            print(f"  File 1: {value}")
            print(f"  File 2: {dict2[key]}")

    # Check for keys in dict2 that are not in dict1
    for key in dict2:
        if key not in dict1:
            print(f"Key '{key}' not found in the first file at path '{path}'")


# Example usage
file1 = "src/desk_validation/model_tree.json"
file2 = "src/model_extraction/output/model_tree.json"
compare_json(file1, file2)
