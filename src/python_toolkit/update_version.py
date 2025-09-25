#!/usr/bin/env python3

"""Script to update package version in pyproject.toml.

Usage: python scripts/update-version.py [major|minor|patch|<version>]
"""

import argparse
import re
import sys
from pathlib import Path


def read_pyproject_toml(file_path: Path) -> str:
    """Read the pyproject.toml file."""
    return file_path.read_text()


def write_pyproject_toml(file_path: Path, content: str) -> None:
    """Write content to pyproject.toml file."""
    file_path.write_text(content)


def get_current_version(content: str) -> str:
    """Extract current version from pyproject.toml content."""
    match = re.search(r'^version = "([^"]+)"', content, re.MULTILINE)
    if not match:
        msg = "Could not find version in pyproject.toml"
        raise ValueError(msg)
    return match.group(1)


def increment_version(current_version: str, increment_type: str) -> str:
    """Increment version based on type (major, minor, patch)."""
    major, minor, patch = map(int, current_version.split("."))

    if increment_type == "major":
        return f"{major + 1}.0.0"
    if increment_type == "minor":
        return f"{major}.{minor + 1}.0"
    if increment_type == "patch":
        return f"{major}.{minor}.{patch + 1}"
    # Assume it's a specific version string
    return increment_type


def update_version_in_content(content: str, new_version: str) -> str:
    """Update version in pyproject.toml content."""
    pattern = r'^version = "[^"]+"'
    replacement = f'version = "{new_version}"'
    return re.sub(pattern, replacement, content, flags=re.MULTILINE)


def main() -> int:
    """Update package version in pyproject.toml via command line."""
    parser = argparse.ArgumentParser(description="Update package version")
    parser.add_argument(
        "increment",
        choices=["major", "minor", "patch"],
        nargs="?",
        help="Version increment type or specific version string",
    )
    parser.add_argument("--version", help="Set specific version (e.g., 1.2.3)")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be changed without making changes"
    )

    args = parser.parse_args()

    # Determine version change
    if args.version:
        increment_type = args.version
    elif args.increment:
        increment_type = args.increment
    else:
        parser.print_help()
        return 1

    # Find pyproject.toml
    pyproject_path = Path("pyproject.toml")
    if not pyproject_path.exists():
        print("Error: pyproject.toml not found in current directory", file=sys.stderr)
        return 1

    try:
        # Read current content
        content = read_pyproject_toml(pyproject_path)
        current_version = get_current_version(content)

        # Calculate new version
        if increment_type in ["major", "minor", "patch"]:
            new_version = increment_version(current_version, increment_type)
        else:
            new_version = increment_type

        print(f"Current version: {current_version}")
        print(f"New version: {new_version}")

        if args.dry_run:
            print("Dry run - no changes made")
            return 0

        # Update content
        new_content = update_version_in_content(content, new_version)

        # Write back
        write_pyproject_toml(pyproject_path, new_content)
        print(f"Version updated successfully to {new_version}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
