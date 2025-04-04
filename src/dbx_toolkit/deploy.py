"""Deploy module for ....

This module automates the generation of ... files for ...
based on a CSV input file using Jinja2 templates. It reads data from a CSV file, uses
it to fill out a predefined Jinja template, and outputs ... files for each row in the CSV.

Dependencies:
- pandas: Used for reading and processing the CSV file.
- jinja2: Used for loading and rendering templates.

Expected CSV Format:
- The CSV should have headers and include at least the following columns:
  ...,...

Output Files:
- For each entry in the CSV, one ... file is generated.

Each file is named according to the ... and is written to the current working directory.

Example Usage:
- Run the script after setting up the environment and ensure the CSV and template files are in the current directory.
- The script can be executed manually or as part of a CI/CD process, such as a pre-commit hooks or GitHub Actions workflow.

"""

from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader

template_dir = Path("fixtures")
output_dir = Path("fixtures/output")

# Load data from CSV
df_target = pd.read_csv(template_dir / "notebooks.csv")

# Setting up the Jinja2 environment
jinja_env = Environment(
    loader=FileSystemLoader("./"), trim_blocks=True, lstrip_blocks=True, autoescape=True
)

# Load templates
template = jinja_env.get_template(str(template_dir / "notebook_template.py.jinja"))

# Iterate jobs
for _, row in df_target.iterrows():
    print("Building notebook:", row["table_name"])

    # Rendering the pipeline template
    rendered_job = template.render(
        table_name=row["table_name"],
    )

    # Write the output to files, adding a newline at the end of file
    with Path(output_dir / f"{row['table_name']}.py").open("w") as f:
        f.write(rendered_job + "\n")

print("Notebooks built successfully.")
