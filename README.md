# dbx-toolkit

[![python](https://img.shields.io/badge/python-3.11-g)](https://www.python.org)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![pydocstyle](https://img.shields.io/badge/pydocstyle-enabled-AD4CD3)](http://www.pydocstyle.org/en/stable/)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
[![ci](https://github.com/revodatanl/dbx-toolkit/actions/workflows/ci.yml/badge.svg)](https://github.com/revodatanl/dbx-toolkit/actions/workflows/ci.yml)
[![semantic-release](https://github.com/revodatanl/dbx-toolkit/actions/workflows/semantic-release.yml/badge.svg)](https://github.com/revodatanl/dbx-toolkit/actions/workflows/semantic-release.yml)

[![tests](docs/assets/badge-tests.svg)](docs/tests/coverage/index.html)
[![coverage](docs/assets/badge-coverage.svg)](docs/tests/coverage/index.html)

Toolkit to quickly diagnose and fix Databricks issues.

The `dbx-toolkit` Bundle was generated by using the [RevoData Asset Bundle Template](https://github.com/revodatanl/revo-asset-bundle-templates) version `0.9.0`.

## Prerequisites

The project heavily depends on the provided `Makefile` for various tasks. Without [make](https://www.gnu.org/software/make) installed, you will need to run the commands described in the `Makefile` manually.

Note that on **Windows** most commands need to be adjusted to function properly.

## Installation

With [make](https://www.gnu.org/software/make) installed, run the following command to install the prerequisites and set up a fully configured development environment:

```bash
make setup
```

This will install the following prerequisites:

- [Homebrew](https://brew.sh)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)
- [Git](https://git-scm.com)
- [uv](https://github.com/astral-sh/uv)

![make-install](docs/assets/make-install.png)

Subsequently, this will use [uv](https://github.com/astral-sh/uv) to configure the correct [Python](https://www.python.org/) version specified in the `.python-version` file. Subsequently it will install the dependencies, set up a virtual environment, activate it, and install the pre-commit hooks.

Note that the default project configuration matches Databricks Runtime 15.4 LTS (Python 3.11.0, Apache Spark 3.5.0, and pandas 1.5.3).

![make-setup](docs/assets/make-setup.png)

## Clean up the development environment

To deactivate and remove the virtual environment, lock file(s), and caches, run the following command:

```bash
make clean
```

![make-clean](docs/assets/make-clean.png)

## RevoData Modules

To add our custom modules to your project, run the following command:

```bash
make module
```

![make-module](docs/assets/make-module.png)

## Create GitHub repository

To create a repository in RevoData's GitHub, and add a remote to the local git repository containing the Bundle, use the following commands:

```bash
make repo
```

This assumes that the [GitHub CLI](https://cli.github.com) is installed, and access to the RevoData GitHub organization is granted and configured.

![make-repo](docs/assets/make-repo.png)


## Bundle Deployment

To deploy the Bundle to the appropriate Databricks workspace, use the following commands:

```bash
make deploy_*
```

The `*` in the command above can be replaced with the following options: `dev` or `prd`.

![make-deploy_dev](docs/assets/make-deploy_dev.png)

## Bundle Destruction

To remove the Bundle from the Databricks workspace, use the following command:

```bash
make destroy_*
```

The `*` in the command above can be replaced with the following options: `dev` or `prd`.

![make-destroy_dev](docs/assets/make-destroy_dev.png)

## Documentation

To auto-generate the documentation for the `dbx-toolkit` project, run the following command:

```bash
make docs
```
