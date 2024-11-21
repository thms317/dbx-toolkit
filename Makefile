.PHONY: install setup clean test deploy_% destroy_% repo module module_% tree docs

install:
	@CURRENT_OS=$$(uname -s); \
	echo "Current OS: $$CURRENT_OS"; \
	if [ "$$CURRENT_OS" = "Darwin" ]; then \
		echo "Verifying if Homebrew is installed..."; \
		which brew > /dev/null || (echo "Homebrew is not installed. Installing Homebrew..." && /bin/bash -c "$$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"); \
		echo "Installing tools..."; \
		for tool in git uv; do \
			if ! command -v $$tool >/dev/null 2>&1; then \
				echo "Installing $$tool..."; \
				brew install $$tool; \
			else \
				echo "$$tool is already installed. Skipping."; \
			fi; \
		done; \
	elif [ "$$CURRENT_OS" = "Linux" ]; then \
		echo "Installing tools..."; \
		if ! command -v git >/dev/null 2>&1; then \
				echo "Installing git..."; \
				sudo apt update && sudo apt install -y git; \
			else \
				echo "git is already installed. Skipping."; \
			fi; \
		if ! command -v uv >/dev/null 2>&1; then \
			echo "Installing uv..."; \
			curl -LsSf https://astral.sh/uv/install.sh | sh; \
			echo "Sourcing ~/.bashrc to update shell environment..."; \
			source ~/.bashrc || true; \
			echo "Continuing even if sourcing ~/.bashrc failed..."; \
		else \
			echo "uv is already installed. Skipping."; \
		fi; \
	else \
		echo "Unsupported OS. Currently supported kernels are either Darwin (macOS) or Linux (ubuntu22.04)."; \
		exit 1; \
	fi; \

	@echo "Setting up Python..."; \
	uv python install || true; \
	echo "All tools installed successfully."

setup:
	@echo "Installing tools..."
	@{ \
		output=$$($(MAKE) install 2>&1); \
		exit_code=$$?; \
		if [ $$exit_code -ne 0 ]; then \
			echo "$$output"; \
			exit $$exit_code; \
		fi; \
	}
	@echo "All tools installed successfully."
	@echo "Setting up the project..."
	@uv sync;

	@if [ ! -d ".git" ]; then \
		echo "Setting up git..."; \
		git init -b main > /dev/null; \
	fi

	@echo "Setting up pre-commit..."
	@. .venv/bin/activate;
	@.venv/bin/pre-commit install --hook-type pre-commit --hook-type commit-msg;

clean:
	@echo "Cleaning up..."
	rm -rf .venv uv.lock
	find . -type d \
		\( -name ".pytest_cache" \
		-o -name ".mypy_cache" \
		-o -name ".ruff_cache" \
		-o -name "dist" \) \
		-exec rm -rf {} +
	@echo "Cleanup completed. Resetting terminal..."
	@reset

test:
	@echo "Running tests..."
	.venv/bin/pre-commit run --all-files
	@uv sync;
	@uv run pytest tests --cov=src --cov-report term;

deploy_%:
	@if [ "$*" != "dev" ] && [ "$*" != "prd" ]; then \
		echo "Error: Invalid environment. Use 'dev' or 'prd'."; \
		exit 1; \
	fi
	.venv/bin/pre-commit run --all-files
	uv sync
	uv build
	@PROFILE_NAME="DEFAULT"; \
	output=$$(databricks auth env --profile "$$PROFILE_NAME" 2>&1); \
	if [[ $$output == *"Error: resolve:"* ]]; then \
		databricks configure --profile "$$PROFILE_NAME"; \
	else \
		databricks bundle deploy --profile "$$PROFILE_NAME" $$(if [ "$*" != "dev" ]; then echo "--target $*"; fi); \
	fi

destroy_%:
	@if [ "$*" != "dev" ] && [ "$*" != "prd" ]; then \
		echo "Error: Invalid environment. Use 'dev' or 'prd'."; \
		exit 1; \
	fi
	@PROFILE_NAME="DEFAULT"; \
	output=$$(databricks auth env --profile "$$PROFILE_NAME" 2>&1); \
	if [[ $$output == *"Error: resolve:"* ]]; then \
		databricks configure --profile "$$PROFILE_NAME"; \
	else \
		databricks bundle destroy --profile "$$PROFILE_NAME" --target $*; \
	fi

# Create a repository in RevoData's GitHub, and adds a remote to the local git repo
repo:
	@printf "Creating repository in RevoData's GitHub...\n"
	@PROJECT_NAME="dbx-toolkit"; \
	REPO_DESCRIPTION=$$(grep 'description =' pyproject.toml | awk -F'"' '{print $$2}'); \
	if ! gh auth status >/dev/null 2>&1; then \
		echo "Error: GitHub CLI is not authenticated. Please run 'gh auth login' first."; \
		exit 1; \
	fi; \
	if ! gh repo view revodatanl/$$PROJECT_NAME > /dev/null 2>&1; then \
		gh repo create revodatanl/$$PROJECT_NAME -y --private -d "$$REPO_DESCRIPTION" > /dev/null 2>&1; \
		(git remote | grep origin || git remote add origin git@github.com:revodatanl/$$PROJECT_NAME.git) > /dev/null 2>&1; \
		printf "Repository created at revodatanl/$$PROJECT_NAME...\n"; \
		printf "Publishing project...\n"; \
		printf "Repository published.\n"; \
	else \
		printf "Repository revodatanl/$$PROJECT_NAME already exists.\n"; \
	fi

# Add custom RevoData modules to the project
module:
	@echo "Select the module to deploy:"
	@echo "1) Deploy Databricks Asset Bundle pipeline for GitHub"
	@echo "2) Deploy Databricks Asset Bundle pipeline for Azure DevOps"
	@echo "Enter the number of the module you want to deploy: "; \
	read choice; \
	case "$$choice" in \
		1) $(MAKE) module_github-deploy-dab ;; \
		2) $(MAKE) module_azure-devops-deploy-dab ;; \
		*) echo "Invalid choice. Exiting.";; \
	esac

module_%:
	@if [ "$*" != "github-deploy-dab" ] && [ "$*" != "azure-devops-deploy-dab" ]; then \
		echo "Error: Invalid module. Use 'github-deploy-dab' or 'azure-devops-deploy-dab'."; \
		exit 1; \
	fi
	@{ \
		set -e; \
		trap 'if [ -f databricks.yml.bak ]; then mv databricks.yml.bak databricks.yml; fi' EXIT; \
		if [ -f databricks.yml ]; then mv databricks.yml databricks.yml.bak; fi; \
		if ! databricks bundle init https://github.com/revodatanl/revo-asset-bundle-templates --template-dir modules/$* 2>&1; then \
			echo "Exiting." >&2; \
		fi; \
	}

tree:
	@echo "Generating project tree..."
	@tree -I '.venv|__pycache__|archive|scratch|.databricks|.ruff_cache|.mypy_cache|.pytest_cache|.git|htmlcov|site|dist|.DS_Store|fixtures' -a

docs:
	@echo "Running tests and generating badges..."
	@uv run pytest -v tests --cov=src --cov-report html:docs/tests/coverage --junitxml=docs/tests/coverage/pytest_coverage.xml
	@uv run coverage xml -o docs/tests/coverage/coverage.xml
	@uv run genbadge coverage -i docs/tests/coverage/coverage.xml -o docs/assets/badge-coverage.svg
	@uv run genbadge tests -i docs/tests/coverage/pytest_coverage.xml -o docs/assets/badge-tests.svg
	@rm -rf docs/tests/coverage/.gitignore
	@echo "Generating HTML documentation..."
	@uv run pdoc --html src/dbx_toolkit -o docs/api --force
	@uv run pdoc --html tests -o docs/api --force
	# @uv run mkdocs build
	@uv run mkdocs serve
