# Write down the rules to compile the project

.PHONY: check-uv install-uv install-precommit install

check-uv:
	@command -v uv >/dev/null 2>&1 || { echo >&2 "uv is not installed. Installing..."; $(MAKE) install-uv; }

install-uv:
	@curl -LsSf https://astral.sh/uv/install.sh | sh

install-precommit:
	@uv run pre-commit install

install: check-uv install-precommit
	@uv sync
