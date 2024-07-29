# Write down the rules to compile the project

.PHONY: check-pip install-pip check-poetry install-poetry install-project

check-pip:
	@command -v pip >/dev/null 2>&1 || { echo >&2 "pip is not installed. Installing..."; $(MAKE) install-pip; }

install-pip:
	@curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
	@python get-pip.py
	@rm get-pip.py

check-poetry:
	@command -v poetry >/dev/null 2>&1 || { echo >&2 "Poetry is not installed. Installing..."; $(MAKE) install-poetry; }

install-poetry:
	@curl -sSL https://install.python-poetry.org | python3 -
	@export PATH=$HOME/.local/bin:$PATH

install-precommit:
	@poetry run pre-commit install

install: check-pip check-poetry install-precommit
	@poetry install --with dev
