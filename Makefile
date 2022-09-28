all: pytest

update:
	./update_deps
	pip install -e ".[testing]"

pytest:
	PYTHONPATH=. pytest --cache-clear
	