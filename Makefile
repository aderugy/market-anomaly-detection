.PHONY: help setup data download test lint format check clean

RUN := uv run

help:  ## Affiche les commandes disponibles
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  make %-12s %s\n", $$1, $$2}'

setup:  ## Installe l'environnement (venv + dépendances verrouillées)
	uv sync

data: download clean-data

download:  ## Télécharge les données brutes dans data/raw/ (+ SOURCES.md)
	$(RUN) scripts/download_data.py

clean-data:
	$(RUN) scripts/clean_data.py

test:  ## Lance les tests
	$(RUN) pytest

lint:  ## Vérifie le style et les erreurs courantes
	$(RUN) ruff check .

format:  ## Formate le code
	$(RUN) ruff format .
	$(RUN) ruff check --fix .

check: lint test  ## Lint + tests (avant chaque commit)

clean:  ## Supprime les fichiers générés (garde data/raw)
	rm -rf data/clean .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +