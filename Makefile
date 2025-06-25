# Makefile
.PHONY: install test import clean

# Installation des dépendances
install:
	pip install -r requirements.txt

# Test de base
test:
	python -c "import osm_importer; print('✅ Module OK')"

# Import avec paramètres par défaut
import:
	python run_import.py --file $(FILE) --config config.yaml

# Import Antarctica (exemple)
import-antarctica:
	python run_import.py --file antarctica-latest.osm.pbf --config config.yaml

# Nettoyage
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
