"""OSM Importer - Importateur de données géographiques OpenStreetMap."""

__version__ = "1.0.0"
__author__ = "OSM Importer Team"

from .main import OSMImporter
from .config import Config

__all__ = ["OSMImporter", "Config"]

# osm_importer/utils/__init__.py
"""Utilitaires pour l'importateur OSM."""

# osm_importer/parsers/__init__.py
"""Parseurs pour les données OSM."""

# osm_importer/processors/__init__.py
"""Processeurs pour l'enrichissement des données."""

# osm_importer/database/__init__.py
"""Opérations de base de données."""
