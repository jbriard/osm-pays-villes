#!/usr/bin/env python3
"""Script de lancement direct pour l'importateur OSM."""

import sys
import os
from pathlib import Path

# Ajouter le répertoire courant au PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import et lancement
from osm_importer.main import cli

if __name__ == '__main__':
    cli()
