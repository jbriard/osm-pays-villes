# OSM Importer - Importateur de données géographiques OpenStreetMap

Importateur performant et robuste pour extraire les pays et villes depuis les fichiers OpenStreetMap (.osm.pbf) vers une base PostgreSQL.

## Fonctionnalités

- ✅ Extraction des pays (boundary=administrative, admin_level=2)
- ✅ Extraction des villes (place=city/town/village/hamlet)
- ✅ Gestion multi-langues (français, anglais, local)
- ✅ Enrichissement automatique (timezones, continents, devises)
- ✅ Simplification des frontières avec Douglas-Peucker
- ✅ Import par batch avec parallélisation
- ✅ Mode UPSERT pour les mises à jour
- ✅ Suivi de progression en temps réel
- ✅ Gestion robuste des erreurs

## Installation

```bash
# Cloner le repository
git clone <repo-url>
cd osm-importer

# Installer les dépendances
pip install -r requirements.txt

# Créer la base de données PostgreSQL
createdb geodata
```

## Configuration

Copier et adapter le fichier `config.yaml` :

```yaml
database:
  host: localhost
  port: 5432
  name: geodata
  user: postgres
  password: your_password

import:
  batch_size: 1000
  num_workers: 4
  simplification_tolerance: 0.01

logging:
  level: INFO
  file: import.log
```

## Utilisation

```bash
# Import d'un fichier OSM
python -m osm_importer.main --file Europe.osm.pbf --config config.yaml

# Avec options personnalisées
python -m osm_importer.main \
  --file Africa.osm.pbf \
  --config config.yaml \
  --workers 8
```

## Structure des données

### Table `countries`
- Métadonnées complètes des pays
- Codes ISO alpha2/alpha3
- Frontières simplifiées en GeoJSON
- Informations enrichies (timezone, continent, devise)

### Table `cities`
- Villes, villages et hameaux
- Coordonnées précises
- Liaison automatique avec les pays
- Classification par type de lieu

## Performance

- **Streaming**: Traitement par flux, jamais de chargement complet en mémoire
- **Parallélisation**: Traitement multi-worker configurable
- **Batch processing**: Import par lot pour optimiser les performances
- **Index**: Création automatique des index après import

## Exemple de sortie

```
Importing Europe.osm.pbf
Countries: [████████████████████] 100% (47/47) - 2.3s
Cities:    [████████░░░░░░░░░░░░] 45% (234,567/521,234) - ETA: 3m 42s
- Extracted: 234,567 | Processed: 232,100 | Imported: 230,000 | Errors: 127
```

## Fichiers sources OSM

Télécharger les fichiers régionaux depuis [Geofabrik](https://download.geofabrik.de/) :
- europe-latest.osm.pbf
- africa-latest.osm.pbf
- asia-latest.osm.pbf
- etc.

## Architecture modulaire

```
osm_importer/
├── main.py                 # Point d'entrée principal
├── config.py              # Configuration Pydantic
├── models.py              # Modèles SQLAlchemy
├── parsers/               # Extraction OSM
├── processors/            # Enrichissement et simplification
├── database/              # Opérations base de données
└── utils/                 # Utilitaires (logs, progression)
```

## Dépendances principales

- **osmium**: Parsing haute performance des fichiers .pbf
- **SQLAlchemy 2.0+**: ORM moderne avec support async
- **asyncpg**: Driver PostgreSQL asynchrone
- **shapely**: Manipulation géométrique
- **timezonefinder**: Détection automatique des fuseaux horaires
- **rich**: Barres de progression élégantes

## Robustesse

- Gestion complète des erreurs avec logs détaillés
- Transactions par batch avec rollback automatique
- Validation des données avec Pydantic
- Encodage UTF-8 pour les caractères spéciaux
- Mode UPSERT pour éviter les doublons
