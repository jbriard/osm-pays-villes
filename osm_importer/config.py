from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
import yaml


class DatabaseConfig(BaseModel):
    host: str = "localhost"
    port: int = 5432
    name: str = "geodata"
    user: str = "postgres"
    password: str = ""


class ImportConfig(BaseModel):
    batch_size: int = 1000
    num_workers: int = 4
    simplification_tolerance: float = 0.01


class OSMConfig(BaseModel):
    tags_countries: List[str] = [
        "boundary=administrative",
        "admin_level=2"
    ]
    tags_cities: List[str] = [
        "place=city",
        "place=town",
        "place=village",
        "place=hamlet"
    ]


class LoggingConfig(BaseModel):
    level: str = "INFO"
    file: str = "import.log"


class Config(BaseModel):
    database: DatabaseConfig
    import_: ImportConfig = Field(alias="import", default_factory=ImportConfig)
    osm: OSMConfig = Field(default_factory=OSMConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @classmethod
    def from_yaml(cls, config_path: str) -> "Config":
        """Charge la configuration depuis un fichier YAML."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)

            # Gérer l'alias 'import' -> 'import_'
            if 'import' in data:
                data['import_'] = data.pop('import')

            return cls(**data)
        except FileNotFoundError:
            print(f"Fichier de configuration introuvable: {config_path}")
            print("Création d'un fichier de configuration par défaut...")
            default_config = cls._create_default_config()
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(default_config, f, default_flow_style=False)
            return cls(**default_config)

    @staticmethod
    def _create_default_config() -> Dict[str, Any]:
        """Crée une configuration par défaut."""
        return {
            'database': {
                'host': 'localhost',
                'port': 5432,
                'name': 'geodata',
                'user': 'postgres',
                'password': 'your_password_here'
            },
            'import': {
                'batch_size': 1000,
                'num_workers': 4,
                'simplification_tolerance': 0.01
            },
            'osm': {
                'tags_countries': [
                    'boundary=administrative',
                    'admin_level=2'
                ],
                'tags_cities': [
                    'place=city',
                    'place=town',
                    'place=village',
                    'place=hamlet'
                ]
            },
            'logging': {
                'level': 'INFO',
                'file': 'import.log'
            }
        }
