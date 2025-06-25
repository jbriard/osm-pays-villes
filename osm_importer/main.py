import asyncio
import click
from pathlib import Path
import logging
from typing import Optional
import time

from .config import Config
from .database.connection import DatabaseManager
from .database.operations import DatabaseOperations
from .parsers.country_parser import CountryParser
from .parsers.city_parser import CityParser
from .processors.data_enricher import DataEnricher
from .utils.logger import setup_logger
from .utils.progress import ProgressTracker


class OSMImporter:
    """Importateur principal pour les données OSM."""

    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_logger(
            level=config.logging.level,
            log_file=config.logging.file
        )
        self.db_manager = DatabaseManager(config)
        self.db_operations = DatabaseOperations(self.db_manager)
        self.data_enricher = DataEnricher()

    async def initialize(self):
        """Initialise l'importateur."""
        await self.db_manager.initialize()
        await self.db_manager.create_tables()
        self.logger.info("Importateur initialisé")

    async def import_file(self, osm_file: Path):
        """Importe un fichier OSM complet."""
        start_time = time.time()
        file_name = osm_file.name

        self.logger.info(f"Début import: {file_name}")

        with ProgressTracker() as progress:
            # Phase 1: Parse et extraction (en une seule passe)
            self.logger.info("Phase 1: Extraction des données OSM")
            countries_data, cities_data = await self._extract_osm_data_optimized(osm_file, progress)

            # Phase 2: Enrichissement
            self.logger.info("Phase 2: Enrichissement des données")
            enriched_countries = await self._enrich_countries(countries_data, progress)
            enriched_cities = await self._enrich_cities(cities_data, progress)

            # Phase 3: Import en base
            self.logger.info("Phase 3: Import en base de données")
            await self._import_to_database(enriched_countries, enriched_cities, progress)

            # Phase 4: Liaison des données
            self.logger.info("Phase 4: Liaison villes-pays")
            await self.db_operations.link_cities_to_countries()

            # Phase 5: Création des index
            self.logger.info("Phase 5: Création des index")
            await self.db_manager.create_indexes()

        elapsed = time.time() - start_time
        stats = await self.db_operations.get_import_stats()

        self.logger.info(f"Import terminé en {elapsed:.1f}s")
        self.logger.info(f"Statistiques: {stats['countries']} pays, {stats['cities']} villes")

    async def _extract_osm_data_optimized(self, osm_file: Path, progress):
        """Extrait les données depuis le fichier OSM en une seule passe."""
        import osmium

        self.logger.info("Extraction en une passe (sans pré-comptage)...")

        # Estimations basées sur la taille du fichier pour l'affichage
        file_size_mb = osm_file.stat().st_size / (1024 * 1024)
        estimated_countries = max(1, int(file_size_mb / 50))  # ~1 pays par 50MB
        estimated_cities = max(10, int(file_size_mb * 100))   # ~100 villes par MB

        # Initialiser les barres de progression avec estimations
        country_task = progress.add_task("countries", f"Pays ({osm_file.name})", estimated_countries)
        city_task = progress.add_task("cities", f"Villes ({osm_file.name})", estimated_cities)

        # Parseurs avec progression
        country_parser = CountryParser(progress)
        city_parser = CityParser(progress)

        # Parser combiné pour une seule passe
        class CombinedParser(osmium.SimpleHandler):
            def __init__(self, country_parser, city_parser, progress_tracker):
                osmium.SimpleHandler.__init__(self)
                self.country_parser = country_parser
                self.city_parser = city_parser
                self.progress = progress_tracker
                self.processed_elements = 0

            def relation(self, r):
                self.country_parser.relation(r)
                self._update_progress()

            def node(self, n):
                self.city_parser.node(n)
                self._update_progress()

            def _update_progress(self):
                self.processed_elements += 1
                # Mettre à jour les estimations tous les 10000 éléments
                if self.processed_elements % 10000 == 0:
                    countries_found = len(self.country_parser.countries)
                    cities_found = len(self.city_parser.cities)

                    # Ajuster les totaux si nécessaire
                    if countries_found > estimated_countries * 0.8:
                        self.progress.update_total("countries", int(countries_found * 1.2))
                    if cities_found > estimated_cities * 0.8:
                        self.progress.update_total("cities", int(cities_found * 1.2))

        # Exécuter le parsing combiné
        combined_parser = CombinedParser(country_parser, city_parser, progress)
        await self._run_parser(osm_file, combined_parser)

        # Finaliser les barres de progression avec les vraies valeurs
        actual_countries = len(country_parser.countries)
        actual_cities = len(city_parser.cities)

        progress.update_total("countries", actual_countries)
        progress.update_total("cities", actual_cities)
        progress.update("countries", completed=actual_countries)
        progress.update("cities", completed=actual_cities)

        self.logger.info(f"Extraction terminée: {actual_countries} pays, {actual_cities} villes")

        return country_parser.countries, city_parser.cities

    async def _run_parser(self, osm_file: Path, parser):
        """Exécute un parser osmium de manière asynchrone avec progression."""
        loop = asyncio.get_event_loop()

        def parse_with_progress():
            # Afficher la progression du fichier
            self.logger.info(f"Traitement de {osm_file.name} ({osm_file.stat().st_size / (1024*1024):.1f} MB)")
            parser.apply_file(str(osm_file))

        await loop.run_in_executor(None, parse_with_progress)

    async def _enrich_countries(self, countries_data, progress):
        """Enrichit les données des pays."""
        if not countries_data:
            return []

        progress.add_task("enrich_countries", "Enrichissement pays", len(countries_data))

        enriched = []
        for country in countries_data:
            try:
                # Conversion vers dictionnaire
                country_dict = {
                    'osm_id': country.osm_id,
                    'name_fr': country.name_fr,
                    'name_en': country.name_en,
                    'name_local': country.name_local,
                    'display_name': country.display_name,
                    'country_code_alpha2': country.country_code_alpha2,
                    'country_code_alpha3': country.country_code_alpha3,
                    'center_lat': country.center_lat,
                    'center_lng': country.center_lng,
                    'boundaries': country.boundaries
                }

                # Enrichissement
                enrichment = self.data_enricher.enrich_country(country)
                country_dict.update(enrichment)

                enriched.append(country_dict)
                progress.update("enrich_countries")

            except Exception as e:
                self.logger.error(f"Erreur enrichissement pays {country.osm_id}: {e}")
                continue

        return enriched

    async def _enrich_cities(self, cities_data, progress):
        """Enrichit les données des villes."""
        if not cities_data:
            return []

        progress.add_task("enrich_cities", "Enrichissement villes", len(cities_data))

        enriched = []
        for city in cities_data:
            try:
                # Conversion vers dictionnaire
                city_dict = {
                    'osm_id': city.osm_id,
                    'name_fr': city.name_fr,
                    'name_en': city.name_en,
                    'name_local': city.name_local,
                    'display_name': city.display_name,
                    'center_lat': city.center_lat,
                    'center_lng': city.center_lng,
                    'region_state': city.region_state,
                    'place_type': city.place_type
                }

                # Enrichissement
                enrichment = self.data_enricher.enrich_city(city)
                city_dict.update(enrichment)

                enriched.append(city_dict)
                progress.update("enrich_cities")

            except Exception as e:
                self.logger.error(f"Erreur enrichissement ville {city.osm_id}: {e}")
                continue

        return enriched

    async def _import_to_database(self, countries, cities, progress):
        """Importe les données enrichies en base."""
        batch_size = self.config.import_.batch_size

        # Import des pays par batch
        if countries:
            progress.add_task("import_countries", "Import pays", len(countries))
            for i in range(0, len(countries), batch_size):
                batch = countries[i:i + batch_size]
                stats = await self.db_operations.upsert_countries(batch)
                progress.update("import_countries", advance=len(batch))
                self.logger.debug(f"Batch pays: {stats}")

        # Import des villes par batch
        if cities:
            progress.add_task("import_cities", "Import villes", len(cities))
            for i in range(0, len(cities), batch_size):
                batch = cities[i:i + batch_size]
                stats = await self.db_operations.upsert_cities(batch)
                progress.update("import_cities", advance=len(batch))
                self.logger.debug(f"Batch villes: {stats}")

    async def close(self):
        """Ferme l'importateur."""
        await self.db_manager.close()


@click.command()
@click.option('--file', '-f', 'osm_file', required=True,
              type=click.Path(exists=True, path_type=Path),
              help='Fichier OSM à importer (.osm.pbf)')
@click.option('--config', '-c', 'config_file',
              type=click.Path(exists=True, path_type=Path),
              default='config.yaml',
              help='Fichier de configuration (défaut: config.yaml)')
@click.option('--workers', '-w', type=int,
              help='Nombre de workers (surcharge la config)')
def cli(osm_file: Path, config_file: Path, workers: Optional[int]):
    """Importateur de données géographiques OpenStreetMap vers PostgreSQL."""
    asyncio.run(main_async(osm_file, config_file, workers))


async def main_async(osm_file: Path, config_file: Path, workers: Optional[int]):
    """Fonction principale asynchrone."""
    try:
        # Chargement de la configuration
        config = Config.from_yaml(config_file)

        # Surcharge du nombre de workers si spécifié
        if workers:
            config.import_.num_workers = workers

        # Initialisation et import
        importer = OSMImporter(config)
        await importer.initialize()

        try:
            await importer.import_file(osm_file)
        finally:
            await importer.close()

    except Exception as e:
        click.echo(f"Erreur: {e}", err=True)
        raise click.Abort()


if __name__ == '__main__':
    cli()
