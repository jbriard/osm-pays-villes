#!/usr/bin/env python3
"""Version ultra-rapide de l'importateur OSM pour de gros fichiers."""

import sys
import os
import asyncio
import osmium
import time
from pathlib import Path
from rich.progress import Progress, BarColumn, TextColumn, SpinnerColumn
from rich.console import Console

# Ajouter le répertoire courant au PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from osm_importer.config import Config
from osm_importer.database.connection import DatabaseManager
from osm_importer.database.operations import DatabaseOperations


class FastOSMParser(osmium.SimpleHandler):
    """Parser OSM rapide avec progression en temps réel."""

    def __init__(self, console):
        osmium.SimpleHandler.__init__(self)
        self.console = console
        self.countries = []
        self.cities = []
        self.processed = 0
        self.start_time = time.time()
        self.last_update = 0

    def relation(self, r):
        """Traite les relations (pays)."""
        if (r.tags.get('boundary') == 'administrative' and
            r.tags.get('admin_level') == '2'):

            country_data = {
                'osm_id': r.id,
                'name_local': r.tags.get('name', f'Pays {r.id}'),
                'name_fr': r.tags.get('name:fr'),
                'name_en': r.tags.get('name:en'),
                'display_name': r.tags.get('name', f'Pays {r.id}'),
                'country_code_alpha2': r.tags.get('ISO3166-1:alpha2'),
                'country_code_alpha3': r.tags.get('ISO3166-1:alpha3'),
                'boundaries': '{"type":"Polygon","coordinates":[[]]}'
            }
            self.countries.append(country_data)

        self._update_progress()

    def node(self, n):
        """Traite les nœuds (villes)."""
        place_type = n.tags.get('place')
        if place_type in {'city', 'town', 'village', 'hamlet'}:

            city_data = {
                'osm_id': n.id,
                'name_local': n.tags.get('name', f'Ville {n.id}'),
                'name_fr': n.tags.get('name:fr'),
                'name_en': n.tags.get('name:en'),
                'display_name': n.tags.get('name', f'Ville {n.id}'),
                'center_lat': float(n.location.lat),
                'center_lng': float(n.location.lon),
                'place_type': place_type
            }
            self.cities.append(city_data)

        self._update_progress()

    def _update_progress(self):
        """Met à jour l'affichage de progression."""
        self.processed += 1

        # Afficher toutes les 100000 éléments
        if self.processed % 100000 == 0:
            elapsed = time.time() - self.start_time
            rate = self.processed / elapsed if elapsed > 0 else 0

            self.console.print(
                f"[green]Traités: {self.processed:,} éléments | "
                f"Pays: {len(self.countries)} | "
                f"Villes: {len(self.cities):,} | "
                f"Vitesse: {rate:.0f} éléments/s[/green]"
            )


async def fast_import(osm_file: Path, config_file: Path):
    """Import rapide sans barres de progression complexes."""
    console = Console()

    console.print(f"[bold blue]🚀 Import rapide: {osm_file.name}[/bold blue]")
    console.print(f"[dim]Taille: {osm_file.stat().st_size / (1024*1024):.1f} MB[/dim]")

    # Configuration
    config = Config.from_yaml(config_file)

    # Base de données
    db_manager = DatabaseManager(config)
    await db_manager.initialize()
    await db_manager.create_tables()

    db_operations = DatabaseOperations(db_manager)

    start_time = time.time()

    # Phase 1: Parsing
    console.print("\n[yellow]📖 Phase 1: Lecture du fichier OSM...[/yellow]")
    parser = FastOSMParser(console)

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]Parsing en cours..."),
        TextColumn("[bold green]{task.completed:,} éléments traités"),
        console=console
    ) as progress:

        task = progress.add_task("parsing", total=None)

        def parse_file():
            parser.apply_file(str(osm_file))

        await asyncio.get_event_loop().run_in_executor(None, parse_file)

    parse_time = time.time() - start_time
    console.print(f"[green]✅ Parsing terminé en {parse_time:.1f}s[/green]")
    console.print(f"[green]   Pays trouvés: {len(parser.countries)}[/green]")
    console.print(f"[green]   Villes trouvées: {len(parser.cities):,}[/green]")

    # Phase 2: Import base de données
    console.print("\n[yellow]💾 Phase 2: Import en base de données...[/yellow]")

    batch_size = config.import_.batch_size

    # Import pays
    if parser.countries:
        with Progress(console=console) as progress:
            task = progress.add_task("Import pays", total=len(parser.countries))

            for i in range(0, len(parser.countries), batch_size):
                batch = parser.countries[i:i + batch_size]
                await db_operations.upsert_countries(batch)
                progress.update(task, advance=len(batch))

    # Import villes
    if parser.cities:
        with Progress(console=console) as progress:
            task = progress.add_task("Import villes", total=len(parser.cities))

            for i in range(0, len(parser.cities), batch_size):
                batch = parser.cities[i:i + batch_size]
                await db_operations.upsert_cities(batch)
                progress.update(task, advance=len(batch))

    # Phase 3: Finalisation
    console.print("\n[yellow]🔗 Phase 3: Finalisation...[/yellow]")
    await db_operations.link_cities_to_countries()
    await db_manager.create_indexes()

    # Statistiques finales
    total_time = time.time() - start_time
    stats = await db_operations.get_import_stats()

    console.print(f"\n[bold green]🎉 Import terminé en {total_time:.1f}s ![/bold green]")
    console.print(f"[green]   📊 Pays: {stats['countries']}[/green]")
    console.print(f"[green]   🏙️  Villes: {stats['cities']:,}[/green]")
    console.print(f"[green]   🔗 Villes liées: {stats['linked_cities']:,}[/green]")
    console.print(f"[green]   ⚡ Vitesse moyenne: {parser.processed / total_time:.0f} éléments/s[/green]")

    await db_manager.close()


if __name__ == '__main__':
    import click

    @click.command()
    @click.argument('osm_file', type=click.Path(exists=True, path_type=Path))
    @click.option('--config', '-c', default='config.yaml',
                  type=click.Path(exists=True, path_type=Path))
    def main(osm_file, config):
        """Import rapide de fichier OSM."""
        asyncio.run(fast_import(osm_file, config))

    main()
