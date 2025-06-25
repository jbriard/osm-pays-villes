import osmium
from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)


class CityData:
    """Structure pour les données d'une ville."""

    def __init__(self):
        self.osm_id: Optional[int] = None
        self.name_fr: Optional[str] = None
        self.name_en: Optional[str] = None
        self.name_local: Optional[str] = None
        self.display_name: Optional[str] = None
        self.center_lat: Optional[float] = None
        self.center_lng: Optional[float] = None
        self.region_state: Optional[str] = None
        self.place_type: Optional[str] = None


class CityParser(osmium.SimpleHandler):
    """Parser pour extraire les villes depuis les données OSM."""

    def __init__(self, progress_tracker=None):
        osmium.SimpleHandler.__init__(self)
        self.cities: List[CityData] = []
        self.progress_tracker = progress_tracker
        self.processed_count = 0
        self.valid_place_types = {'city', 'town', 'village', 'hamlet'}

    def node(self, n):
        """Traite les nœuds OSM pour identifier les villes."""
        try:
            if not self._is_city(n):
                return

            city = CityData()
            city.osm_id = n.id

            # Extraire les noms
            self._extract_names(n, city)

            # Extraire le type de lieu
            city.place_type = n.tags.get('place')

            # Coordonnées
            city.center_lat = float(n.location.lat)
            city.center_lng = float(n.location.lon)

            # Région/État
            city.region_state = n.tags.get('addr:state') or n.tags.get('state')

            if city.name_local:  # Au minimum le nom local requis
                self.cities.append(city)
                self.processed_count += 1

                if self.progress_tracker:
                    self.progress_tracker.update("cities", advance=1)

        except Exception as e:
            logger.error(f"Erreur traitement ville {n.id}: {e}")

    def _is_city(self, node) -> bool:
        """Vérifie si le nœud représente une ville."""
        place = node.tags.get('place')
        return place in self.valid_place_types

    def _extract_names(self, node, city: CityData):
        """Extrait les noms dans différentes langues."""
        tags = node.tags

        # Priorité: name:fr → name:en → name
        city.name_fr = tags.get('name:fr')
        city.name_en = tags.get('name:en')
        city.name_local = tags.get('name', tags.get('name:en', tags.get('name:fr')))

        # Display name
        city.display_name = (
            city.name_fr or
            city.name_en or
            city.name_local or
            f"Ville {node.id}"
        )

