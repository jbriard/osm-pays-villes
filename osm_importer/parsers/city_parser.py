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
        self.country_code_from_tags: Optional[str] = None


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

            # Région/État amélioré
            city.region_state = self._extract_region_state(n.tags)

            # Code pays depuis les tags OSM
            city.country_code_from_tags = self._extract_country_code(n.tags)

            if city.name_local:
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

        city.name_fr = tags.get('name:fr')
        city.name_en = tags.get('name:en')
        city.name_local = tags.get('name', tags.get('name:en', tags.get('name:fr')))

        city.display_name = (
            city.name_fr or
            city.name_en or
            city.name_local or
            f"Ville {node.id}"
        )

    def _extract_region_state(self, tags: Dict[str, str]) -> Optional[str]:
        """Extrait la région/état avec plusieurs stratégies."""
        region_tags = [
            'addr:state',
            'state',
            'addr:province',
            'province',
            'addr:region',
            'region',
            'is_in:state',
            'is_in:province',
            'is_in:region'
        ]

        for tag in region_tags:
            value = tags.get(tag)
            if value and value.strip():
                return value.strip()

        # Parser is_in
        is_in = tags.get('is_in')
        if is_in:
            parts = [part.strip() for part in is_in.split(',')]
            if len(parts) >= 2:
                return parts[-2]

        return None

    def _extract_country_code(self, tags: Dict[str, str]) -> Optional[str]:
        """Extrait le code pays depuis les tags OSM."""
        country_tags = [
            'addr:country',
            'country',
            'addr:country_code',
            'country_code',
            'ISO3166-1:alpha2',
            'is_in:country',
            'is_in:country_code'
        ]

        for tag in country_tags:
            value = tags.get(tag)
            if value and value.strip():
                country_code = value.strip().upper()
                if len(country_code) == 2 and country_code.isalpha():
                    return country_code

        # Essayer is_in
        is_in = tags.get('is_in')
        if is_in:
            parts = [part.strip() for part in is_in.split(',')]
            if parts:
                last_part = parts[-1].upper()
                if len(last_part) == 2 and last_part.isalpha():
                    return last_part

        return None