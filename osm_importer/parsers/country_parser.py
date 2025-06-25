import osmium
from typing import Dict, Optional, List, Tuple
import logging
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union
import json

logger = logging.getLogger(__name__)


class CountryData:
    """Structure pour les données d'un pays."""

    def __init__(self):
        self.osm_id: Optional[int] = None
        self.name_fr: Optional[str] = None
        self.name_en: Optional[str] = None
        self.name_local: Optional[str] = None
        self.display_name: Optional[str] = None
        self.country_code_alpha2: Optional[str] = None
        self.country_code_alpha3: Optional[str] = None
        self.center_lat: Optional[float] = None
        self.center_lng: Optional[float] = None
        self.boundaries: Optional[str] = None  # GeoJSON


class CountryParser(osmium.SimpleHandler):
    """Parser pour extraire les pays depuis les données OSM."""

    def __init__(self, progress_tracker=None):
        osmium.SimpleHandler.__init__(self)
        self.countries: List[CountryData] = []
        self.progress_tracker = progress_tracker
        self.processed_count = 0

    def relation(self, r):
        """Traite les relations OSM pour identifier les pays."""
        try:
            # Vérifier si c'est un pays (boundary=administrative, admin_level=2)
            if not self._is_country(r):
                return

            country = CountryData()
            country.osm_id = r.id

            # Extraire les noms
            self._extract_names(r, country)

            # Extraire les codes pays
            self._extract_country_codes(r, country)

            # Calculer le centre (approximatif depuis les tags si disponible)
            self._extract_center(r, country)

            # Extraire les frontières (simplifié pour cette version)
            self._extract_boundaries(r, country)

            if country.name_local:  # Au minimum le nom local requis
                self.countries.append(country)
                self.processed_count += 1

                if self.progress_tracker:
                    self.progress_tracker.update("countries", advance=1)

        except Exception as e:
            logger.error(f"Erreur traitement pays {r.id}: {e}")

    def _is_country(self, relation) -> bool:
        """Vérifie si la relation représente un pays."""
        tags = relation.tags
        return (
            tags.get('boundary') == 'administrative' and
            tags.get('admin_level') == '2'
        )

    def _extract_names(self, relation, country: CountryData):
        """Extrait les noms dans différentes langues."""
        tags = relation.tags

        # Priorité: name:fr → name:en → name
        country.name_fr = tags.get('name:fr')
        country.name_en = tags.get('name:en')
        country.name_local = tags.get('name', tags.get('name:en', tags.get('name:fr')))

        # Display name (le plus approprié)
        country.display_name = (
            country.name_fr or
            country.name_en or
            country.name_local or
            f"Pays {relation.id}"
        )

    def _extract_country_codes(self, relation, country: CountryData):
        """Extrait les codes pays ISO."""
        tags = relation.tags
        country.country_code_alpha2 = tags.get('ISO3166-1:alpha2')
        country.country_code_alpha3 = tags.get('ISO3166-1:alpha3')

    def _extract_center(self, relation, country: CountryData):
        """Extrait les coordonnées du centre."""
        tags = relation.tags

        # Essayer d'obtenir depuis les tags admin_centre_coordinates
        if 'admin_centre' in tags:
            # Logique simplifiée - en réalité il faudrait résoudre les références
            pass

        # Pour l'instant, on laisse None - sera calculé plus tard depuis les boundaries

    def _extract_boundaries(self, relation, country: CountryData):
        """Extrait et simplifie les frontières."""
        # Version simplifiée - les vraies frontières nécessitent de résoudre
        # toutes les ways de la relation, ce qui est complexe
        # Pour l'instant, on génère un placeholder
        country.boundaries = json.dumps({
            "type": "Polygon",
            "coordinates": [[]]  # Vide pour l'instant
        })
