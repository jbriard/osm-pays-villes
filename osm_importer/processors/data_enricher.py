from timezonefinder import TimezoneFinder
import pycountry
from typing import Optional, List, Dict
import logging

logger = logging.getLogger(__name__)


class DataEnricher:
    """Enrichit les données géographiques avec des informations supplémentaires."""

    def __init__(self):
        self.timezone_finder = TimezoneFinder()
        self.continent_map = self._build_continent_map()

    def enrich_country(self, country_data) -> Dict:
        """Enrichit les données d'un pays."""
        enriched = {}

        try:
            # Timezone (approximative depuis le centre)
            if country_data.center_lat and country_data.center_lng:
                timezone = self.timezone_finder.timezone_at(
                    lat=float(country_data.center_lat),
                    lng=float(country_data.center_lng)
                )
                enriched['timezones'] = [timezone] if timezone else []

            # Continent et région
            if country_data.country_code_alpha2:
                continent_info = self.continent_map.get(country_data.country_code_alpha2.upper())
                if continent_info:
                    enriched['continent'] = continent_info['continent']
                    enriched['region'] = continent_info['region']

            # Informations depuis pycountry
            if country_data.country_code_alpha2:
                country_info = pycountry.countries.get(alpha_2=country_data.country_code_alpha2.upper())
                if country_info:
                    # Code alpha3 si manquant
                    if not country_data.country_code_alpha3:
                        enriched['country_code_alpha3'] = country_info.alpha_3

                    # Nom anglais si manquant
                    if not country_data.name_en:
                        enriched['name_en'] = country_info.name

            # Monnaie (mapping basique)
            enriched['currency_code'] = self._get_currency_code(country_data.country_code_alpha2)

            # Langues officielles (mapping basique)
            enriched['official_languages'] = self._get_official_languages(country_data.country_code_alpha2)

        except Exception as e:
            logger.error(f"Erreur enrichissement pays {country_data.osm_id}: {e}")

        return enriched

    def enrich_city(self, city_data) -> Dict:
        """Enrichit les données d'une ville."""
        enriched = {}

        try:
            # Timezone
            if city_data.center_lat and city_data.center_lng:
                timezone = self.timezone_finder.timezone_at(
                    lat=float(city_data.center_lat),
                    lng=float(city_data.center_lng)
                )
                enriched['timezone'] = timezone

        except Exception as e:
            logger.error(f"Erreur enrichissement ville {city_data.osm_id}: {e}")

        return enriched

    def _build_continent_map(self) -> Dict[str, Dict[str, str]]:
        """Construit la carte des continents et régions."""
        # Mapping simplifié - en production, utiliser une source plus complète
        return {
            'FR': {'continent': 'Europe', 'region': 'Western Europe'},
            'DE': {'continent': 'Europe', 'region': 'Western Europe'},
            'IT': {'continent': 'Europe', 'region': 'Southern Europe'},
            'ES': {'continent': 'Europe', 'region': 'Southern Europe'},
            'GB': {'continent': 'Europe', 'region': 'Northern Europe'},
            'US': {'continent': 'North America', 'region': 'Northern America'},
            'CA': {'continent': 'North America', 'region': 'Northern America'},
            'BR': {'continent': 'South America', 'region': 'South America'},
            'AR': {'continent': 'South America', 'region': 'South America'},
            'CN': {'continent': 'Asia', 'region': 'Eastern Asia'},
            'JP': {'continent': 'Asia', 'region': 'Eastern Asia'},
            'IN': {'continent': 'Asia', 'region': 'Southern Asia'},
            'AU': {'continent': 'Oceania', 'region': 'Australia and New Zealand'},
            'ZA': {'continent': 'Africa', 'region': 'Southern Africa'},
            'EG': {'continent': 'Africa', 'region': 'Northern Africa'},
            'NG': {'continent': 'Africa', 'region': 'Western Africa'},
            # Ajouter d'autres pays selon les besoins
        }

    def _get_currency_code(self, country_code: Optional[str]) -> Optional[str]:
        """Retourne le code monnaie pour un pays."""
        if not country_code:
            return None

        # Mapping simplifié
        currency_map = {
            'FR': 'EUR', 'DE': 'EUR', 'IT': 'EUR', 'ES': 'EUR',
            'US': 'USD', 'CA': 'CAD', 'GB': 'GBP', 'JP': 'JPY',
            'CN': 'CNY', 'IN': 'INR', 'BR': 'BRL', 'AU': 'AUD',
            'ZA': 'ZAR', 'CH': 'CHF', 'SE': 'SEK', 'NO': 'NOK'
        }
        return currency_map.get(country_code.upper())

    def _get_official_languages(self, country_code: Optional[str]) -> List[str]:
        """Retourne les langues officielles d'un pays."""
        if not country_code:
            return []

        # Mapping simplifié
        language_map = {
            'FR': ['fr'], 'DE': ['de'], 'IT': ['it'], 'ES': ['es'],
            'GB': ['en'], 'US': ['en'], 'CA': ['en', 'fr'],
            'JP': ['ja'], 'CN': ['zh'], 'IN': ['hi', 'en'],
            'BR': ['pt'], 'AR': ['es'], 'AU': ['en'],
            'ZA': ['af', 'en'], 'CH': ['de', 'fr', 'it']
        }
        return language_map.get(country_code.upper(), [])
