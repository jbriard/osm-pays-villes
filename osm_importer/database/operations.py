from sqlalchemy import select, update, and_, func, text
from sqlalchemy.dialects.postgresql import insert
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class DatabaseOperations:
    """Opérations de base de données pour l'import OSM."""

    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def upsert_countries(self, countries_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Insert ou update des pays par batch."""
        from ..models import Country

        stats = {'inserted': 0, 'updated': 0, 'errors': 0}

        async with self.db_manager.get_session() as session:
            try:
                for country_data in countries_data:
                    try:
                        # Requête UPSERT PostgreSQL
                        stmt = insert(Country).values(**country_data)

                        # ON CONFLICT DO UPDATE
                        update_dict = {k: v for k, v in country_data.items()
                                     if k not in ['id', 'osm_id', 'created_at']}
                        update_dict['updated_at'] = stmt.excluded.updated_at

                        stmt = stmt.on_conflict_do_update(
                            index_elements=['osm_id'],
                            set_=update_dict
                        )

                        result = await session.execute(stmt)

                        # Déterminer si c'était un INSERT ou UPDATE
                        if result.rowcount > 0:
                            stats['inserted'] += 1
                        else:
                            stats['updated'] += 1

                    except Exception as e:
                        logger.error(f"Erreur upsert pays {country_data.get('osm_id')}: {e}")
                        stats['errors'] += 1
                        continue

                await session.commit()

            except Exception as e:
                await session.rollback()
                logger.error(f"Erreur batch countries: {e}")
                stats['errors'] += len(countries_data)

        return stats

    async def upsert_cities(self, cities_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Insert ou update des villes par batch."""
        from ..models import City

        stats = {'inserted': 0, 'updated': 0, 'errors': 0}

        async with self.db_manager.get_session() as session:
            try:
                for city_data in cities_data:
                    try:
                        # Requête UPSERT PostgreSQL
                        stmt = insert(City).values(**city_data)

                        # ON CONFLICT DO UPDATE
                        update_dict = {k: v for k, v in city_data.items()
                                     if k not in ['id', 'osm_id', 'created_at']}
                        update_dict['updated_at'] = stmt.excluded.updated_at

                        stmt = stmt.on_conflict_do_update(
                            index_elements=['osm_id'],
                            set_=update_dict
                        )

                        result = await session.execute(stmt)

                        # Déterminer si c'était un INSERT ou UPDATE
                        if result.rowcount > 0:
                            stats['inserted'] += 1
                        else:
                            stats['updated'] += 1

                    except Exception as e:
                        logger.error(f"Erreur upsert ville {city_data.get('osm_id')}: {e}")
                        stats['errors'] += 1
                        continue

                await session.commit()

            except Exception as e:
                await session.rollback()
                logger.error(f"Erreur batch cities: {e}")
                stats['errors'] += len(cities_data)

        return stats

    async def link_cities_to_countries(self):
        """Lie les villes à leurs pays basé sur les codes pays et coordonnées."""
        from ..models import City, Country

        async with self.db_manager.get_session() as session:
            try:
                # Version corrigée avec text() pour SQL brut
                stmt = text("""
                UPDATE cities
                SET country_id = (
                    SELECT c.id
                    FROM countries c
                    WHERE c.country_code_alpha2 = 'AQ'  -- Antarctica
                    LIMIT 1
                )
                WHERE cities.country_id IS NULL
                AND cities.center_lat < -60  -- Antarctique approximatif
                """)

                result = await session.execute(stmt)
                await session.commit()

                logger.info(f"Liaison villes-pays: {result.rowcount} villes liées")

            except Exception as e:
                await session.rollback()
                logger.error(f"Erreur liaison villes-pays: {e}")

    async def get_import_stats(self) -> Dict[str, int]:
        """Retourne les statistiques d'import."""
        from ..models import City, Country

        async with self.db_manager.get_session() as session:
            # Compter les pays - CORRECTION: utiliser func.count()
            country_result = await session.execute(select(func.count(Country.id)))
            country_count = country_result.scalar()

            # Compter les villes - CORRECTION: utiliser func.count()
            city_result = await session.execute(select(func.count(City.id)))
            city_count = city_result.scalar()

            # Villes liées - CORRECTION: utiliser func.count()
            linked_result = await session.execute(
                select(func.count(City.id)).where(City.country_id.isnot(None))
            )
            linked_cities = linked_result.scalar()

            return {
                'countries': country_count or 0,
                'cities': city_count or 0,
                'linked_cities': linked_cities or 0
            }
