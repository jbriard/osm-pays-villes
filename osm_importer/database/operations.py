from sqlalchemy import select, update, and_, func, text
from sqlalchemy.dialects.postgresql import insert
from typing import List, Dict, Any, Optional
import logging
import json
from shapely.geometry import Point, Polygon, MultiPolygon
from shapely.ops import unary_union

logger = logging.getLogger(__name__)


class DatabaseOperations:
    """Opérations de base de données avec liaison géométrique précise."""

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.country_geometries = {}  # Cache des géométries

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
                        stmt = insert(City).values(**city_data)

                        update_dict = {k: v for k, v in city_data.items()
                                     if k not in ['id', 'osm_id', 'created_at']}
                        update_dict['updated_at'] = stmt.excluded.updated_at

                        stmt = stmt.on_conflict_do_update(
                            index_elements=['osm_id'],
                            set_=update_dict
                        )

                        result = await session.execute(stmt)

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

    async def load_country_geometries(self):
        """Charge les géométries des pays en mémoire pour les calculs."""
        from ..models import Country

        logger.info("Chargement des géométries des pays...")
        
        async with self.db_manager.get_session() as session:
            result = await session.execute(
                select(Country.id, Country.country_code_alpha2, Country.boundaries, Country.name_local)
                .where(Country.boundaries.isnot(None))
            )
            
            countries = result.fetchall()
            
            for country_id, code, boundaries_json, name in countries:
                try:
                    if boundaries_json:
                        geom_data = json.loads(boundaries_json)
                        
                        # Créer la géométrie Shapely
                        if geom_data.get('type') == 'Polygon':
                            coords = geom_data.get('coordinates', [])
                            if coords and coords[0]:  # Vérifier que ce n'est pas vide
                                geom = Polygon(coords[0])
                                self.country_geometries[country_id] = {
                                    'geometry': geom,
                                    'code': code,
                                    'name': name
                                }
                        
                except Exception as e:
                    logger.warning(f"Erreur géométrie pays {name} ({code}): {e}")
                    continue
            
            logger.info(f"Géométries chargées pour {len(self.country_geometries)} pays")

    def find_country_for_point(self, lat: float, lng: float) -> Optional[int]:
        """Trouve le pays contenant un point donné."""
        if not self.country_geometries:
            return None
            
        point = Point(lng, lat)  # Shapely utilise (lng, lat)
        
        # Recherche dans toutes les géométries
        for country_id, country_data in self.country_geometries.items():
            try:
                if country_data['geometry'].contains(point):
                    return country_id
            except Exception as e:
                logger.debug(f"Erreur test point dans {country_data['name']}: {e}")
                continue
                
        return None

    async def link_cities_to_countries(self):
        """Liaison précise villes-pays avec calculs géométriques."""
        from ..models import City, Country

        # 1. Charger les géométries
        await self.load_country_geometries()
        
        if not self.country_geometries:
            logger.warning("Aucune géométrie de pays disponible, utilisation de la méthode approximative")
            await self.link_cities_to_countries_fallback()
            return

        # 2. Liaison directe par code pays dans les tags
        await self._link_by_country_tags()

        # 3. Liaison géométrique précise
        await self._link_by_geometry()

        # 4. Liaison approximative pour les cas non résolus
        await self._link_by_proximity()

        # 5. Statistiques
        stats = await self.get_linking_stats()
        logger.info(f"Liaison précise terminée - {stats['linked_cities']}/{stats['total_cities']} "
                   f"villes liées ({stats['link_percentage']:.1f}%)")

    async def _link_by_country_tags(self):
        """Liaison directe par codes pays dans les tags OSM."""
        async with self.db_manager.get_session() as session:
            stmt = text("""
            UPDATE cities 
            SET country_id = (
                SELECT c.id 
                FROM countries c 
                WHERE c.country_code_alpha2 = cities.country_code_from_tags
                LIMIT 1
            )
            WHERE cities.country_id IS NULL 
            AND cities.country_code_from_tags IS NOT NULL
            """)
            
            result = await session.execute(stmt)
            await session.commit()
            logger.info(f"Liaison par tags: {result.rowcount} villes")

    async def _link_by_geometry(self):
        """Liaison par calculs géométriques précis."""
        from ..models import City

        async with self.db_manager.get_session() as session:
            # Récupérer les villes non liées par petits lots
            batch_size = 1000
            offset = 0
            total_linked = 0

            while True:
                result = await session.execute(
                    select(City.id, City.center_lat, City.center_lng)
                    .where(and_(
                        City.country_id.is_(None),
                        City.center_lat.isnot(None),
                        City.center_lng.isnot(None)
                    ))
                    .limit(batch_size)
                    .offset(offset)
                )
                
                cities = result.fetchall()
                if not cities:
                    break

                # Traiter le lot
                updates = []
                for city_id, lat, lng in cities:
                    try:
                        country_id = self.find_country_for_point(float(lat), float(lng))
                        if country_id:
                            updates.append({'city_id': city_id, 'country_id': country_id})
                    except Exception as e:
                        logger.debug(f"Erreur géométrie ville {city_id}: {e}")
                        continue

                # Appliquer les mises à jour
                if updates:
                    for update_data in updates:
                        await session.execute(
                            update(City)
                            .where(City.id == update_data['city_id'])
                            .values(country_id=update_data['country_id'])
                        )
                    
                    await session.commit()
                    total_linked += len(updates)
                    logger.debug(f"Lot géométrique: {len(updates)}/{len(cities)} villes liées")

                offset += batch_size

            logger.info(f"Liaison géométrique: {total_linked} villes")

    async def _link_by_proximity(self):
        """Liaison par proximité pour les cas non résolus."""
        async with self.db_manager.get_session() as session:
            stmt = text("""
            UPDATE cities
            SET country_id = (
                SELECT c.id
                FROM countries c
                WHERE c.center_lat IS NOT NULL 
                AND c.center_lng IS NOT NULL
                ORDER BY 
                    SQRT(POWER(cities.center_lat - c.center_lat, 2) + 
                         POWER(cities.center_lng - c.center_lng, 2))
                LIMIT 1
            )
            WHERE cities.country_id IS NULL
            AND cities.center_lat IS NOT NULL
            AND cities.center_lng IS NOT NULL
            """)
            
            result = await session.execute(stmt)
            await session.commit()
            logger.info(f"Liaison par proximité: {result.rowcount} villes")

    async def link_cities_to_countries_fallback(self):
        """Méthode de fallback si pas de géométries disponibles."""
        logger.info("Utilisation de la méthode de liaison approximative")
        
        # Liaison par tags
        await self._link_by_country_tags()
        
        # Liaison approximative par régions (version simplifiée)
        await self._link_by_simple_regions()
        
        # Liaison par proximité
        await self._link_by_proximity()

    async def _link_by_simple_regions(self):
        """Liaison approximative par grandes régions géographiques."""
        async with self.db_manager.get_session() as session:
            
            # Quelques règles simples et sûres
            simple_rules = [
                # Antarctique
                ("'AQ'", "cities.center_lat < -60"),
                
                # Groenland
                ("'GL'", "cities.center_lat > 70 AND cities.center_lng BETWEEN -50 AND -10"),
                
                # Islande
                ("'IS'", "cities.center_lat BETWEEN 63 AND 67 AND cities.center_lng BETWEEN -25 AND -13"),
                
                # Australie (continent principal)
                ("'AU'", "cities.center_lat BETWEEN -45 AND -10 AND cities.center_lng BETWEEN 110 AND 155"),
                
                # Nouvelle-Zélande
                ("'NZ'", "cities.center_lat BETWEEN -48 AND -34 AND cities.center_lng BETWEEN 165 AND 180"),
            ]

            total_linked = 0
            for country_code, condition in simple_rules:
                stmt = text(f"""
                UPDATE cities
                SET country_id = (
                    SELECT c.id
                    FROM countries c
                    WHERE c.country_code_alpha2 = {country_code}
                    LIMIT 1
                )
                WHERE cities.country_id IS NULL
                AND {condition}
                """)
                
                result = await session.execute(stmt)
                await session.commit()
                total_linked += result.rowcount
                
            logger.info(f"Liaison par régions simples: {total_linked} villes")

    async def get_linking_stats(self) -> Dict[str, Any]:
        """Retourne les statistiques de liaison."""
        from ..models import City

        async with self.db_manager.get_session() as session:
            total_result = await session.execute(select(func.count(City.id)))
            total_cities = total_result.scalar() or 0

            linked_result = await session.execute(
                select(func.count(City.id)).where(City.country_id.isnot(None))
            )
            linked_cities = linked_result.scalar() or 0

            unlinked_cities = total_cities - linked_cities
            link_percentage = (linked_cities / total_cities * 100) if total_cities > 0 else 0

            return {
                'total_cities': total_cities,
                'linked_cities': linked_cities,
                'unlinked_cities': unlinked_cities,
                'link_percentage': link_percentage
            }

    async def get_import_stats(self) -> Dict[str, int]:
        """Retourne les statistiques d'import."""
        from ..models import City, Country

        async with self.db_manager.get_session() as session:
            country_result = await session.execute(select(func.count(Country.id)))
            country_count = country_result.scalar()

            city_result = await session.execute(select(func.count(City.id)))
            city_count = city_result.scalar()

            linked_result = await session.execute(
                select(func.count(City.id)).where(City.country_id.isnot(None))
            )
            linked_cities = linked_result.scalar()

            return {
                'countries': country_count or 0,
                'cities': city_count or 0,
                'linked_cities': linked_cities or 0
            }