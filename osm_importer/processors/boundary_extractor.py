import osmium
from typing import Dict, Optional, List, Tuple, Set
import logging
from collections import defaultdict
import json
import time

logger = logging.getLogger(__name__)


class BoundaryExtractor:
    """Extracteur de frontières robuste avec debugging détaillé."""
    
    def __init__(self):
        self.nodes_cache = {}
        self.ways_cache = {}
        self.relations_cache = {}
        self.country_boundaries = {}
        
    def extract_boundaries_from_file(self, osm_file_path: str, country_osm_ids: List[int] = None):
        """Extrait les frontières pour des pays spécifiques."""
        logger.info(f"Extraction des frontières depuis {osm_file_path}")
        
        # Phase 1: Identifier les relations pays et leurs ways
        logger.info("Phase 1: Analyse des relations pays...")
        country_ways = self._find_country_ways(osm_file_path, country_osm_ids)
        
        if not country_ways:
            logger.warning("Aucune relation pays trouvée")
            return {}
        
        logger.info(f"Trouvé {len(country_ways)} pays avec {sum(len(ways) for ways in country_ways.values())} ways")
        
        # Phase 2: Extraire les ways nécessaires
        logger.info("Phase 2: Extraction des ways...")
        needed_ways = set()
        for ways in country_ways.values():
            needed_ways.update(ways)
        
        ways_data = self._extract_ways(osm_file_path, needed_ways)
        logger.info(f"Extrait {len(ways_data)} ways sur {len(needed_ways)} demandés")
        
        # Phase 3: Extraire les nodes nécessaires
        logger.info("Phase 3: Extraction des nodes...")
        needed_nodes = set()
        for way_nodes in ways_data.values():
            needed_nodes.update(way_nodes)
        
        nodes_data = self._extract_nodes(osm_file_path, needed_nodes)
        logger.info(f"Extrait {len(nodes_data)} nodes sur {len(needed_nodes)} demandés")
        
        # Phase 4: Construire les géométries
        logger.info("Phase 4: Construction des géométries...")
        boundaries = {}
        
        for country_id, ways in country_ways.items():
            try:
                boundary = self._build_boundary(country_id, ways, ways_data, nodes_data)
                if boundary:
                    boundaries[country_id] = boundary
                    logger.info(f"Frontière construite pour pays {country_id}")
                else:
                    logger.warning(f"Échec construction frontière pour pays {country_id}")
            except Exception as e:
                logger.error(f"Erreur construction frontière pays {country_id}: {e}")
        
        return boundaries
    
    def _find_country_ways(self, osm_file_path: str, target_countries: List[int] = None) -> Dict[int, List[int]]:
        """Trouve les ways pour chaque relation pays."""
        
        class RelationFinder(osmium.SimpleHandler):
            def __init__(self):
                osmium.SimpleHandler.__init__(self)
                self.country_ways = {}
                self.processed = 0
            
            def relation(self, r):
                self.processed += 1
                if self.processed % 10000 == 0:
                    logger.debug(f"Relations traitées: {self.processed}")
                
                # Vérifier si c'est un pays
                if (r.tags.get('boundary') == 'administrative' and 
                    r.tags.get('admin_level') == '2'):
                    
                    # Filtrer par pays cibles si spécifié
                    if target_countries and r.id not in target_countries:
                        return
                    
                    name = r.tags.get('name', f'Pays {r.id}')
                    logger.info(f"Trouvé pays: {name} (ID: {r.id})")
                    
                    # Extraire les ways membres
                    ways = []
                    for member in r.members:
                        if member.type == 'w':  # Way
                            ways.append(member.ref)
                    
                    if ways:
                        self.country_ways[r.id] = ways
                        logger.info(f"  -> {len(ways)} ways trouvés")
                    else:
                        logger.warning(f"  -> Aucun way trouvé pour {name}")
        
        handler = RelationFinder()
        handler.apply_file(osm_file_path)
        return handler.country_ways
    
    def _extract_ways(self, osm_file_path: str, needed_ways: Set[int]) -> Dict[int, List[int]]:
        """Extrait les ways spécifiés."""
        
        class WayExtractor(osmium.SimpleHandler):
            def __init__(self, target_ways):
                osmium.SimpleHandler.__init__(self)
                self.target_ways = target_ways
                self.ways_data = {}
                self.processed = 0
            
            def way(self, w):
                self.processed += 1
                if self.processed % 100000 == 0:
                    logger.debug(f"Ways traités: {self.processed}, trouvés: {len(self.ways_data)}")
                
                if w.id in self.target_ways:
                    nodes = [node.ref for node in w.nodes]
                    self.ways_data[w.id] = nodes
                    
                    if len(self.ways_data) % 1000 == 0:
                        logger.debug(f"Ways extraits: {len(self.ways_data)}/{len(self.target_ways)}")
        
        handler = WayExtractor(needed_ways)
        handler.apply_file(osm_file_path)
        return handler.ways_data
    
    def _extract_nodes(self, osm_file_path: str, needed_nodes: Set[int]) -> Dict[int, Tuple[float, float]]:
        """Extrait les nodes spécifiés."""
        
        class NodeExtractor(osmium.SimpleHandler):
            def __init__(self, target_nodes):
                osmium.SimpleHandler.__init__(self)
                self.target_nodes = target_nodes
                self.nodes_data = {}
                self.processed = 0
            
            def node(self, n):
                self.processed += 1
                if self.processed % 1000000 == 0:
                    logger.debug(f"Nodes traités: {self.processed}, trouvés: {len(self.nodes_data)}")
                
                if n.id in self.target_nodes:
                    self.nodes_data[n.id] = (n.location.lon, n.location.lat)
                    
                    if len(self.nodes_data) % 10000 == 0:
                        logger.debug(f"Nodes extraits: {len(self.nodes_data)}/{len(self.target_nodes)}")
        
        handler = NodeExtractor(needed_nodes)
        handler.apply_file(osm_file_path)
        return handler.nodes_data
    
    def _build_boundary(self, country_id: int, ways: List[int], 
                       ways_data: Dict[int, List[int]], 
                       nodes_data: Dict[int, Tuple[float, float]]) -> Optional[str]:
        """Construit la géométrie d'un pays."""
        
        logger.info(f"Construction frontière pays {country_id} avec {len(ways)} ways")
        
        # Convertir les ways en coordonnées
        way_coordinates = {}
        valid_ways = 0
        
        for way_id in ways:
            if way_id not in ways_data:
                logger.debug(f"Way {way_id} non trouvé dans les données")
                continue
                
            node_ids = ways_data[way_id]
            coords = []
            
            for node_id in node_ids:
                if node_id in nodes_data:
                    coords.append(nodes_data[node_id])
                else:
                    logger.debug(f"Node {node_id} non trouvé")
            
            if len(coords) >= 2:
                way_coordinates[way_id] = coords
                valid_ways += 1
            else:
                logger.debug(f"Way {way_id} invalide: {len(coords)} coordonnées")
        
        logger.info(f"Ways valides: {valid_ways}/{len(ways)}")
        
        if not way_coordinates:
            logger.warning(f"Aucun way valide pour pays {country_id}")
            return None
        
        # Essayer de construire un polygone simple
        try:
            # Méthode 1: Concaténer tous les ways (approximatif)
            all_coords = []
            for coords in way_coordinates.values():
                all_coords.extend(coords)
            
            if len(all_coords) < 3:
                logger.warning(f"Pas assez de coordonnées: {len(all_coords)}")
                return None
            
            # Supprimer les doublons consécutifs
            unique_coords = [all_coords[0]]
            for coord in all_coords[1:]:
                if coord != unique_coords[-1]:
                    unique_coords.append(coord)
            
            # Fermer le polygone
            if len(unique_coords) >= 3 and unique_coords[0] != unique_coords[-1]:
                unique_coords.append(unique_coords[0])
            
            if len(unique_coords) < 4:  # Besoin d'au moins 4 points pour un polygone fermé
                logger.warning(f"Polygone trop petit: {len(unique_coords)} points")
                return None
            
            # Créer le GeoJSON
            geojson = {
                "type": "Polygon",
                "coordinates": [[[coord[0], coord[1]] for coord in unique_coords]]
            }
            
            logger.info(f"Polygone créé avec {len(unique_coords)} points")
            return json.dumps(geojson)
            
        except Exception as e:
            logger.error(f"Erreur construction polygone: {e}")
            return None


# Version simplifiée du CountryParser
class CountryParserSimplified(osmium.SimpleHandler):
    """Parser simplifié qui utilise l'extracteur de frontières."""

    def __init__(self, progress_tracker=None):
        osmium.SimpleHandler.__init__(self)
        self.countries = []
        self.progress_tracker = progress_tracker
        self.processed_count = 0
        self.country_ids = []  # Pour stocker les IDs des pays trouvés

    def relation(self, r):
        """Collecte les métadonnées des pays."""
        try:
            if not self._is_country(r):
                return

            from osm_importer.parsers.country_parser import CountryData
            country = CountryData()
            country.osm_id = r.id

            # Extraire les métadonnées
            self._extract_names(r, country)
            self._extract_country_codes(r, country)

            # Frontière vide pour l'instant
            country.boundaries = json.dumps({"type": "Polygon", "coordinates": [[]]})

            if country.name_local:
                self.countries.append(country)
                self.country_ids.append(r.id)
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

    def _extract_names(self, relation, country):
        """Extrait les noms dans différentes langues."""
        tags = relation.tags
        country.name_fr = tags.get('name:fr')
        country.name_en = tags.get('name:en')
        country.name_local = tags.get('name', tags.get('name:en', tags.get('name:fr')))
        country.display_name = (
            country.name_fr or country.name_en or country.name_local or f"Pays {relation.id}"
        )

    def _extract_country_codes(self, relation, country):
        """Extrait les codes pays ISO."""
        tags = relation.tags
        country.country_code_alpha2 = tags.get('ISO3166-1:alpha2')
        country.country_code_alpha3 = tags.get('ISO3166-1:alpha3')

    def extract_boundaries_post_processing(self, osm_file_path: str):
        """Extrait les frontières après le parsing initial."""
        if not self.country_ids:
            logger.warning("Aucun pays trouvé pour extraction des frontières")
            return

        logger.info(f"Post-traitement: extraction des frontières pour {len(self.country_ids)} pays")
        
        # Utiliser l'extracteur de frontières
        extractor = BoundaryExtractor()
        boundaries = extractor.extract_boundaries_from_file(osm_file_path, self.country_ids)
        
        # Mettre à jour les frontières
        updated_count = 0
        for country in self.countries:
            if country.osm_id in boundaries:
                country.boundaries = boundaries[country.osm_id]
                updated_count += 1
                logger.info(f"Frontière mise à jour pour {country.name_local} ({country.osm_id})")
        
        logger.info(f"Frontières mises à jour: {updated_count}/{len(self.countries)}")


# Script de test pour debug
def test_boundary_extraction(osm_file_path: str, country_name: str = None):
    """Test l'extraction de frontières pour debug."""
    
    # Configurer le logging détaillé
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger.info(f"Test extraction frontières: {osm_file_path}")
    
    # Phase 1: Trouver un pays spécifique pour test
    class CountryFinder(osmium.SimpleHandler):
        def __init__(self, target_name):
            osmium.SimpleHandler.__init__(self)
            self.target_name = target_name.lower() if target_name else None
            self.found_countries = []
        
        def relation(self, r):
            if (r.tags.get('boundary') == 'administrative' and 
                r.tags.get('admin_level') == '2'):
                
                name = r.tags.get('name', '')
                if not self.target_name or self.target_name in name.lower():
                    self.found_countries.append({
                        'id': r.id,
                        'name': name,
                        'code': r.tags.get('ISO3166-1:alpha2', 'N/A')
                    })
                    logger.info(f"Pays trouvé: {name} (ID: {r.id}, Code: {r.tags.get('ISO3166-1:alpha2', 'N/A')})")
    
    # Chercher les pays
    finder = CountryFinder(country_name)
    finder.apply_file(osm_file_path)
    
    if not finder.found_countries:
        logger.error("Aucun pays trouvé")
        return
    
    # Tester avec le premier pays trouvé
    test_country = finder.found_countries[0]
    logger.info(f"Test avec: {test_country['name']} (ID: {test_country['id']})")
    
    # Extraire les frontières
    extractor = BoundaryExtractor()
    boundaries = extractor.extract_boundaries_from_file(osm_file_path, [test_country['id']])
    
    if test_country['id'] in boundaries:
        boundary_geojson = boundaries[test_country['id']]
        
        # Analyser le résultat
        import json
        geom = json.loads(boundary_geojson)
        coords = geom['coordinates'][0] if geom['coordinates'] else []
        
        logger.info(f"Succès ! Frontière extraite avec {len(coords)} points")
        logger.info(f"Premier point: {coords[0] if coords else 'N/A'}")
        logger.info(f"Dernier point: {coords[-1] if coords else 'N/A'}")
        
        # Sauvegarder pour inspection
        output_file = f"boundary_{test_country['code']}_{test_country['id']}.geojson"
        with open(output_file, 'w') as f:
            json.dump(geom, f, indent=2)
        logger.info(f"Frontière sauvegardée: {output_file}")
        
    else:
        logger.error("Échec extraction frontière")


if __name__ == "__main__":
    # Test avec un fichier OSM
    # Exemple: python boundary_extractor.py
    import sys
    
    if len(sys.argv) > 1:
        osm_file = sys.argv[1]
        country = sys.argv[2] if len(sys.argv) > 2 else None
        test_boundary_extraction(osm_file, country)
    else:
        print("Usage: python boundary_extractor.py <fichier.osm.pbf> [nom_pays]")