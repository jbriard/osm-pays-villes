#!/usr/bin/env python3
"""Script de debug pour l'extraction des frontières OSM."""

import sys
import os
import asyncio
import osmium
import json
from pathlib import Path
import logging

# Configuration du logging détaillé
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BoundaryDebugger(osmium.SimpleHandler):
    """Debugger spécialisé pour analyser les frontières OSM."""

    def __init__(self, target_country_name=None):
        osmium.SimpleHandler.__init__(self)
        self.target_country_name = target_country_name.lower() if target_country_name else None
        self.countries_found = []
        self.nodes_cache = {}
        self.ways_cache = {}
        self.current_country = None
        self.debug_mode = True

    def node(self, n):
        """Cache tous les nodes."""
        self.nodes_cache[n.id] = (n.location.lon, n.location.lat)

    def way(self, w):
        """Cache tous les ways."""
        node_refs = [node.ref for node in w.nodes]
        self.ways_cache[w.id] = node_refs

    def relation(self, r):
        """Analyse les relations pays."""
        if (r.tags.get('boundary') == 'administrative' and
            r.tags.get('admin_level') == '2'):

            name = r.tags.get('name', f'Country {r.id}')
            code = r.tags.get('ISO3166-1:alpha2', 'N/A')

            # Filtrer par nom si spécifié
            if self.target_country_name and self.target_country_name not in name.lower():
                return

            logger.info(f"\n=== PAYS TROUVÉ ===")
            logger.info(f"Nom: {name}")
            logger.info(f"ID OSM: {r.id}")
            logger.info(f"Code ISO: {code}")

            # Analyser les membres
            ways_outer = []
            ways_inner = []
            relations = []

            for member in r.members:
                if member.type == 'w':  # Way
                    if member.role == 'inner':
                        ways_inner.append(member.ref)
                    else:  # outer ou pas de rôle
                        ways_outer.append(member.ref)
                elif member.type == 'r':  # Relation
                    relations.append(member.ref)

            logger.info(f"Ways outer: {len(ways_outer)}")
            logger.info(f"Ways inner: {len(ways_inner)}")
            logger.info(f"Relations: {len(relations)}")

            if ways_outer:
                logger.info(f"Premiers ways outer: {ways_outer[:5]}")

            # Essayer de construire la géométrie
            boundary = self._build_boundary_debug(ways_outer, ways_inner, name)

            self.countries_found.append({
                'id': r.id,
                'name': name,
                'code': code,
                'boundary': boundary,
                'ways_outer': len(ways_outer),
                'ways_inner': len(ways_inner)
            })

    def _build_boundary_debug(self, ways_outer, ways_inner, country_name):
        """Construction de frontière avec debug détaillé."""
        logger.info(f"\n--- Construction frontière pour {country_name} ---")

        if not ways_outer:
            logger.warning("Aucun way outer trouvé")
            return None

        # Vérifier la disponibilité des ways
        available_ways = 0
        total_coords = 0

        way_coordinates = {}

        for way_id in ways_outer[:10]:  # Limiter pour le debug
            if way_id in self.ways_cache:
                node_refs = self.ways_cache[way_id]
                coords = []

                for node_ref in node_refs:
                    if node_ref in self.nodes_cache:
                        coords.append(self.nodes_cache[node_ref])

                if coords:
                    way_coordinates[way_id] = coords
                    available_ways += 1
                    total_coords += len(coords)
                    logger.info(f"Way {way_id}: {len(coords)} coordonnées")

                    # Afficher quelques coordonnées pour debug
                    if len(coords) >= 2:
                        logger.info(f"  Premier point: {coords[0]}")
                        logger.info(f"  Dernier point: {coords[-1]}")
                else:
                    logger.warning(f"Way {way_id}: aucune coordonnée trouvée")
            else:
                logger.warning(f"Way {way_id}: non trouvé dans le cache")

        logger.info(f"Ways disponibles: {available_ways}/{len(ways_outer[:10])}")
        logger.info(f"Total coordonnées: {total_coords}")

        if not way_coordinates:
            logger.error("Aucun way utilisable trouvé")
            return None

        # Construire un polygone simple (concaténation)
        try:
            all_coords = []
            for way_id, coords in way_coordinates.items():
                all_coords.extend(coords)
                logger.info(f"Ajouté way {way_id}: {len(coords)} points")

            logger.info(f"Total points concaténés: {len(all_coords)}")

            if len(all_coords) < 3:
                logger.error("Pas assez de points pour un polygone")
                return None

            # Supprimer les doublons consécutifs
            unique_coords = [all_coords[0]]
            for coord in all_coords[1:]:
                if coord != unique_coords[-1]:
                    unique_coords.append(coord)

            logger.info(f"Points uniques: {len(unique_coords)}")

            # Fermer le polygone
            if unique_coords[0] != unique_coords[-1]:
                unique_coords.append(unique_coords[0])

            # Vérifier la validité
            if len(unique_coords) < 4:
                logger.error(f"Polygone invalide: {len(unique_coords)} points")
                return None

            # Créer le GeoJSON
            geojson = {
                "type": "Polygon",
                "coordinates": [[[lon, lat] for lon, lat in unique_coords]]
            }

            logger.info(f"✅ Polygone créé avec {len(unique_coords)} points")

            # Calculer les limites (bounding box)
            lons = [coord[0] for coord in unique_coords]
            lats = [coord[1] for coord in unique_coords]

            logger.info(f"Bounding box:")
            logger.info(f"  Longitude: {min(lons):.4f} à {max(lons):.4f}")
            logger.info(f"  Latitude: {min(lats):.4f} à {max(lats):.4f}")

            return json.dumps(geojson)

        except Exception as e:
            logger.error(f"Erreur construction polygone: {e}")
            return None


def debug_boundaries(osm_file, country_name=None):
    """Debug principal."""
    logger.info(f"🔍 Debug extraction frontières: {osm_file}")

    if country_name:
        logger.info(f"🎯 Recherche pays: {country_name}")
    else:
        logger.info("🌍 Analyse de tous les pays")

    # Vérifier que le fichier existe
    if not os.path.exists(osm_file):
        logger.error(f"❌ Fichier non trouvé: {osm_file}")
        return

    # Lancer l'analyse
    debugger = BoundaryDebugger(country_name)

    try:
        logger.info("📖 Lecture du fichier OSM...")
        debugger.apply_file(osm_file)

        # Résultats
        logger.info(f"\n📊 RÉSULTATS:")
        logger.info(f"Pays trouvés: {len(debugger.countries_found)}")
        logger.info(f"Nodes en cache: {len(debugger.nodes_cache):,}")
        logger.info(f"Ways en cache: {len(debugger.ways_cache):,}")

        # Détails par pays
        for country in debugger.countries_found:
            logger.info(f"\n🏳️ {country['name']} ({country['code']})")
            logger.info(f"   ID: {country['id']}")
            logger.info(f"   Ways outer: {country['ways_outer']}")
            logger.info(f"   Ways inner: {country['ways_inner']}")

            if country['boundary']:
                logger.info(f"   ✅ Frontière construite")

                # Sauvegarder pour inspection
                filename = f"debug_boundary_{country['code']}_{country['id']}.geojson"
                with open(filename, 'w') as f:
                    geom = json.loads(country['boundary'])
                    json.dump(geom, f, indent=2)
                logger.info(f"   💾 Sauvegardé: {filename}")
            else:
                logger.info(f"   ❌ Échec construction frontière")

        if not debugger.countries_found:
            logger.warning("⚠️ Aucun pays trouvé. Suggestions:")
            logger.warning("   1. Vérifiez que le fichier contient des données de pays")
            logger.warning("   2. Essayez sans filtrer par nom")
            logger.warning("   3. Vérifiez l'encodage du nom du pays")

    except Exception as e:
        logger.error(f"❌ Erreur lors de l'analyse: {e}")
        raise


def quick_country_list(osm_file):
    """Liste rapide des pays dans le fichier."""
    logger.info(f"📋 Liste des pays dans {osm_file}")

    class QuickLister(osmium.SimpleHandler):
        def __init__(self):
            osmium.SimpleHandler.__init__(self)
            self.countries = []

        def relation(self, r):
            if (r.tags.get('boundary') == 'administrative' and
                r.tags.get('admin_level') == '2'):

                name = r.tags.get('name', f'Country {r.id}')
                code = r.tags.get('ISO3166-1:alpha2', 'N/A')

                self.countries.append({
                    'name': name,
                    'code': code,
                    'id': r.id
                })

    lister = QuickLister()
    lister.apply_file(osm_file)

    logger.info(f"Pays trouvés: {len(lister.countries)}")
    for country in sorted(lister.countries, key=lambda x: x['name']):
        print(f"  {country['name']} ({country['code']}) - ID: {country['id']}")

    return lister.countries


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python debug_boundaries.py <fichier.osm.pbf> [nom_pays]")
        print("  python debug_boundaries.py <fichier.osm.pbf> --list")
        print("\nExemples:")
        print("  python debug_boundaries.py france.osm.pbf France")
        print("  python debug_boundaries.py europe.osm.pbf --list")
        sys.exit(1)

    osm_file = sys.argv[1]

    if len(sys.argv) > 2 and sys.argv[2] == '--list':
        quick_country_list(osm_file)
    else:
        country_name = sys.argv[2] if len(sys.argv) > 2 else None
        debug_boundaries(osm_file, country_name)
