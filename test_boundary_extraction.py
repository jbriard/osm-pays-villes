#!/usr/bin/env python3
"""Test simple de l'extraction des frontières."""

import sys
import os
import logging
from pathlib import Path

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_boundaries(osm_file_path):
    """Test l'extraction des frontières sur un fichier OSM."""

    print(f"🔍 Test d'extraction des frontières")
    print(f"📁 Fichier: {osm_file_path}")

    # Vérifier que le fichier existe
    if not os.path.exists(osm_file_path):
        print(f"❌ Erreur: Fichier non trouvé: {osm_file_path}")
        return False

    # Taille du fichier
    file_size = os.path.getsize(osm_file_path) / (1024 * 1024)
    print(f"📏 Taille: {file_size:.1f} MB")

    try:
        # Importer le parser
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

        # Test 1: Parser simple pour lister les pays
        print("\n📋 Étape 1: Liste des pays disponibles")
        countries_found = list_countries_in_file(osm_file_path)

        if not countries_found:
            print("❌ Aucun pays trouvé dans le fichier")
            return False

        print(f"✅ Trouvé {len(countries_found)} pays")
        for country in countries_found[:5]:  # Afficher les 5 premiers
            print(f"   • {country['name']} ({country['code']}) - ID: {country['id']}")

        if len(countries_found) > 5:
            print(f"   ... et {len(countries_found) - 5} autres")

        # Test 2: Extraction complète avec frontières
        print(f"\n🗺️  Étape 2: Extraction des frontières")

        # Prendre le premier pays pour test
        test_country = countries_found[0]
        print(f"Test avec: {test_country['name']} (ID: {test_country['id']})")

        success = extract_single_country_boundary(osm_file_path, test_country['id'])

        if success:
            print("✅ Test réussi ! Les frontières peuvent être extraites.")
            return True
        else:
            print("❌ Test échoué. Problème avec l'extraction des frontières.")
            return False

    except Exception as e:
        print(f"❌ Erreur lors du test: {e}")
        import traceback
        traceback.print_exc()
        return False


def list_countries_in_file(osm_file_path):
    """Liste rapidement les pays dans un fichier OSM."""
    import osmium

    class CountryLister(osmium.SimpleHandler):
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

    lister = CountryLister()
    lister.apply_file(osm_file_path)

    return sorted(lister.countries, key=lambda x: x['name'])


def extract_single_country_boundary(osm_file_path, country_id):
    """Extrait la frontière d'un seul pays pour test."""
    import osmium
    import json

    class SingleCountryExtractor(osmium.SimpleHandler):
        def __init__(self, target_country_id):
            osmium.SimpleHandler.__init__(self)
            self.target_country_id = target_country_id
            self.target_ways = set()
            self.target_nodes = set()
            self.ways_data = {}
            self.nodes_data = {}
            self.country_name = None
            self.pass_number = 1

        def relation(self, r):
            if self.pass_number == 1 and r.id == self.target_country_id:
                self.country_name = r.tags.get('name', f'Country {r.id}')
                print(f"   Trouvé relation pays: {self.country_name}")

                # Collecter les ways
                for member in r.members:
                    if member.type == 'w':
                        self.target_ways.add(member.ref)

                print(f"   Ways à extraire: {len(self.target_ways)}")

        def way(self, w):
            if self.pass_number == 2 and w.id in self.target_ways:
                node_refs = [node.ref for node in w.nodes]
                self.ways_data[w.id] = node_refs
                self.target_nodes.update(node_refs)

        def node(self, n):
            if self.pass_number == 3 and n.id in self.target_nodes:
                self.nodes_data[n.id] = (n.location.lon, n.location.lat)

    extractor = SingleCountryExtractor(country_id)

    # Passe 1: Trouver les ways
    print("   Passe 1: Identification des ways...")
    extractor.pass_number = 1
    extractor.apply_file(osm_file_path)

    if not extractor.target_ways:
        print("   ❌ Aucun way trouvé pour ce pays")
        return False

    # Passe 2: Extraire les ways
    print(f"   Passe 2: Extraction de {len(extractor.target_ways)} ways...")
    extractor.pass_number = 2
    extractor.apply_file(osm_file_path)

    print(f"   Ways extraits: {len(extractor.ways_data)}")

    # Calculer les nodes nécessaires
    for way_nodes in extractor.ways_data.values():
        extractor.target_nodes.update(way_nodes)

    # Passe 3: Extraire les nodes
    print(f"   Passe 3: Extraction de {len(extractor.target_nodes)} nodes...")
    extractor.pass_number = 3
    extractor.apply_file(osm_file_path)

    print(f"   Nodes extraits: {len(extractor.nodes_data)}")

    # Construire la géométrie
    print("   Construction de la géométrie...")

    all_coords = []
    valid_ways = 0

    for way_id, node_refs in extractor.ways_data.items():
        way_coords = []
        for node_ref in node_refs:
            if node_ref in extractor.nodes_data:
                way_coords.append(extractor.nodes_data[node_ref])

        if len(way_coords) >= 2:
            all_coords.extend(way_coords)
            valid_ways += 1

    print(f"   Ways valides: {valid_ways}/{len(extractor.ways_data)}")
    print(f"   Points totaux: {len(all_coords)}")

    if len(all_coords) < 3:
        print("   ❌ Pas assez de points pour un polygone")
        return False

    # Nettoyer et fermer le polygone
    unique_coords = [all_coords[0]]
    for coord in all_coords[1:]:
        if coord != unique_coords[-1]:
            unique_coords.append(coord)

    if unique_coords[0] != unique_coords[-1]:
        unique_coords.append(unique_coords[0])

    if len(unique_coords) < 4:
        print("   ❌ Polygone trop petit après nettoyage")
        return False

    # Créer le GeoJSON
    geojson = {
        "type": "Feature",
        "properties": {
            "name": extractor.country_name,
            "osm_id": country_id
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[lon, lat] for lon, lat in unique_coords]]
        }
    }

    # Statistiques
    lons = [coord[0] for coord in unique_coords]
    lats = [coord[1] for coord in unique_coords]

    print(f"   ✅ Polygone créé avec {len(unique_coords)} points")
    print(f"   Bounding box: {min(lons):.3f},{min(lats):.3f} à {max(lons):.3f},{max(lats):.3f}")

    # Sauvegarder
    output_file = f"test_boundary_{country_id}.geojson"
    with open(output_file, 'w') as f:
        json.dump(geojson, f, indent=2)

    print(f"   💾 Frontière sauvegardée: {output_file}")

    return True


def main():
    """Point d'entrée principal."""
    if len(sys.argv) != 2:
        print("Usage: python test_boundary_extraction.py <fichier.osm.pbf>")
        print("\nExemples:")
        print("  python test_boundary_extraction.py france.osm.pbf")
        print("  python test_boundary_extraction.py europe.osm.pbf")
        sys.exit(1)

    osm_file = sys.argv[1]

    print("🧪 Test d'extraction des frontières OSM")
    print("=" * 50)

    success = test_boundaries(osm_file)

    print("\n" + "=" * 50)
    if success:
        print("✅ SUCCÈS: L'extraction des frontières fonctionne!")
        print("\n💡 Prochaines étapes:")
        print("   1. Intégrer le parser v2 dans votre système")
        print("   2. Tester avec vos fichiers OSM complets")
        print("   3. Vérifier les frontières dans QGIS ou un visualiseur GeoJSON")
    else:
        print("❌ ÉCHEC: Problème avec l'extraction des frontières")
        print("\n🔧 Suggestions de débogage:")
        print("   1. Vérifiez que le fichier OSM contient bien des données de pays")
        print("   2. Essayez avec un fichier plus petit (ex: Monaco, Liechtenstein)")
        print("   3. Vérifiez les logs pour plus de détails")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
