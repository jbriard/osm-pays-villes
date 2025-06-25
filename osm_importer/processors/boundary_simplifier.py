import json
import logging
from typing import List, Dict, Any, Optional
import math

logger = logging.getLogger(__name__)


class BoundarySimplifier:
    """Simplificateur de frontières géographiques sans Shapely."""

    def __init__(self, tolerance: float = 0.01):
        """
        Args:
            tolerance: Tolérance de simplification en degrés (~1km pour 0.01°)
        """
        self.tolerance = tolerance

    def simplify_boundary(self, geometry_data: List[Dict]) -> Optional[str]:
        """Simplifie une frontière et retourne le GeoJSON."""
        try:
            # Convertir les données OSM en coordonnées
            coordinates = self._extract_coordinates(geometry_data)

            if not coordinates:
                return None

            # Simplifier avec Douglas-Peucker
            simplified_coords = self._douglas_peucker_simplify(coordinates, self.tolerance)

            # Convertir en GeoJSON
            return self._to_geojson(simplified_coords)

        except Exception as e:
            logger.error(f"Erreur simplification frontière: {e}")
            return None

    def _extract_coordinates(self, geometry_data: List[Dict]) -> List[tuple]:
        """Extrait les coordonnées depuis les données OSM."""
        coordinates = []

        for geom in geometry_data:
            try:
                if geom['type'] == 'way' and 'nodes' in geom:
                    coords = [(float(node['lon']), float(node['lat'])) for node in geom['nodes']]
                    coordinates.extend(coords)
            except Exception as e:
                logger.warning(f"Erreur extraction coordonnées: {e}")
                continue

        return coordinates

    def _douglas_peucker_simplify(self, coordinates: List[tuple], tolerance: float) -> List[tuple]:
        """Algorithme Douglas-Peucker pour simplifier une ligne."""
        if len(coordinates) <= 2:
            return coordinates

        # Trouver le point le plus éloigné de la ligne start-end
        start = coordinates[0]
        end = coordinates[-1]
        max_distance = 0
        max_index = 0

        for i in range(1, len(coordinates) - 1):
            distance = self._perpendicular_distance(coordinates[i], start, end)
            if distance > max_distance:
                max_distance = distance
                max_index = i

        # Si la distance max est supérieure à la tolérance, subdiviser
        if max_distance > tolerance:
            # Récursion sur les deux segments
            left = self._douglas_peucker_simplify(coordinates[:max_index + 1], tolerance)
            right = self._douglas_peucker_simplify(coordinates[max_index:], tolerance)

            # Combiner (éviter la duplication du point de jonction)
            return left[:-1] + right
        else:
            # Tous les points intermédiaires peuvent être supprimés
            return [start, end]

    def _perpendicular_distance(self, point: tuple, line_start: tuple, line_end: tuple) -> float:
        """Calcule la distance perpendiculaire d'un point à une ligne."""
        x0, y0 = point
        x1, y1 = line_start
        x2, y2 = line_end

        # Si la ligne est un point
        if x1 == x2 and y1 == y2:
            return math.sqrt((x0 - x1)**2 + (y0 - y1)**2)

        # Distance perpendiculaire à la ligne
        numerator = abs((y2 - y1) * x0 - (x2 - x1) * y0 + x2 * y1 - y2 * x1)
        denominator = math.sqrt((y2 - y1)**2 + (x2 - x1)**2)

        return numerator / denominator if denominator != 0 else 0

    def _to_geojson(self, coordinates: List[tuple]) -> str:
        """Convertit des coordonnées en GeoJSON."""
        if len(coordinates) < 3:
            return json.dumps({
                "type": "Polygon",
                "coordinates": [[]]
            })

        # Fermer le polygone si nécessaire
        if coordinates[0] != coordinates[-1]:
            coordinates.append(coordinates[0])

        return json.dumps({
            "type": "Polygon",
            "coordinates": [coordinates]
        })
