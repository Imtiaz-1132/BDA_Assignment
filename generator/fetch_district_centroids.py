"""Download Bangladesh ADM2 boundaries and derive district representative points."""

import json
from pathlib import Path

import requests

API_URL = "https://www.geoboundaries.org/api/current/gbOpen/BGD/ADM2/"
OUTPUT_DIR = Path("data/geography")


def ring_centroid(ring):
    """Calculate a polygon ring centroid using the shoelace formula."""
    points = ring if ring[0] == ring[-1] else ring + [ring[0]]
    cross_sum = 0.0
    cx_sum = 0.0
    cy_sum = 0.0

    for i in range(len(points) - 1):
        x1, y1 = points[i]
        x2, y2 = points[i + 1]
        cross = x1 * y2 - x2 * y1
        cross_sum += cross
        cx_sum += (x1 + x2) * cross
        cy_sum += (y1 + y2) * cross

    if abs(cross_sum) < 1e-12:
        return None, 0.0

    return (cx_sum / (3 * cross_sum), cy_sum / (3 * cross_sum)), abs(cross_sum) / 2


def polygon_parts(geometry):
    if geometry["type"] == "Polygon":
        return [geometry["coordinates"]]
    if geometry["type"] == "MultiPolygon":
        return geometry["coordinates"]
    raise ValueError(f"Unsupported geometry type: {geometry['type']}")


def geometry_centroid(geometry):
    """Area-weight outer rings across polygons; holes are excluded."""
    weighted_x = 0.0
    weighted_y = 0.0
    total_area = 0.0

    for polygon in polygon_parts(geometry):
        centroid, area = ring_centroid(polygon[0])
        if centroid is not None and area > 0:
            weighted_x += centroid[0] * area
            weighted_y += centroid[1] * area
            total_area += area

    if total_area == 0:
        raise ValueError("Could not calculate centroid for a district")

    return weighted_x / total_area, weighted_y / total_area


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    metadata = response.json()

    geojson_url = metadata.get("simplifiedGeometryGeoJSON") or metadata.get("gjDownloadURL")
    if not geojson_url:
        raise ValueError("geoBoundaries metadata has no GeoJSON download URL")

    geo_response = requests.get(geojson_url, timeout=90)
    geo_response.raise_for_status()
    geojson = geo_response.json()

    raw_path = OUTPUT_DIR / "bangladesh_adm2.geojson"
    raw_path.write_text(json.dumps(geojson), encoding="utf-8")

    features = geojson.get("features", [])
    if len(features) != 64:
        raise ValueError(f"Expected 64 district features, received {len(features)}")

    # Synthetic D001-D064 are mapped deterministically to alphabetically
    # sorted real district names. The mapping must be reused consistently.
    rows = []
    for feature in features:
        properties = feature.get("properties", {})
        name = (
            properties.get("shapeName")
            or properties.get("shapeName_en")
            or properties.get("name")
        )
        if not name:
            raise ValueError(f"Cannot identify district from properties: {list(properties)}")

        longitude, latitude = geometry_centroid(feature["geometry"])
        rows.append({
            "district_name": str(name).strip(),
            "latitude": round(latitude, 6),
            "longitude": round(longitude, 6),
        })

    rows.sort(key=lambda row: row["district_name"].casefold())
    for i, row in enumerate(rows, start=1):
        row["district_id"] = f"D{i:03d}"

    output_path = OUTPUT_DIR / "district_centroids.csv"
    with output_path.open("w", encoding="utf-8", newline="") as file:
        import csv
        writer = csv.DictWriter(
            file,
            fieldnames=["district_id", "district_name", "latitude", "longitude"],
        )
        writer.writeheader()
        writer.writerows(rows)

    attribution = (
        "Boundary data: geoBoundaries, Bangladesh ADM2; "
        f"source: {metadata.get('boundarySource', 'see geoBoundaries metadata')}; "
        f"license: {metadata.get('boundaryLicense', 'see source metadata')}; "
        "https://www.geoboundaries.org/"
    )
    (OUTPUT_DIR / "ATTRIBUTION.txt").write_text(attribution + "\n", encoding="utf-8")

    print(f"Districts: {len(rows)}")
    print(f"GeoJSON: {raw_path}")
    print(f"Centroids: {output_path}")
    print(f"Attribution: {attribution}")
    print("Sample districts:")
    for row in rows[:5]:
        print(row)


if __name__ == "__main__":
    main()
