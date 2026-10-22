"""Aggregate WorldPop 2022 population rasters by Bangladesh district."""

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from rasterio.features import rasterize

ROOT = Path(__file__).resolve().parents[1]
RASTER_DIR = ROOT / "data" / "population"
GEOJSON_PATH = ROOT / "data" / "geography" / "bangladesh_adm2.geojson"
OUTPUT_PATH = RASTER_DIR / "district_population_2022.csv"


def main():
    geojson = json.loads(GEOJSON_PATH.read_text(encoding="utf-8"))
    features = geojson["features"]

    if len(features) != 64:
        raise ValueError(f"Expected 64 districts; found {len(features)}")

    districts = [
        feature["properties"]["shapeName"] for feature in features
    ]
    rasters = sorted(RASTER_DIR.glob("bgd_*_2022_*.tif"))

    if not rasters:
        raise FileNotFoundError(f"No WorldPop TIFF files found in {RASTER_DIR}")

    rows = []

    for raster_path in rasters:
        match = re.fullmatch(
            r"bgd_([fmt])_(\d+)_2022_CN_1km_R2025A_UA_v1\.tif",
            raster_path.name,
        )
        if not match:
            print(f"Skipping unexpected filename: {raster_path.name}")
            continue

        sex_code, age_code = match.groups()
        sex = {"f": "female", "m": "male", "t": "total"}[sex_code]

        with rasterio.open(raster_path) as src:
            if src.crs is None or src.crs.to_epsg() != 4326:
                raise ValueError(f"Unexpected CRS in {raster_path.name}: {src.crs}")

            raster = src.read(1, masked=True)

            district_labels = rasterize(
                [
                    (feature["geometry"], index)
                    for index, feature in enumerate(features, start=1)
                ],
                out_shape=(src.height, src.width),
                transform=src.transform,
                fill=0,
                dtype="int16",
                all_touched=False,
            )

            valid = (
                (district_labels > 0)
                & ~np.ma.getmaskarray(raster)
                & np.isfinite(raster.data)
                & (raster.data >= 0)
            )

            labels = district_labels[valid]
            values = raster.data[valid].astype("float64")

            sums = np.bincount(
                labels, weights=values, minlength=len(districts) + 1
            )

            for index, district in enumerate(districts, start=1):
                rows.append({
                    "district": district,
                    "year": 2022,
                    "sex": sex,
                    "age_group": age_code,
                    "population": round(float(sums[index]), 2),
                    "source_file": raster_path.name,
                    "source": "WorldPop",
                })

        print("Processed:", raster_path.name)

    result = pd.DataFrame(rows)
    result.to_csv(OUTPUT_PATH, index=False)

    print("\nAggregation completed.")
    print("Raster files processed:", result["source_file"].nunique())
    print("Districts:", result["district"].nunique())
    print("Output rows:", len(result))
    print("CSV:", OUTPUT_PATH)


if __name__ == "__main__":
    main()
