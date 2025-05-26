"""
street_scraper.py

Récupère, pour chaque municipalité du Québec, la liste de
toutes les rues nommées, et écrit un CSV avec colonnes :
province, city, street.
"""

import csv
import json
import time
from pathlib import Path

import requests

# ── Config ───────────────────────────────────────────────────────────────
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
HEADERS = {"User-Agent": "QC-Street-Scraper/1.0"}

BASE = Path(__file__).resolve().parent
DATA_DIR = BASE / "data"
CITIES_CACHE = DATA_DIR / "qc_cities.json"
OUTPUT_CSV = DATA_DIR / "streets.csv"

DATA_DIR.mkdir(exist_ok=True)


# ── Helpers Overpass ─────────────────────────────────────────────────────
def fetch_all_cities() -> dict[str, int]:
    q = (
        "[out:json][timeout:60];"
        'area["boundary"="administrative"]["admin_level"="4"]["name"="Québec"]->.prov;'
        'rel["boundary"="administrative"]["admin_level"="8"](area.prov);'
        "out tags;"
    )
    r = requests.post(OVERPASS_URL, data=q, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return {
        elt["tags"]["name"]: elt["id"]
        for elt in r.json().get("elements", [])
        if "tags" in elt and elt["tags"].get("name")
    }


def fetch_streets_for_city(rel_id: int) -> list[str]:
    area_id = 3600000000 + rel_id
    q = (
        "[out:json][timeout:120];"
        f"area({area_id})->.a;"
        '(way["highway"]["name"](area.a););'
        "out tags;"
    )
    r = requests.post(OVERPASS_URL, data=q, headers=HEADERS, timeout=120)
    r.raise_for_status()
    elems = r.json().get("elements", [])
    return sorted(
        {
            elt["tags"]["name"].strip()
            for elt in elems
            if "tags" in elt and "name" in elt["tags"]
        }
    )


# ── Main ────────────────────────────────────────────────────────────────
def main():
    # 1) charger ou construire le cache des villes
    if CITIES_CACHE.exists():
        # on précise encoding pour lire en UTF-8
        raw = CITIES_CACHE.read_text(encoding="utf-8")
        city2rel = json.loads(raw)
    else:
        city2rel = fetch_all_cities()
        # on précise encoding pour écrire en UTF-8
        CITIES_CACHE.write_text(
            json.dumps(city2rel, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    # 2) ouvrir le CSV et écrire l’en-tête
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["province", "city", "street"])

        # 3) pour chaque ville, scraper ses rues
        for city, rel_id in city2rel.items():
            print(f"→ {city:<30}", end="")
            try:
                streets = fetch_streets_for_city(rel_id)
                for street in streets:
                    writer.writerow(["Québec", city, street])
                print(f"{len(streets):4d} rues")
            except Exception as e:
                print(f"ERROR: {e}")
            time.sleep(1)  # pour respecter Overpass

    print(f"\nTerminé ! Toutes les rues enregistrées dans : {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
