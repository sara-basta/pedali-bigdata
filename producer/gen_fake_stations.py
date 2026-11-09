"""Générateur de SECOURS pour data/stations.json (tests de la Partie 3 uniquement).

À remplacer par le stations.json fourni par l'enseignant pour le rendu final.
400 stations : S-CAS-001..S-CAS-100, S-RAB-..., S-MAR-..., S-TNG-...

Lancement :
    docker exec -it jupyter python producer/gen_fake_stations.py
"""
import json
import random

random.seed(7)

CITIES = {
    "CAS": ("Casablanca", 33.5731, -7.5898,
            ["Maarif", "Anfa", "Ain Diab", "Gauthier", "Sidi Maarouf", "Hay Hassani", "Derb Sultan", "Habous"]),
    "RAB": ("Rabat", 34.0209, -6.8416,
            ["Agdal", "Hassan", "Océan", "Souissi", "Hay Riad", "Yacoub El Mansour"]),
    "MAR": ("Marrakech", 31.6295, -7.9811,
            ["Gueliz", "Médina", "Hivernage", "Palmeraie", "Targa", "Mhamid"]),
    "TNG": ("Tanger", 35.7595, -5.8340,
            ["Centre-ville", "Malabata", "Marshan", "Boukhalef", "Iberia"]),
}
ZONES = ["gare", "université", "plage", "centre d'affaires", "résidentiel", "tourisme"]
ZONE_WEIGHTS = [0.08, 0.12, 0.10, 0.18, 0.40, 0.12]

stations = []
for code, (city, lat0, lon0, districts) in CITIES.items():
    for i in range(1, 101):
        zone = "gare" if i == 1 else random.choices(ZONES, ZONE_WEIGHTS)[0]   # 001 = gare principale
        stations.append({
            "station_id": f"S-{code}-{i:03d}",
            "name": f"Station {city} {i:03d}",
            "city": city,
            "district": random.choice(districts),
            "zone_type": zone,
            "capacity": random.choice([10, 12, 15, 20, 25, 30]),
            "latitude": round(lat0 + random.uniform(-0.05, 0.05), 6),
            "longitude": round(lon0 + random.uniform(-0.06, 0.06), 6),
        })

with open("data/stations.json", "w", encoding="utf-8") as f:
    json.dump(stations, f, ensure_ascii=False, indent=1)
print(f"{len(stations)} stations écrites dans data/stations.json")
