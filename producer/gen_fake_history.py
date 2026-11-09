"""Générateur de SECOURS pour trips_history.jsonl (tests de la Partie 2 uniquement).
 
À remplacer par le producer.py fourni par l'enseignant pour le rendu final.
Respecte le schéma du sujet : un événement JSON par changement d'état,
~5 % d'événements en retard, ~2 % de doublons, champs null selon le statut.
 
Lancement (dans le conteneur, rien à installer sur Windows) :
    docker exec -it jupyter python producer/gen_fake_history.py --n 50000
Sortie : data/trips_history.jsonl
"""
import argparse
import json
import random
from datetime import datetime, timedelta, timezone
 
CITIES = {"Casablanca": "CAS", "Rabat": "RAB", "Marrakech": "MAR", "Tanger": "TNG"}
CITY_WEIGHTS = [0.45, 0.2, 0.2, 0.15]
STATIONS_PER_CITY = 100                      # 400 stations au total
VEHICLES = {"BIKE": (1.0, 0.8), "EBIKE": (1.6, 1.2), "SCOOTER": (1.3, 1.0)}  # (MAD/min, vitesse relative)
 
ap = argparse.ArgumentParser()
ap.add_argument("--n", type=int, default=50000, help="nombre d'événements")
ap.add_argument("--seed", type=int, default=42)
ap.add_argument("--out", default="data/trips_history.jsonl")
args = ap.parse_args()
random.seed(args.seed)
 
start = datetime(2026, 11, 1, tzinfo=timezone.utc)
 
 
def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
 
 
def station(code):
    # stations populaires : distribution asymétrique pour avoir un vrai top 5
    i = min(int(random.expovariate(1 / 25)), STATIONS_PER_CITY - 1) + 1
    return f"S-{code}-{i:03d}"
 
 
def trip_events(n):
    city = random.choices(list(CITIES), CITY_WEIGHTS)[0]
    code = CITIES[city]
    vtype = random.choice(list(VEHICLES))
    # heures de pointe plus fréquentes
    hour = random.choices(range(24), [1]*6 + [4, 8, 8, 4, 3, 3, 3, 3, 3, 4, 6, 8, 7, 4, 3, 2, 2, 1])[0]
    t0 = start + timedelta(days=random.randint(0, 29), hours=hour,
                           minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    base = {
        "trip_id": f"T-{n:08d}",
        "user_id": f"U-{random.randint(20000, 20999)}",
        "vehicle_id": f"V-{vtype[:2]}-{random.randint(1, 999):04d}",
        "vehicle_type": vtype,
        "start_station": station(code),
        "end_station": None, "city": city, "status": None,
        "duration_s": None, "distance_km": None,
        "battery_pct": random.randint(15, 100) if vtype != "BIKE" else None,
        "price_mad": None, "event_time": None,
    }
    r = random.random()
    steps = [("RESERVED", t0)]
    if r < 0.12:
        steps.append(("CANCELLED", t0 + timedelta(seconds=random.randint(30, 300))))
    else:
        t1 = t0 + timedelta(seconds=random.randint(60, 400))
        steps.append(("UNLOCKED", t1))
        if r < 0.17:
            steps.append(("FAULT_REPORTED", t1 + timedelta(seconds=random.randint(60, 600))))
        else:
            dur = int(random.lognormvariate(6.9, 0.55))          # médiane ~16 min
            dur = max(120, min(dur, 5400))
            price_min, speed = VEHICLES[vtype]
            steps.append(("ENDED", t1 + timedelta(seconds=dur)))
            end = {"duration_s": dur,
                   "distance_km": round(dur / 3600 * 15 * speed, 1),
                   "price_mad": round(5 + dur / 60 * price_min, 2),
                   "end_station": station(code)}
    events = []
    for status, ts in steps:
        e = dict(base, status=status, event_time=iso(ts))
        if status == "ENDED":
            e.update(end)
        events.append(e)
    return events
 
 
rows, trip_no = [], 78000
while len(rows) < args.n:
    trip_no += 1
    rows.extend(trip_events(trip_no))
rows = rows[:args.n]
 
# ~5 % en retard (event_time reculé jusqu'à 15 min), ~2 % de doublons
for e in rows:
    if random.random() < 0.05:
        t = datetime.strptime(e["event_time"], "%Y-%m-%dT%H:%M:%SZ")
        e["event_time"] = iso(t - timedelta(seconds=random.randint(60, 900)))
rows += [dict(e) for e in random.sample(rows, int(len(rows) * 0.02))]
random.shuffle(rows)
 
with open(args.out, "w", encoding="utf-8") as f:
    for e in rows:
        f.write(json.dumps(e) + "\n")
print(f"{len(rows)} événements écrits dans {args.out}")
 