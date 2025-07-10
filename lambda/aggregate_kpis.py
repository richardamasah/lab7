import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from datetime import datetime


DB_PATH = Path(__file__).resolve().parent.parent / 'dynamodb_mock' / 'db.json'
OUT_DIR = Path(__file__).resolve().parent.parent / 'output' / 'kpis'
OUT_DIR.mkdir(parents=True, exist_ok=True)

def load_db():
    if DB_PATH.exists():
        with open(DB_PATH, 'r') as f:
            return json.load(f)
    return {}

def aggregate_kpis(trips):
    result_by_date = defaultdict(list)
    stale_count_by_date = defaultdict(int)

    for trip_id, trip in trips.items():
        if trip.get("status") != "complete":
            continue

        start_event = trip.get("start_event", {})
        end_event = trip.get("end_event", {})

        date_str = start_event.get("pickup_datetime", "")
        if not date_str:
            continue

        trip_date = date_str.split(" ")[0]
        fare = float(end_event.get("fare_amount", 0.0))

        result_by_date[trip_date].append(fare)

        # Count stale trips even if complete (optional)
        if trip.get("is_stale") == True:
            stale_count_by_date[trip_date] += 1

    for trip_date, fares in result_by_date.items():
        total = sum(fares)
        count = len(fares)
        avg = total / count if count else 0
        min_fare = min(fares) if fares else 0
        max_fare = max(fares) if fares else 0
        stale_count = stale_count_by_date.get(trip_date, 0)

        kpi = {
            "trip_date": trip_date,
            "total_trips": count,
            "total_fare": round(total, 2),
            "average_fare": round(avg, 2),
            "min_fare": round(min_fare, 2),
            "max_fare": round(max_fare, 2),
            "stale_trip_count": stale_count
        }

        out_path = OUT_DIR / f"{trip_date}_kpis.json"
        with open(out_path, 'w') as f:
            json.dump(kpi, f, indent=2)

        print(f"✅ KPI saved: {out_path.name}")

def generate_in_progress_kpis(trips):
    summary_by_date = defaultdict(lambda: {"in_progress_trips": 0, "stale_in_progress": 0})

    for trip_id, trip in trips.items():
        if trip.get("status") != "in_progress":
            continue

        start_event = trip.get("start_event", {})
        date_str = start_event.get("pickup_datetime", "")
        if not date_str:
            continue

        trip_date = date_str.split(" ")[0]

        summary_by_date[trip_date]["in_progress_trips"] += 1
        if trip.get("is_stale"):
            summary_by_date[trip_date]["stale_in_progress"] += 1

    for trip_date, data in summary_by_date.items():
        out_path = OUT_DIR / f"in_progress_summary_{trip_date}.json"
        with open(out_path, 'w') as f:
            json.dump({
                "trip_date": trip_date,
                **data
            }, f, indent=2)

        print(f"🟡 In-progress KPI saved: {out_path.name}")





if __name__ == "__main__":
    trips = load_db()
    aggregate_kpis(trips)
    generate_in_progress_kpis(trips)

