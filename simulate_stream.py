import csv
import json
from pathlib import Path

MAX_MATCHED_TRIPS = 10  # ✅ Simulate only 10 matching trips

def read_csv_as_dict(path):
    with open(path, 'r') as f:
        return list(csv.DictReader(f))

def write_queue_file(path, events):
    with open(path, 'w') as f:
        for event in events:
            f.write(json.dumps(event) + '\n')

def build_event(row, event_type):
    trip_id = row.get("trip_id", "").strip()
    row["trip_id"] = trip_id  # ensure trip_id key exists in .data
    return {
        "event_type": event_type,
        "trip_id": trip_id,
        "data": row
    }

if __name__ == "__main__":
    base = Path(__file__).resolve().parent
    data_dir = base / "data"
    queue_dir = base / "queues"
    queue_dir.mkdir(exist_ok=True)

    # Read both CSVs
    start_rows = read_csv_as_dict(data_dir / "trip_start.csv")
    end_rows = read_csv_as_dict(data_dir / "trip_end.csv")

    # Build lookup sets
    start_ids = {row["trip_id"].strip() for row in start_rows}
    end_ids = {row["trip_id"].strip() for row in end_rows}
    common_ids = list(start_ids & end_ids)[:MAX_MATCHED_TRIPS]

    # Filter only matched rows
    start_events = [build_event(row, "trip_start") for row in start_rows if row["trip_id"].strip() in common_ids]
    end_events = [build_event(row, "trip_end") for row in end_rows if row["trip_id"].strip() in common_ids]

    # Save to queue files
    write_queue_file(queue_dir / "trip_start_queue.json", start_events)
    write_queue_file(queue_dir / "trip_end_queue.json", end_events)

    print(f"✅ Created queue with {len(start_events)} matched trip_start and {len(end_events)} trip_end events.")
