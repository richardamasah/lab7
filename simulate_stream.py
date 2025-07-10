import csv
import json
from pathlib import Path

def csv_to_json_lines(csv_path, json_path, event_type):
    with open(csv_path, 'r') as csvfile, open(json_path, 'w') as jsonfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            event = {
                "event_type": event_type,       # "trip_start" or "trip_end"
                "trip_id": row.get("trip_id"),  # for partitioning
                "data": row                     # raw data as-is
            }
            jsonfile.write(json.dumps(event) + '\n')

if __name__ == "__main__":
    # Paths
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / 'data'
    queue_dir = base_dir / 'queues'
    queue_dir.mkdir(exist_ok=True)

    # Convert trip_start.csv
    csv_to_json_lines(
        csv_path=data_dir / 'trip_start.csv',
        json_path=queue_dir / 'trip_start_queue.json',
        event_type="trip_start"
    )

    # Convert trip_end.csv
    csv_to_json_lines(
        csv_path=data_dir / 'trip_end.csv',
        json_path=queue_dir / 'trip_end_queue.json',
        event_type="trip_end"
    )

    print("✅ Streams simulated: trip_start_queue.json and trip_end_queue.json")
