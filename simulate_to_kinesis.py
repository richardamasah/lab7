import csv
import json
import boto3
from pathlib import Path

STREAM_NAME = "trip_events_stream"
REGION = "eu-north-1"
MAX_EVENTS = 10  # Number of matched trips to simulate

kinesis = boto3.client("kinesis", region_name=REGION)

def load_csv(path):
    with open(path, 'r') as f:
        return list(csv.DictReader(f))

def send_event(event):
    response = kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(event),
        PartitionKey=event["trip_id"]
    )
    print(f"✅ Sent: {event['event_type']} - {event['trip_id']}")

def build_event(row, event_type):
    trip_id = row["trip_id"].strip()
    row["trip_id"] = trip_id  # Ensure consistency
    return {
        "event_type": event_type,
        "trip_id": trip_id,
        "data": row
    }

if __name__ == "__main__":
    base = Path(__file__).resolve().parent
    data_dir = base / "data"

    trip_start = load_csv(data_dir / "trip_start.csv")
    trip_end = load_csv(data_dir / "trip_end.csv")

    start_ids = {row["trip_id"].strip() for row in trip_start}
    end_ids = {row["trip_id"].strip() for row in trip_end}
    matched_ids = list(start_ids & end_ids)[:MAX_EVENTS]

    start_events = [build_event(row, "trip_start") for row in trip_start if row["trip_id"].strip() in matched_ids]
    end_events = [build_event(row, "trip_end") for row in trip_end if row["trip_id"].strip() in matched_ids]

    print(f"📤 Sending {len(start_events)} trip_start and {len(end_events)} trip_end events to Kinesis...")

    for event in start_events + end_events:
        send_event(event)
