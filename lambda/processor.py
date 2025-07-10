import json
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / 'dynamodb_mock' / 'db.json'

# ---------- Helper functions ----------
def load_db():
    if DB_PATH.exists():
        with open(DB_PATH, 'r') as f:
            return json.load(f)
    return {}

def save_db(data):
    with open(DB_PATH, 'w') as f:
        json.dump(data, f, indent=2)

def validate_event(event):
    # Simple check – extend later
    if not all(k in event for k in ["trip_id", "data", "event_type"]):
        return False
    return True

def process_event(event):
    trip_id = event['trip_id']
    db = load_db()

    trip = db.get(trip_id, {
        "trip_id": trip_id,
        "status": "new",
        "start_event": None,
        "end_event": None
    })

    if event['event_type'] == "trip_start":
        trip["start_event"] = event["data"]
    elif event['event_type'] == "trip_end":
        trip["end_event"] = event["data"]

    # Set status based on available parts
    if trip["start_event"] and trip["end_event"]:
        trip["status"] = "complete"
    else:
        trip["status"] = "in_progress"

    db[trip_id] = trip
    save_db(db)

    print(f"✅ Processed {event['event_type']} for {trip_id} → Status: {trip['status']}")

# ---------- Run the processor ----------
def run_processor(queue_file):
    with open(queue_file, 'r') as f:
        for line in f:
            event = json.loads(line)
            if validate_event(event):
                process_event(event)
            else:
                print(f"❌ Invalid event skipped: {line}")

if __name__ == "__main__":
    base = Path(__file__).resolve().parent.parent
    run_processor(base / 'queues' / 'trip_start_queue.json')
    run_processor(base / 'queues' / 'trip_end_queue.json')

    print("🏁 All events processed.")
