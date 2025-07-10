import json
import time
from pathlib import Path
from datetime import datetime

# ------------------- PATH ---------------------
DB_PATH = Path(__file__).resolve().parent.parent / 'dynamodb_mock' / 'db.json'

# ------------------- HELPERS -------------------
def load_db():
    if DB_PATH.exists():
        with open(DB_PATH, 'r') as f:
            return json.load(f)
    return {}

def save_db(data):
    with open(DB_PATH, 'w') as f:
        json.dump(data, f, indent=2)

def validate_event(event):
    return all(k in event for k in ["trip_id", "data", "event_type"])

def current_time():
    return datetime.utcnow().isoformat()

# ------------------- MAIN PROCESSING -------------------
def process_event(event):
    trip_id = event['trip_id'].strip()
    db = load_db()

    trip = db.get(trip_id)
    if not trip:
        trip = {
            "trip_id": trip_id,
            "status": "new",
            "start_event": None,
            "end_event": None,
            "start_event_arrived_at": None,
            "end_event_arrived_at": None,
            "is_stale": False
        }
    else:
        trip.setdefault("start_event", None)
        trip.setdefault("end_event", None)
        trip.setdefault("start_event_arrived_at", None)
        trip.setdefault("end_event_arrived_at", None)
        trip.setdefault("is_stale", False)

    event_type = event['event_type']
    new_data = event["data"]

    # 🔐 IDEMPOTENCY CHECK
    if event_type == "trip_start" and trip["start_event"] == new_data:
        print(f"⚠️ Duplicate trip_start for {trip_id} → skipped")
        return
    if event_type == "trip_end" and trip["end_event"] == new_data:
        print(f"⚠️ Duplicate trip_end for {trip_id} → skipped")
        return

    # 🧠 Inject data + timestamp
    if event_type == "trip_start":
        trip["start_event"] = new_data
        trip["start_event_arrived_at"] = current_time()

    elif event_type == "trip_end":
        trip["end_event"] = new_data
        trip["end_event_arrived_at"] = current_time()

    # 🔁 Update status
    if trip["start_event"] and trip["end_event"]:
        trip["status"] = "complete"
    else:
        trip["status"] = "in_progress"

    # ---------------- STALE DETECTION ----------------
    trip["is_stale"] = False  # default

    cutoff_minutes = 10  # Simulate 10 min; change to 60 in production

    if trip["status"] == "in_progress":
        now = datetime.utcnow()
        if trip["start_event"] and not trip["end_event"]:
            try:
                t = datetime.fromisoformat(trip["start_event_arrived_at"])
                if (now - t).total_seconds() > cutoff_minutes * 60:
                    trip["is_stale"] = True
            except:
                pass

        elif trip["end_event"] and not trip["start_event"]:
            try:
                t = datetime.fromisoformat(trip["end_event_arrived_at"])
                if (now - t).total_seconds() > cutoff_minutes * 60:
                    trip["is_stale"] = True
            except:
                pass

    # 🔐 Save to DB
    db[trip_id] = trip
    save_db(db)

    print(f"✅ {event_type} for {trip_id} → {trip['status']} | stale: {trip['is_stale']}")

# ------------------- QUEUE SIMULATOR -------------------
def run_processor(queue_file, mode='one_by_one', batch_size=5, delay=0.1):
    print(f"📂 Reading from: {queue_file}")

    with open(queue_file, 'r') as f:
        lines = f.readlines()

    print(f"📊 Total lines in file: {len(lines)}")

    if mode == 'one_by_one':
        for i, line in enumerate(lines):
            try:
                event = json.loads(line)
                if validate_event(event):
                    if event['trip_id'] == "c66ce556bc":
                        print(f"📥 [{i}] Handling trip_id: {event['trip_id']} from {event['event_type']}")
                    process_event(event)
                else:
                    print(f"❌ [{i}] Invalid schema: {line}")
            except Exception as e:
                print(f"❌ [{i}] Failed to parse line: {e}")
            time.sleep(delay)

    elif mode == 'batch':
        for i in range(0, len(lines), batch_size):
            batch = lines[i:i+batch_size]
            for j, line in enumerate(batch):
                try:
                    event = json.loads(line)
                    if validate_event(event):
                        process_event(event)
                    else:
                        print(f"❌ [{i+j}] Invalid schema: {line}")
                except Exception as e:
                    print(f"❌ [{i+j}] Failed to parse line: {e}")
            print(f"🟡 Batch of {len(batch)} processed...\n")
            time.sleep(delay)

# ------------------- MAIN -------------------
if __name__ == "__main__":
    base = Path(__file__).resolve().parent.parent

    print("🔁 Processing trip_start_queue.json...")
    run_processor(base / 'queues' / 'trip_start_queue.json', mode='one_by_one', delay=0.05)

    print("\n🔁 Processing trip_end_queue.json...")
    run_processor(base / 'queues' / 'trip_end_queue.json', mode='one_by_one', delay=0.05)

    print("\n🏁 All events processed.")
