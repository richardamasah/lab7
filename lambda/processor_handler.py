import json
import base64
import boto3
from datetime import datetime
import re

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

TABLE_NAME = 'TripEvents'
BUCKET_NAME = 'lab6kinesis'
RAW_FOLDER = 'raw_events'
INVALID_FOLDER = 'invalid_events'

table = dynamodb.Table(TABLE_NAME)

def current_time():
    return datetime.utcnow().isoformat()

def is_valid_event(event):
    """Basic validation for trip_start and trip_end"""
    event_type = event.get("event_type")
    data = event.get("data", {})
    
    if event_type not in ["trip_start", "trip_end"]:
        return False
    
    # Required for both
    if "trip_id" not in event or not event["trip_id"]:
        return False

    if event_type == "trip_start":
        required_fields = ["pickup_location_id", "dropoff_location_id", "vendor_id", "pickup_datetime", "estimated_dropoff_datetime", "estimated_fare_amount"]
    else:  # trip_end
        required_fields = ["dropoff_datetime", "rate_code", "passenger_count", "trip_distance", "fare_amount", "tip_amount", "payment_type", "trip_type", "trip_id"]

    for field in required_fields:
        if field not in data:
            return False

    # Optional: Validate datetime format
    if "pickup_datetime" in data:
        if not re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", data["pickup_datetime"]):
            return False

    return True

def archive_event_to_s3(event, folder):
    date = datetime.utcnow().strftime("%Y-%m-%d")
    trip_id = event.get("trip_id", "unknown")
    key = f"{folder}/{date}/{trip_id}.json"
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(event, indent=2))
    print(f"📦 Archived to: {key}")

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            event_data = json.loads(payload)

            # Archive ALL events to raw
            archive_event_to_s3(event_data, RAW_FOLDER)

            # Validate
            if not is_valid_event(event_data):
                archive_event_to_s3(event_data, INVALID_FOLDER)
                print(f"❌ Invalid schema: {event_data.get('trip_id')}")
                continue

            process_event(event_data)

        except Exception as e:
            print(f"❌ Processing failed: {e}")

    return {"statusCode": 200}

def process_event(event):
    trip_id = event['trip_id'].strip()
    event_type = event['event_type']
    new_data = event['data']

    try:
        response = table.get_item(Key={'trip_id': trip_id})
        trip = response.get('Item', {
            "trip_id": trip_id,
            "status": "new",
            "start_event": None,
            "end_event": None,
            "start_event_arrived_at": None,
            "end_event_arrived_at": None,
            "is_stale": False
        })

        # Idempotency
        if event_type == "trip_start" and trip.get("start_event") == new_data:
            print(f"⚠️ Duplicate trip_start for {trip_id}")
            return
        if event_type == "trip_end" and trip.get("end_event") == new_data:
            print(f"⚠️ Duplicate trip_end for {trip_id}")
            return

        # Add event
        if event_type == "trip_start":
            trip["start_event"] = new_data
            trip["start_event_arrived_at"] = current_time()
        else:
            trip["end_event"] = new_data
            trip["end_event_arrived_at"] = current_time()

        # Determine status
        trip["status"] = "complete" if trip["start_event"] and trip["end_event"] else "in_progress"

        # Check stale
        trip["is_stale"] = False
        now = datetime.utcnow()
        if trip["status"] == "in_progress":
            if trip["start_event"] and not trip["end_event"]:
                t = datetime.fromisoformat(trip["start_event_arrived_at"])
                if (now - t).total_seconds() > 600:
                    trip["is_stale"] = True
            elif trip["end_event"] and not trip["start_event"]:
                t = datetime.fromisoformat(trip["end_event_arrived_at"])
                if (now - t).total_seconds() > 600:
                    trip["is_stale"] = True

        table.put_item(Item=trip)
        print(f"✅ {event_type} for {trip_id} → {trip['status']} | stale: {trip['is_stale']}")

    except Exception as e:
        print(f"❌ Failed to process trip {trip_id}: {e}")
