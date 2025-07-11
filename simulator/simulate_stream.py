import json
import boto3
from time import sleep
from pathlib import Path

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TripEvents')

BASE_DIR = Path(__file__).resolve().parent.parent
start_path = BASE_DIR / 'queues' / 'trip_start_queue.json'
end_path = BASE_DIR / 'queues' / 'trip_end_queue.json'

def load_events(path, limit=1000):
    with open(path) as f:
        lines = f.readlines()
    return [json.loads(line.strip()) for line in lines[:limit]]

def send_to_dynamo(events):
    for i, event in enumerate(events):
        trip_id = event['trip_id']
        event_type = event['event_type']
        data = event['data']

        # Use update_item to merge with existing record
        update_expr = f"SET {event_type}_event = :data, {event_type}_event_arrived_at = :ts"
        expr_attr_vals = {
            ':data': data,
            ':ts': f"{i}"  # just a dummy timestamp, can use datetime.utcnow().isoformat()
        }

        table.update_item(
            Key={'trip_id': trip_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_attr_vals
        )

        if i % 100 == 0:
            print(f"✅ Sent {i+1} events")

if __name__ == "__main__":
    start_events = load_events(start_path, limit=1000)
    end_events = load_events(end_path, limit=1000)

    print("🔁 Sending trip_start events...")
    send_to_dynamo(start_events)

    print("🔁 Sending trip_end events...")
    send_to_dynamo(end_events)

    print("🏁 Finished sending 1000 start + 1000 end events")
