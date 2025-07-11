import boto3
import json
from pathlib import Path
from time import sleep

BASE_DIR = Path(__file__).resolve().parent.parent
KINESIS_STREAM_NAME = 'trip_events_stream'
kinesis = boto3.client('kinesis')

def send_events(path, limit=1000, label=''):
    with open(path) as f:
        lines = f.readlines()

    for i, line in enumerate(lines[:limit]):
        data = json.loads(line.strip())
        payload = json.dumps(data)
        kinesis.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=payload,
            PartitionKey=data['trip_id']
        )

        if i % 100 == 0:
            print(f"✅ {label} Sent {i + 1} events")

    print(f"🏁 Finished sending {label} events")

if __name__ == "__main__":
    start_path = BASE_DIR / 'queues' / 'trip_start_queue.json'
    end_path = BASE_DIR / 'queues' / 'trip_end_queue.json'

    print("📤 Sending 1,000 trip_start events to Kinesis...")
    send_events(start_path, limit=1000, label='trip_start')

    print("📤 Sending 1,000 trip_end events to Kinesis...")
    send_events(end_path, limit=1000, label='trip_end')
