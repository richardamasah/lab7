import json
import boto3
from time import sleep # Imported but not used in the provided snippet's logic
from pathlib import Path
from datetime import datetime # Import datetime for proper timestamps
from botocore.exceptions import ClientError # Import specific exception for AWS client errors

# --- AWS Client Initialization ---
# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
# Get a reference to the DynamoDB table
table = dynamodb.Table('TripEvents')

# --- File Paths Configuration ---
# Determine the base directory for relative path resolution
BASE_DIR = Path(__file__).resolve().parent.parent
# Define paths to the JSON files containing trip events
start_path = BASE_DIR / 'queues' / 'trip_start_queue.json'
end_path = BASE_DIR / 'queues' / 'trip_end_queue.json'

# --- Helper Functions ---

def load_events(path, limit=1000):
    """
    Loads trip events from a JSONL (JSON Lines) file.
    Each line in the file is expected to be a valid JSON object.
    
    Args:
        path (Path): The path to the JSONL file.
        limit (int): The maximum number of events to load.
        
    Returns:
        list: A list of loaded event dictionaries.
    """
    events = []
    try:
        print(f"Attempting to load events from: {path}")
        with open(path, 'r') as f:
            for i, line in enumerate(f):
                if i >= limit:
                    break
                try:
                    events.append(json.loads(line.strip()))
                except json.JSONDecodeError as e:
                    print(f"Error: Failed to parse JSON on line {i+1} in {path}. Skipping line. Error: {e}. Line content: {line.strip()[:100]}...")
        print(f"Successfully loaded {len(events)} events from {path}.")
    except FileNotFoundError:
        print(f"Error: File not found at {path}. Please ensure the file exists.")
    except Exception as e:
        print(f"Error: An unexpected error occurred while loading events from {path}: {e}")
    return events

def send_to_dynamo(events):
    """
    Sends a list of trip events to DynamoDB using update_item.
    This function expects events to have 'trip_id', 'event_type', and 'data' keys.
    
    Args:
        events (list): A list of event dictionaries to send.
    """
    print(f"Starting to send {len(events)} events to DynamoDB...")
    for i, event in enumerate(events):
        trip_id = event.get('trip_id')
        event_type = event.get('event_type')
        data = event.get('data')

        # Basic validation of event structure before sending
        if not all([trip_id, event_type, data]):
            print(f"Skipping malformed event at index {i}: Missing trip_id, event_type, or data. Event: {event}")
            continue

        try:
            # Construct UpdateExpression and ExpressionAttributeValues
            # This expression sets the specific event type's data and its arrival timestamp
            update_expr = f"SET {event_type}_event = :data, {event_type}_event_arrived_at = :ts"
            expr_attr_vals = {
                ':data': data,
                ':ts': datetime.utcnow().isoformat() # Use proper UTC ISO timestamp
            }

            table.update_item(
                Key={'trip_id': trip_id},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_attr_vals
            )

            # Log progress periodically
            if (i + 1) % 100 == 0:
                print(f"Sent {i+1} events to DynamoDB so far for event type {event_type}.")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            # Log specific DynamoDB errors for debugging
            print(f"Error: DynamoDB Client Error sending event {i} (trip_id: {trip_id}, type: {event_type}): {error_code} - {e}")
            # Depending on error (e.g., ProvisionedThroughputExceededException), you might add a retry here
        except Exception as e:
            # Catch any other unexpected errors during processing a single event
            print(f"Error: An unexpected error occurred while sending event {i} (trip_id: {trip_id}, type: {event_type}) to DynamoDB: {e}")
    print(f"Finished sending {len(events)} events to DynamoDB of type {event_type}.")

# --- Main Execution Block ---
if __name__ == "__main__":
    print("Starting data ingestion script.")
    
    # Load trip_start events
    start_events = load_events(start_path, limit=1000)
    # Load trip_end events
    end_events = load_events(end_path, limit=1000)

    # Send trip_start events to DynamoDB
    if start_events:
        print("Sending trip_start events to DynamoDB...")
        try:
            send_to_dynamo(start_events)
        except Exception as e:
            print(f"Critical Error: Failed to send trip_start events to DynamoDB: {e}")
    else:
        print("No trip_start events loaded. Skipping send.")

    # Send trip_end events to DynamoDB
    if end_events:
        print("Sending trip_end events to DynamoDB...")
        try:
            send_to_dynamo(end_events)
        except Exception as e:
            print(f"Critical Error: Failed to send trip_end events to DynamoDB: {e}")
    else:
        print("No trip_end events loaded. Skipping send.")

    print("Finished sending all specified start and end events to DynamoDB.")