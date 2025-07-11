import csv
import json
import boto3
from pathlib import Path
from botocore.exceptions import ClientError # Import specific exception for AWS client errors

# --- Kinesis Stream Configuration ---
STREAM_NAME = "trip_events_stream" # Name of the Kinesis Data Stream
REGION = "eu-north-1"             # AWS Region where the Kinesis stream is located
MAX_EVENTS = 10                   # Number of matched trips to simulate (max events to send)

# Initialize Kinesis client
kinesis = boto3.client("kinesis", region_name=REGION)
print(f"Kinesis client initialized for stream: {STREAM_NAME} in region: {REGION}")

# --- Helper Functions ---

def load_csv(path):
    """
    Loads data from a CSV file into a list of dictionaries.
    
    Args:
        path (Path): The file path to the CSV.
        
    Returns:
        list: A list of dictionaries, where each dictionary represents a row.
    """
    data = []
    try:
        print(f"Attempting to load data from CSV: {path}")
        with open(path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        print(f"Successfully loaded {len(data)} rows from {path}.")
    except FileNotFoundError:
        print(f"Error: CSV file not found at {path}. Please ensure the file exists.")
    except csv.Error as e:
        print(f"Error: CSV parsing error in {path}: {e}")
    except Exception as e:
        print(f"Error: An unexpected error occurred while loading CSV from {path}: {e}")
    return data

def send_event(event):
    """
    Sends a single event (dictionary) to the configured Kinesis Data Stream.
    The event is JSON-serialized and then encoded.
    
    Args:
        event (dict): The event dictionary to send. Must contain 'trip_id'.
    """
    trip_id = event.get("trip_id", "unknown_trip") # Get trip_id for logging
    event_type = event.get("event_type", "unknown_type") # Get event_type for logging
    
    try:
        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(event),
            PartitionKey=trip_id # Using trip_id as PartitionKey ensures all events for a trip go to the same shard
        )
        # Log success with key details from the event and Kinesis response
        print(f"Sent event to Kinesis: Type: {event_type}, Trip ID: {trip_id}, ShardId: {response.get('ShardId')}, SequenceNumber: {response.get('SequenceNumber')}")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        # Log specific Kinesis client errors (e.g., ResourceNotFoundException, ProvisionedThroughputExceededException)
        print(f"Error: Kinesis Client Error sending event (Type: {event_type}, Trip ID: {trip_id}): {error_code} - {e}")
        # Depending on the error, you might want to implement a retry mechanism or Dead-Letter Queue (DLQ)
    except json.JSONDecodeError as e:
        print(f"Error: Could not serialize event to JSON for Trip ID {trip_id}: {e}. Event data: {event}")
    except Exception as e:
        # Catch any other unexpected errors
        print(f"Error: An unexpected error occurred while sending event (Type: {event_type}, Trip ID: {trip_id}) to Kinesis: {e}")

def build_event(row, event_type):
    """
    Constructs a standardized event dictionary from a CSV row.
    
    Args:
        row (dict): A dictionary representing a row from the CSV.
        event_type (str): The type of event (e.g., "trip_start", "trip_end").
        
    Returns:
        dict: The structured event dictionary.
    """
    # Strip whitespace from trip_id to ensure consistency for matching and partitioning
    trip_id = row.get("trip_id", "").strip()
    if not trip_id:
        print(f"Warning: Row has no trip_id. Skipping event build for row: {row}")
        return None # Return None if trip_id is critical and missing
        
    row["trip_id"] = trip_id # Update the row with stripped trip_id for consistency
    return {
        "event_type": event_type,
        "trip_id": trip_id,
        "data": row # The original row data becomes the 'data' payload
    }

# --- Main Execution Block ---
if __name__ == "__main__":
    print("Starting Kinesis event simulation script.")
    
    # Define paths to data directories
    base = Path(__file__).resolve().parent
    data_dir = base / "data"

    # Load trip_start and trip_end data from CSV files
    trip_start_raw = load_csv(data_dir / "trip_start.csv")
    trip_end_raw = load_csv(data_dir / "trip_end.csv")

    if not trip_start_raw or not trip_end_raw:
        print("Not enough data loaded from CSV files. Exiting.")
        sys.exit(1) # Exit if essential data is missing

    # Identify trip IDs that exist in both start and end datasets
    # This helps simulate complete trips for testing the pipeline
    start_ids = {row["trip_id"].strip() for row in trip_start_raw if row.get("trip_id")}
    end_ids = {row["trip_id"].strip() for row in trip_end_raw if row.get("trip_id")}
    
    matched_ids = list(start_ids.intersection(end_ids))[:MAX_EVENTS] # Limit to MAX_EVENTS
    print(f"Found {len(matched_ids)} matched trip IDs for simulation (limited to {MAX_EVENTS}).")

    # Build structured event dictionaries for matched trip IDs
    start_events = [build_event(row, "trip_start") for row in trip_start_raw if row.get("trip_id", "").strip() in matched_ids]
    end_events = [build_event(row, "trip_end") for row in trip_end_raw if row.get("trip_id", "").strip() in matched_ids]
    
    # Filter out any None events if build_event returned None for invalid rows
    start_events = [e for e in start_events if e is not None]
    end_events = [e for e in end_events if e is not None]

    print(f"Prepared {len(start_events)} trip_start events and {len(end_events)} trip_end events for Kinesis.")

    # Send all matched start and end events to Kinesis
    # Events are sent in a combined list, order is not guaranteed at the Kinesis level,
    # which simulates real-world unordered arrival.
    combined_events = start_events + end_events
    print(f"Sending a total of {len(combined_events)} events to Kinesis...")

    events_sent_count = 0
    for event in combined_events:
        try:
            send_event(event)
            events_sent_count += 1
        except Exception as e:
            # Error during send_event is already logged, just count and continue
            print(f"Failed to send an event. Continuing with next. Error: {e}")
            
    print(f"Finished sending {events_sent_count} events to Kinesis stream {STREAM_NAME}.")
    if events_sent_count < len(combined_events):
        print(f"Warning: {len(combined_events) - events_sent_count} events failed to send.")