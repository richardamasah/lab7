import json
import base64
import boto3
from datetime import datetime
import re

# Initialize AWS clients for DynamoDB and S3
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# --- Configuration Constants ---
TABLE_NAME = 'TripEvents'
BUCKET_NAME = 'lab6kinesis'
RAW_FOLDER = 'raw_events'
INVALID_FOLDER = 'invalid_events' # Folder for events that fail schema validation

# Get a reference to the DynamoDB table
table = dynamodb.Table(TABLE_NAME)

# --- Helper Functions ---

def current_time():
    """
    Returns the current UTC time in ISO 8601 format.
    Used for timestamping when events are processed.
    """
    return datetime.utcnow().isoformat()

def is_valid_event(event):
    """
    Performs basic schema validation for incoming trip_start and trip_end events.
    Ensures required fields are present and checks datetime format.
    """
    event_type = event.get("event_type")
    data = event.get("data", {})
    
    # Check if event_type is valid
    if event_type not in ["trip_start", "trip_end"]:
        print(f"Validation Error: Unknown event_type '{event_type}'")
        return False
    
    # Check for mandatory 'trip_id' in the main event structure
    if "trip_id" not in event or not event["trip_id"]:
        print(f"Validation Error: 'trip_id' missing or empty in event: {event}")
        return False

    # Define required fields based on event type
    if event_type == "trip_start":
        required_fields = ["pickup_location_id", "dropoff_location_id", "vendor_id", "pickup_datetime", "estimated_dropoff_datetime", "estimated_fare_amount"]
    else:  # event_type == "trip_end"
        required_fields = ["dropoff_datetime", "rate_code", "passenger_count", "trip_distance", "fare_amount", "tip_amount", "payment_type", "trip_type", "trip_id"]

    # Check if all required fields are present in the 'data' payload
    for field in required_fields:
        if field not in data:
            print(f"Validation Error for {event_type} (trip_id: {event.get('trip_id')}): Missing required field '{field}' in 'data'.")
            return False

    # Optional: Validate datetime format using regex for specific fields
    # This assumes a "YYYY-MM-DD HH:MM:SS" format.
    datetime_fields_to_check = []
    if event_type == "trip_start":
        datetime_fields_to_check.append("pickup_datetime")
        datetime_fields_to_check.append("estimated_dropoff_datetime")
    elif event_type == "trip_end":
        datetime_fields_to_check.append("dropoff_datetime")

    for field in datetime_fields_to_check:
        if field in data and not re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", str(data[field])):
            print(f"Validation Error for {event_type} (trip_id: {event.get('trip_id')}): Invalid datetime format for '{field}'. Expected YYYY-MM-DD HH:MM:SS.")
            return False

    return True

def archive_event_to_s3(event_data, folder):
    """
    Archives a raw event to a specified S3 folder.
    Used for storing both raw and invalid events for auditing/reprocessing.
    """
    # Generate S3 key based on current UTC date and trip_id
    date = datetime.utcnow().strftime("%Y-%m-%d")
    trip_id = event_data.get("trip_id", "unknown_trip_id") # Use a default if trip_id is missing
    # Add a timestamp to the key to prevent overwriting if multiple events for the same trip_id arrive in the same second
    timestamp = datetime.utcnow().strftime("%H%M%S%f")[:-3] # milliseconds
    key = f"{folder}/{date}/{trip_id}/{timestamp}.json"
    
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(event_data, indent=2))
        print(f"Archived event to S3: s3://{BUCKET_NAME}/{key}")
    except Exception as e:
        # Log error but don't re-raise, as archiving is secondary to core processing
        print(f"Error archiving event to S3 {key}: {e}")

# --- Main Lambda Handler ---

def lambda_handler(event, context):
    """
    The main Lambda function handler.
    Processes records from a Kinesis Data Stream.
    """
    print(f"Received {len(event['Records'])} records from Kinesis.")
    
    for record in event['Records']:
        try:
            # Decode Kinesis data payload
            # Kinesis records are base64 encoded
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            event_data = json.loads(payload)
            print(f"Processing Kinesis record with sequence number: {record['kinesis']['sequenceNumber']}")
            print(f"Raw event payload: {json.dumps(event_data)}")

            # Archive ALL incoming events to the raw data S3 folder
            archive_event_to_s3(event_data, RAW_FOLDER)

            # Validate the event schema
            if not is_valid_event(event_data):
                # If validation fails, archive to invalid events folder and skip further processing
                archive_event_to_s3(event_data, INVALID_FOLDER)
                print(f"Invalid event schema detected for trip_id: {event_data.get('trip_id', 'N/A')}. Event moved to invalid folder.")
                continue # Skip to the next Kinesis record

            # If validation passes, proceed with processing the event
            process_event(event_data)

        except json.JSONDecodeError as e:
            # Handle cases where the payload is not valid JSON
            print(f"Error: Could not decode JSON payload from Kinesis record. Payload: {payload[:200]}... Error: {e}")
            # Optionally, archive the malformed payload to a 'malformed' S3 folder for later inspection
        except KeyError as e:
            # Handle missing expected keys in the Kinesis record structure
            print(f"Error: Missing expected key in Kinesis record: {e}. Record: {record}")
        except base64.binascii.Error as e:
            # Handle cases where base64 decoding fails
            print(f"Error: Could not base64 decode Kinesis data. Error: {e}")
        except Exception as e:
            # Catch any other unexpected errors during record processing
            print(f"An unhandled error occurred during record processing: {e}. Record: {record}")
            # Depending on requirements, you might want to send this to a DLQ

    # Return 200 OK to Kinesis to acknowledge successful batch processing.
    # Note: If any record processing fails in a batch, Kinesis will re-deliver the entire batch.
    # For fine-grained error handling and partial batch success, more advanced patterns are needed (e.g., SQS DLQ).
    return {"statusCode": 200}

# --- Event Processing Logic ---

def process_event(event):
    """
    Processes a validated trip event, updating or creating the trip record in DynamoDB.
    Handles idempotency and updates trip status.
    """
    # Extract key information from the event
    trip_id = event['trip_id'].strip()
    event_type = event['event_type']
    new_data = event['data']

    print(f"Attempting to process {event_type} for trip_id: {trip_id}")

    try:
        # Attempt to retrieve existing trip item from DynamoDB
        response = table.get_item(Key={'trip_id': trip_id})
        trip = response.get('Item', {
            "trip_id": trip_id,
            "status": "new", # Initial status for a new trip
            "start_event": None,
            "end_event": None,
            "start_event_arrived_at": None,
            "end_event_arrived_at": None,
            "is_stale": False # Flag for trips that remain in_progress for too long
        })
        print(f"Current trip state for {trip_id}: {trip.get('status')}")

        # Idempotency check: Prevent processing identical duplicate events
        if event_type == "trip_start":
            if trip.get("start_event") == new_data:
                print(f"Skipping: Identical duplicate trip_start event for {trip_id} received. No state change needed.")
                return # Exit function for duplicate
            # If a different start event arrived for the same trip_id, it indicates an issue
            elif trip.get("start_event") is not None and trip.get("start_event") != new_data:
                print(f"Warning: A new trip_start event for {trip_id} arrived with different data. Overwriting previous start_event.")
                # Depending on business rules, this might warrant more specific error handling or logging.

        if event_type == "trip_end":
            if trip.get("end_event") == new_data:
                print(f"Skipping: Identical duplicate trip_end event for {trip_id} received. No state change needed.")
                return # Exit function for duplicate
            # If a different end event arrived for the same trip_id, it indicates an issue
            elif trip.get("end_event") is not None and trip.get("end_event") != new_data:
                print(f"Warning: A new trip_end event for {trip_id} arrived with different data. Overwriting previous end_event.")


        # Update the trip item with the new event data and arrival timestamp
        if event_type == "trip_start":
            trip["start_event"] = new_data
            trip["start_event_arrived_at"] = current_time()
            print(f"Updated start_event for {trip_id}.")
        else: # event_type == "trip_end"
            trip["end_event"] = new_data
            trip["end_event_arrived_at"] = current_time()
            print(f"Updated end_event for {trip_id}.")

        # Determine the current status of the trip
        # A trip is 'complete' if both start and end events have been received
        trip["status"] = "complete" if trip["start_event"] and trip["end_event"] else "in_progress"

        # Check for stale trips: A trip is considered stale if it's 'in_progress'
        # and has been in that state for more than 600 seconds (10 minutes) since its last event arrived.
        trip["is_stale"] = False
        now = datetime.utcnow()
        if trip["status"] == "in_progress":
            # Check if it's stale due to a missing end event after start
            if trip["start_event"] and not trip["end_event"] and trip["start_event_arrived_at"]:
                t = datetime.fromisoformat(trip["start_event_arrived_at"])
                if (now - t).total_seconds() > 600:
                    trip["is_stale"] = True
                    print(f"Trip {trip_id} marked as stale: missing end event after start for over 10 minutes.")
            # Check if it's stale due to a missing start event after end (less common, but possible)
            elif trip["end_event"] and not trip["start_event"] and trip["end_event_arrived_at"]:
                t = datetime.fromisoformat(trip["end_event_arrived_at"])
                if (now - t).total_seconds() > 600:
                    trip["is_stale"] = True
                    print(f"Trip {trip_id} marked as stale: missing start event after end for over 10 minutes.")

        # Persist the updated trip item to DynamoDB
        table.put_item(Item=trip)
        print(f"Successfully processed {event_type} for {trip_id}. New status: {trip['status']} | Is Stale: {trip['is_stale']}")

    except dynamodb.meta.client.exceptions.ClientError as e:
        # Catch specific DynamoDB client errors (e.g., throttling, access denied)
        error_code = e.response.get("Error", {}).get("Code")
        print(f"DynamoDB Client Error processing trip {trip_id} ({event_type}): {error_code} - {e}")
        # Depending on the error, you might want to re-raise or implement a retry mechanism.
    except Exception as e:
        # Catch any other unexpected errors during process_event
        print(f"An unexpected error occurred while processing trip {trip_id} ({event_type}): {e}")