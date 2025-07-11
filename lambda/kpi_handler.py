import json
import boto3
from datetime import datetime
from collections import defaultdict
from botocore.exceptions import ClientError # Import specific exception for AWS client errors

# Initialize AWS clients for DynamoDB and S3
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# --- Configuration Constants ---
TABLE_NAME = "TripEvents"  # DynamoDB table storing trip data
BUCKET = "lab6kinesis"     # S3 bucket for KPI output

def lambda_handler(event, context):
    """
    Main Lambda function handler for generating daily KPIs.
    Scans the DynamoDB TripEvents table, calculates complete and in-progress
    trip metrics, and uploads them as JSON files to S3.
    """
    print("KPI generation Lambda started.")
    
    table = dynamodb.Table(TABLE_NAME)
    
    # Dictionaries to store aggregated data by date
    # result_by_date: stores fare and stale status for complete trips
    result_by_date = defaultdict(list)
    # in_progress_by_date: stores counts for in-progress and stale in-progress trips
    in_progress_by_date = defaultdict(lambda: {"in_progress_trips": 0, "stale_in_progress": 0})

    # --- 1. Scan DynamoDB Table ---
    # It's important to note that full table scans can be expensive and slow for large tables.
    # For production, consider using DynamoDB Streams for real-time aggregation
    # or GSI/Exports for targeted batch processing if data volume is high.
    items = []
    try:
        print(f"Scanning DynamoDB table: {TABLE_NAME}...")
        # Paginate through scan results to ensure all items are retrieved
        response = table.scan()
        items.extend(response.get("Items", []))
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get("Items", []))
        print(f"Completed scan. Retrieved {len(items)} items from DynamoDB.")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        print(f"Error: DynamoDB Client Error during scan: {error_code} - {e}")
        return {"statusCode": 500, "message": f"DynamoDB scan failed: {error_code}"}
    except Exception as e:
        print(f"Error: An unexpected error occurred during DynamoDB scan: {e}")
        return {"statusCode": 500, "message": f"Unexpected error during DynamoDB scan: {e}"}

    # --- Process Retrieved Items ---
    for trip in items:
        trip_id = trip.get("trip_id")
        status = trip.get("status")
        
        # Determine the trip date from the pickup_datetime of the start_event
        # This assumes pickup_datetime is the canonical date for trip KPIs
        start_event = trip.get("start_event", {})
        pickup_datetime_str = start_event.get("pickup_datetime", "")
        
        # Extract date part (e.g., "YYYY-MM-DD")
        trip_date = pickup_datetime_str.split(" ")[0] if pickup_datetime_str else None

        # Skip items without a valid trip date (e.g., incomplete start events or malformed data)
        if not trip_date:
            print(f"Warning: Skipping trip {trip_id} due to missing or invalid pickup_datetime for date extraction.")
            continue

        # --- Aggregate COMPLETE KPI data ---
        if status == "complete":
            end_event = trip.get("end_event", {})
            try:
                # Ensure fare_amount is a float; default to 0.0 if not found or invalid
                fare = float(end_event.get("fare_amount", 0.0))
                result_by_date[trip_date].append({
                    "fare": fare,
                    "is_stale": trip.get("is_stale", False) # Default to False if not present
                })
            except (ValueError, TypeError) as e:
                print(f"Error: Could not convert fare_amount to float for trip {trip_id}. Skipping fare for KPI. Error: {e}")
                # This trip's fare will be excluded from calculation for the day
                
        # --- Aggregate IN-PROGRESS KPI data ---
        elif status == "in_progress":
            in_progress_by_date[trip_date]["in_progress_trips"] += 1
            if trip.get("is_stale"):
                in_progress_by_date[trip_date]["stale_in_progress"] += 1

    # --- 2. Calculate and Upload COMPLETE KPIs to S3 ---
    print("Calculating and uploading complete trip KPIs to S3...")
    for trip_date, fares_list in result_by_date.items():
        if not fares_list:
            print(f"No complete trip data for date: {trip_date}. Skipping KPI upload.")
            continue

        total = sum([x["fare"] for x in fares_list])
        count = len(fares_list)
        
        # Initialize min/max with a value from the list to avoid errors with empty list
        min_fare_val = min([x["fare"] for x in fares_list]) if fares_list else 0.0
        max_fare_val = max([x["fare"] for x in fares_list]) if fares_list else 0.0

        kpi = {
            "trip_date": trip_date,
            "total_trips": count,
            "total_fare": round(total, 2),
            "average_fare": round(total / count, 2) if count else 0.0, # Avoid division by zero
            "min_fare": round(min_fare_val, 2),
            "max_fare": round(max_fare_val, 2),
            "stale_trip_count": sum(1 for x in fares_list if x.get("is_stale", False))
        }

        # Define S3 key for the complete KPI file
        # Using a date-based prefix for better organization and query performance
        key = f"kpis/complete/{trip_date}/complete_kpis_{trip_date}.json" 
        try:
            s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(kpi, indent=2))
            print(f"Uploaded complete KPI for {trip_date}: s3://{BUCKET}/{key}")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            print(f"Error: S3 Client Error uploading complete KPI for {trip_date}: {error_code} - {e}")
        except Exception as e:
            print(f"Error: An unexpected error occurred uploading complete KPI for {trip_date} to S3: {e}")

    # --- 3. Calculate and Upload IN-PROGRESS KPIs to S3 ---
    print("Calculating and uploading in-progress trip KPIs to S3...")
    for trip_date, data in in_progress_by_date.items():
        # Define S3 key for the in-progress KPI file
        key = f"kpis/in_progress/{trip_date}/in_progress_kpis_{trip_date}.json"
        out_data = {
            "trip_date": trip_date,
            **data # Unpack the defaultdict content directly
        }
        try:
            s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(out_data, indent=2))
            print(f"Uploaded in-progress KPI for {trip_date}: s3://{BUCKET}/{key}")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            print(f"Error: S3 Client Error uploading in-progress KPI for {trip_date}: {error_code} - {e}")
        except Exception as e:
            print(f"Error: An unexpected error occurred uploading in-progress KPI for {trip_date} to S3: {e}")

    print("KPI generation Lambda finished.")
    return {"statusCode": 200, "message": "KPIs uploaded to S3"}