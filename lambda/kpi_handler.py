import json
import boto3
from datetime import datetime
from collections import defaultdict

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

TABLE_NAME = "TripEvents"
BUCKET = "lab6kinesis"

def lambda_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)
    result_by_date = defaultdict(list)
    in_progress_by_date = defaultdict(lambda: {"in_progress_trips": 0, "stale_in_progress": 0})

    # 1. Scan DynamoDB
    response = table.scan()
    items = response.get("Items", [])

    for trip in items:
        trip_id = trip.get("trip_id")
        status = trip.get("status")
        start_event = trip.get("start_event", {})
        trip_date = start_event.get("pickup_datetime", "").split(" ")[0]

        if not trip_date:
            continue

        # --- COMPLETE KPI ---
        if status == "complete":
            end_event = trip.get("end_event", {})
            fare = float(end_event.get("fare_amount", 0.0))
            result_by_date[trip_date].append({
                "fare": fare,
                "is_stale": trip.get("is_stale", False)
            })

        # --- IN-PROGRESS KPI ---
        elif status == "in_progress":
            in_progress_by_date[trip_date]["in_progress_trips"] += 1
            if trip.get("is_stale"):
                in_progress_by_date[trip_date]["stale_in_progress"] += 1

    # 2. Upload COMPLETE KPI
    for trip_date, fares in result_by_date.items():
        total = sum([x["fare"] for x in fares])
        count = len(fares)
        kpi = {
            "trip_date": trip_date,
            "total_trips": count,
            "total_fare": round(total, 2),
            "average_fare": round(total / count, 2) if count else 0,
            "min_fare": round(min([x["fare"] for x in fares]), 2),
            "max_fare": round(max([x["fare"] for x in fares]), 2),
            "stale_trip_count": sum(1 for x in fares if x["is_stale"])
        }

        key = f"kpis/complete/{trip_date}.json"
        s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(kpi, indent=2))
        print(f"✅ Uploaded complete KPI: {key}")

    # 3. Upload IN-PROGRESS KPI
    for trip_date, data in in_progress_by_date.items():
        out_data = {
            "trip_date": trip_date,
            **data
        }
        key = f"kpis/in_progress/{trip_date}.json"
        s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(out_data, indent=2))
        print(f"🟡 Uploaded in-progress KPI: {key}")

    return {"statusCode": 200, "message": "KPIs uploaded to S3"}
