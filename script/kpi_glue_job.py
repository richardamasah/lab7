import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from datetime import datetime
import json
import boto3
from botocore.exceptions import ClientError # Import specific exception for AWS client errors
from pyspark.sql.functions import col, split, when, lit, sum as spark_sum, avg as spark_avg, min as spark_min, max as spark_max, count as spark_count

# --- Glue Context Initialization ---
# Initialize SparkContext and GlueContext for Glue job execution
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session # Get the Spark session from GlueContext

print("Glue job started: Initializing Spark and Glue contexts.")

# --- AWS Client Setup ---
# Initialize AWS S3 and DynamoDB clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
# Reference to the DynamoDB table where trip events are stored
table = dynamodb.Table('TripEvents')
# S3 bucket where final KPIs will be stored
bucket = 'lab6kinesis'

print(f"AWS clients initialized. Targeting DynamoDB table: {table.name}, S3 bucket: {bucket}")

# --- Data Extraction from DynamoDB ---
def scan_dynamo_all():
    """
    Scans the entire DynamoDB table to retrieve all items.
    Handles pagination to ensure all items are fetched.
    """
    items = []
    response = None
    try:
        print(f"Starting full scan of DynamoDB table: {table.name}")
        # Perform the initial scan
        response = table.scan()
        items.extend(response.get('Items', []))

        # Continue scanning if LastEvaluatedKey is present (indicating more data)
        while 'LastEvaluatedKey' in response:
            print(f"Continuing scan from LastEvaluatedKey: {response['LastEvaluatedKey']}")
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))
        
        print(f"Finished scanning DynamoDB. Total items retrieved: {len(items)}")
        return items
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        print(f"Error: DynamoDB Client Error during scan: {error_code} - {e}")
        # Re-raise the exception to fail the Glue job if data cannot be retrieved
        raise
    except Exception as e:
        print(f"Error: An unexpected error occurred during DynamoDB scan: {e}")
        raise # Re-raise the exception to fail the Glue job

# Load data into a Spark DataFrame
# The DynamoDB items are converted to JSON strings and then parallelized to create an RDD,
# which is then read as a JSON DataFrame.
try:
    records = scan_dynamo_all()
    # Handle case where no records are returned from DynamoDB
    if not records:
        print("No records found in DynamoDB. Exiting job.")
        sys.exit(0) # Exit gracefully if no data to process

    # Convert complex Python objects (like Decimals from DynamoDB) to JSON strings
    # and then parse back into a Spark DataFrame. This ensures Spark handles types correctly.
    # json.dumps handles Decimal types which PySpark's read.json might struggle with directly
    # if not properly configured.
    records_as_json_strings = [json.dumps(r, default=str) for r in records] 
    df = spark.read.json(spark.sparkContext.parallelize(records_as_json_strings))
    print(f"Loaded {df.count()} records into Spark DataFrame.")
    df.printSchema() # Print schema for debugging
    df.show(5, truncate=False) # Show sample data for debugging

except Exception as e:
    print(f"Fatal Error: Failed to load data from DynamoDB or create DataFrame: {e}")
    sys.exit(1) # Exit with an error code

# --- Data Transformation ---
print("Starting data transformation and filtering...")

# Filter for 'complete' trips only as per requirements
complete_df = df.filter(col("status") == 'complete')
print(f"Filtered to {complete_df.count()} complete trips.")

# Select and rename necessary fields for aggregation
# Using selectExpr for concise column selection and aliasing
complete_df = complete_df.selectExpr(
    "trip_id",
    "start_event.pickup_datetime as pickup_datetime",
    "end_event.fare_amount as fare_amount",
    "is_stale"
)
print("Selected relevant fields for complete trips.")
complete_df.printSchema()
complete_df.show(5, truncate=False)

# Cast 'fare_amount' to double for numerical operations.
# Using .cast("double") handles potential nulls gracefully.
complete_df = complete_df.withColumn("fare_amount", col("fare_amount").cast("double"))
print("Cast 'fare_amount' to double.")
complete_df.printSchema()

# Extract 'trip_date' from 'pickup_datetime'.
# Assumes 'pickup_datetime' is in "YYYY-MM-DD HH:MM:SS" format.
complete_df = complete_df.withColumn("trip_date", split(col("pickup_datetime"), " ").getItem(0))
print("Extracted 'trip_date' from 'pickup_datetime'.")
complete_df.show(5, truncate=False)

# --- Aggregation ---
print("Starting aggregation of complete trips by trip_date...")

# Group by 'trip_date' and calculate required KPIs
# Using explicit spark_sum, spark_avg etc. to avoid conflicts with Python built-in sum
agg_df = complete_df.groupBy("trip_date").agg(
    spark_sum("fare_amount").alias("total_fare"),
    spark_avg("fare_amount").alias("average_fare"),
    spark_min("fare_amount").alias("min_fare"),
    spark_max("fare_amount").alias("max_fare"),
    spark_count("trip_id").alias("total_trips") # Counting trip_id to get total trips
)
print("Calculated main aggregation metrics.")
agg_df.show()

# Calculate stale trip counts separately
# Filter trips where 'is_stale' is true and count them per date
stale_df = complete_df.filter(col("is_stale") == True)
stale_counts = stale_df.groupBy("trip_date").count().withColumnRenamed("count", "stale_trip_count")
print("Calculated stale trip counts.")
stale_counts.show()

# Join the main aggregation with stale counts
# Using a left join to ensure all dates from agg_df are retained, filling 0 for dates with no stale trips
final_df = agg_df.join(stale_counts, on="trip_date", how="left").fillna(0, subset=["stale_trip_count"])
print("Joined main aggregation with stale trip counts.")
final_df.show()

# --- Write Results to S3 ---
print("Writing aggregated KPI data to S3...")

# Collect the DataFrame into a list of rows to iterate and write to S3
# Note: .collect() brings all data to the driver. For very large datasets,
# consider writing directly from Spark using .write.json() to a single file per partition,
# or partitioning by date directly if the target S3 path structure allows it.
# For this specific requirement (each row as a separate file), collect is necessary.
rows = final_df.collect()
if not rows:
    print("No aggregated KPI data to write to S3.")
else:
    for row in rows:
        try:
            # Prepare the output JSON structure
            output = {
                "trip_date": row["trip_date"],
                "total_trips": int(row["total_trips"]),
                "total_fare": round(row["total_fare"], 2),
                "average_fare": round(row["average_fare"], 2),
                "min_fare": round(row["min_fare"], 2),
                "max_fare": round(row["max_fare"], 2),
                "stale_trip_count": int(row["stale_trip_count"])
            }
            # Define the S3 key (path) for the output file
            key = f"kpis/complete/{row['trip_date']}/complete_kpis_{row['trip_date']}.json"
            
            # Upload the JSON data to S3
            s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(output, indent=2))
            print(f"Wrote KPI to S3: s3://{bucket}/{key}")
        except KeyError as e:
            print(f"Error: Missing expected column in row for trip_date {row.get('trip_date', 'N/A')}: {e}. Skipping row.")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            print(f"Error: S3 Client Error writing KPI for {row.get('trip_date', 'N/A')}: {error_code} - {e}. Skipping upload for this date.")
        except Exception as e:
            print(f"Error: An unexpected error occurred writing KPI for {row.get('trip_date', 'N/A')} to S3: {e}. Skipping upload for this date.")

print("Glue job finished: All complete trip KPIs processed and uploaded to S3.")