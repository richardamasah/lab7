import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from datetime import datetime
import json
import boto3

# Set up Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# S3 + Dynamo
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TripEvents')
bucket = 'lab6kinesis'

# Pull all items from DynamoDB
def scan_dynamo_all():
    items = []
    response = table.scan()
    items.extend(response.get('Items', []))
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))
    return items

# Load data
records = scan_dynamo_all()
df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in records]))

# Filter complete trips only
complete_df = df.filter(df.status == 'complete')

# Extract fields
complete_df = complete_df.selectExpr(
    "trip_id",
    "start_event.pickup_datetime as pickup_datetime",
    "end_event.fare_amount as fare_amount",
    "is_stale"
)

# Convert fare to float
complete_df = complete_df.withColumn("fare_amount", complete_df.fare_amount.cast("double"))

# Extract trip_date
from pyspark.sql.functions import col, split
complete_df = complete_df.withColumn("trip_date", split(col("pickup_datetime"), " ").getItem(0))

# Group and aggregate
agg_df = complete_df.groupBy("trip_date").agg(
    {"fare_amount": "sum", "fare_amount": "avg", "fare_amount": "min", "fare_amount": "max", "trip_id": "count"}
).withColumnRenamed("sum(fare_amount)", "total_fare") \
 .withColumnRenamed("avg(fare_amount)", "average_fare") \
 .withColumnRenamed("min(fare_amount)", "min_fare") \
 .withColumnRenamed("max(fare_amount)", "max_fare") \
 .withColumnRenamed("count(trip_id)", "total_trips")

# Add stale trip count manually
from pyspark.sql.functions import when, lit
stale_df = complete_df.filter(col("is_stale") == True)
stale_counts = stale_df.groupBy("trip_date").count().withColumnRenamed("count", "stale_trip_count")

# Join stale counts
final_df = agg_df.join(stale_counts, on="trip_date", how="left").fillna(0)

# Write each row to S3 as separate KPI file
rows = final_df.collect()
for row in rows:
    output = {
        "trip_date": row["trip_date"],
        "total_trips": int(row["total_trips"]),
        "total_fare": round(row["total_fare"], 2),
        "average_fare": round(row["average_fare"], 2),
        "min_fare": round(row["min_fare"], 2),
        "max_fare": round(row["max_fare"], 2),
        "stale_trip_count": int(row["stale_trip_count"])
    }
    key = f"kpis/complete/{row['trip_date']}.json"
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(output, indent=2))
    print(f"✅ Wrote KPI to S3: {key}")
