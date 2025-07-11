import boto3
import time

# Initialize Kinesis client
kinesis = boto3.client('kinesis', region_name='eu-north-1')

# Stream name
stream_name = "twitter-stream"

# Counter to track lines
line_count = 0

# Open the file and stream data
with open('sampled_100k_sentiment140.csv', 'r', encoding='latin-1') as f:
    next(f)  # Skip header row
    for line in f:
        line = line.strip()
        try:
            response = kinesis.put_record(
                StreamName=stream_name,
                Data=line.encode('utf-8'),  # ensure bytes, not string
                PartitionKey="partitionkey"
            )
            line_count += 1
            if line_count % 100 == 0:
                print(f"[INFO] {line_count} records sent. Shard ID: {response['ShardId']}")
            time.sleep(0.1)  # simulate real-time streaming
        except Exception as e:
            print(f"[ERROR] Failed to put record at line {line_count}: {str(e)}")
